#!/usr/bin/env python3
"""
ui.py — Interface graphique Gradio pour Python Agent
=====================================================
Permet de configurer les bases de données, le LLM, les agents,
et de lancer des instructions via un chat avec affichage des étapes.

Usage:
  python ui.py
  python ui.py --port 8080
  python ui.py --host 0.0.0.0 --share

Compatibilité : Gradio 3.0+ (3.x et 4.x)
"""
import inspect
import json
import os
import queue
import re
import sys
import argparse
import threading
import traceback
import urllib.request
import urllib.error
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

# Ensure project root is on sys.path so we can import core/agents
_ROOT = Path(__file__).parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

try:
    import gradio as gr
except ImportError:
    print("Gradio non installé. Installez-le avec : pip install gradio")
    sys.exit(1)

CONFIG_FILE = _ROOT / "config.json"

# ---------------------------------------------------------------------------
# Agent metadata (for UI display)
# ---------------------------------------------------------------------------

AGENT_INFO: Dict[str, Dict] = {
    "analyst": {
        "display": "📊 Analyst",
        "description": "Analyse de données approfondie, statistiques, tendances",
        "default_max_steps": 15,
        "default_reflection": 5,
        "category": "base",
        "mission_key": "ANALYST_MISSION",
    },
    "quality": {
        "display": "🔍 Quality",
        "description": "Audit qualité des données (nulls, doublons, outliers)",
        "default_max_steps": 15,
        "default_reflection": 5,
        "category": "base",
        "mission_key": "QUALITY_MISSION",
    },
    "pattern": {
        "display": "🔮 Pattern",
        "description": "Découverte de patterns, corrélations, anomalies",
        "default_max_steps": 15,
        "default_reflection": 5,
        "category": "base",
        "mission_key": "PATTERN_MISSION",
    },
    "query": {
        "display": "⚡ Query",
        "description": "Construction et optimisation SQL",
        "default_max_steps": 15,
        "default_reflection": 5,
        "category": "base",
        "mission_key": "QUERY_MISSION",
    },
    "sql_analyst": {
        "display": "🐘 SQL Analyst",
        "description": "Expert SQL ClickHouse senior avec EXPLAIN preflight",
        "default_max_steps": 20,
        "default_reflection": 5,
        "category": "clickhouse",
        "mission_key": "CH_SQL_ANALYST_MISSION",
    },
    "clickhouse_generic": {
        "display": "🔄 CH Generic",
        "description": "Analyste ClickHouse polyvalent avec décomposition DAG",
        "default_max_steps": 20,
        "default_reflection": 5,
        "category": "clickhouse",
        "mission_key": "CH_GENERIC_MISSION",
    },
    "clickhouse_table_manager": {
        "display": "🗂️ Table Manager",
        "description": "Administrateur DDL ClickHouse (CREATE/ALTER avec garde-fous)",
        "default_max_steps": 15,
        "default_reflection": 5,
        "category": "clickhouse",
        "mission_key": "CH_TABLE_MANAGER_MISSION",
    },
    "clickhouse_writer": {
        "display": "✍️ Writer",
        "description": "DML sécurisé — INSERT restreint aux tables agent_*",
        "default_max_steps": 15,
        "default_reflection": 5,
        "category": "clickhouse",
        "mission_key": "CH_WRITER_MISSION",
    },
    "clickhouse_specific": {
        "display": "📋 Specific",
        "description": "Exécution de templates paramétrés (P1-P5)",
        "default_max_steps": 15,
        "default_reflection": 5,
        "category": "clickhouse",
        "mission_key": "CH_SPECIFIC_MISSION",
    },
    "text_to_sql_translator": {
        "display": "🗣️ Text-to-SQL",
        "description": "Traduction langage naturel → SQL ClickHouse optimisé",
        "default_max_steps": 15,
        "default_reflection": 5,
        "category": "clickhouse",
        "mission_key": "CH_TEXT_TO_SQL_MISSION",
    },
}

TEMPLATE_CHOICES = list(AGENT_INFO.keys())


def _get_mission(mission_key: str) -> str:
    """Fetch the default mission text from utils.prompts."""
    try:
        import utils.prompts as p
        return getattr(p, mission_key, "")
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# Compatibility helpers
# ---------------------------------------------------------------------------

def _kw(obj, **kwargs) -> dict:
    """
    Filtre les kwargs pour ne garder que ceux acceptés par obj.
    Fonctionne pour les classes (inspecte __init__) et les callables.
    Si la signature accepte **kwargs, on retourne tout sans filtrage.
    """
    try:
        if isinstance(obj, type):
            sig = inspect.signature(obj.__init__)
        else:
            sig = inspect.signature(obj)
        params = sig.parameters
        for p in params.values():
            if p.kind == inspect.Parameter.VAR_KEYWORD:
                return kwargs
        supported = set(params.keys()) - {"self"}
        return {k: v for k, v in kwargs.items() if k in supported}
    except Exception:
        return kwargs


def _get_theme():
    """Retourne un thème Gradio si disponible, None sinon."""
    try:
        return gr.themes.Soft()
    except AttributeError:
        pass
    try:
        return gr.themes.Default()
    except AttributeError:
        pass
    return None


try:
    _Group = gr.Group
except AttributeError:
    import contextlib
    _Group = contextlib.nullcontext


# ---------------------------------------------------------------------------
# Helpers config
# ---------------------------------------------------------------------------

def _strip_config_comments(raw: str) -> dict:
    """Supprime les commentaires // et les clés _comment du JSON."""
    raw = re.sub(r'(?m)(?<=\s)//[^\n]*', '', raw)
    config = json.loads(raw)

    def _drop(obj):
        if isinstance(obj, dict):
            return {k: _drop(v) for k, v in obj.items() if not k.startswith("_comment")}
        if isinstance(obj, list):
            return [_drop(i) for i in obj]
        return obj

    return _drop(config)


def load_config() -> Dict[str, Any]:
    if CONFIG_FILE.exists():
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return _strip_config_comments(f.read())
    return {}


def save_config(config: Dict[str, Any]) -> None:
    existing = {}
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                existing = _strip_config_comments(f.read())
        except Exception:
            pass
    existing.update(config)
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(existing, f, indent=2, ensure_ascii=False)


def _get_agent_override(agent_key: str) -> Dict[str, Any]:
    """Shortcut: get per-agent override dict from config."""
    return load_config().get("agent_overrides", {}).get(agent_key, {})


def _get_custom_agents() -> List[Dict]:
    """Return the list of user-created custom agents."""
    return load_config().get("custom_agents", [])


def _all_agent_choices() -> List[str]:
    """Return the full list of agent choices for the chat dropdown."""
    choices = ["manager"] + list(AGENT_INFO.keys())
    for ca in _get_custom_agents():
        name = ca.get("name", "").strip()
        if name and name not in choices:
            choices.append(name)
    return choices


# ---------------------------------------------------------------------------
# Step formatting (for chat display)
# ---------------------------------------------------------------------------

def _fmt_result(result: Any) -> str:
    """Format a tool result for markdown display."""
    if result is None:
        return "*aucun résultat*"
    if isinstance(result, list):
        if not result:
            return "`[]` — liste vide"
        sample = json.dumps(result[0], ensure_ascii=False, default=str)[:120]
        total = f" **({len(result)} lignes)**" if len(result) > 1 else ""
        return f"`{sample}`{total}"
    if isinstance(result, dict):
        return f"`{json.dumps(result, ensure_ascii=False, default=str)[:200]}`"
    return f"`{str(result)[:300]}`"


def _format_step_markdown(data: dict) -> str:
    """Convert a step_callback payload into clean markdown for the chatbot."""
    t = data.get("type", "step")

    if t == "dispatch":
        agent_type = data.get("agent_type", "?")
        task = data.get("task", "")[:300]
        return (
            f"\n---\n"
            f"**🚀 Dispatch → `{agent_type}`**\n"
            f"> {task}\n"
        )

    if t == "dispatch_done":
        agent_type = data.get("agent_type", "?")
        summary = data.get("summary", "")[:300]
        steps = data.get("steps", "?")
        return (
            f"**✅ `{agent_type}` terminé** — {steps} étape(s)\n"
            f"> {summary}\n"
        )

    # t == "step"
    agent     = data.get("agent", "Agent")
    step_num  = data.get("step", "?")
    max_steps = data.get("max_steps", "?")
    thought   = (data.get("thought") or "")[:300]
    action    = data.get("action", "?")
    params    = data.get("params", {})
    confidence= int(float(data.get("confidence", 0)) * 100)
    error     = data.get("error")
    result    = data.get("result")

    params_str = json.dumps(params, ensure_ascii=False, default=str)
    if len(params_str) > 180:
        params_str = params_str[:177] + "…"

    if error:
        result_md = f"❌ `{str(error)[:250]}`"
    else:
        result_md = _fmt_result(result)

    # Compact one-liner for think/store_finding actions; detailed for the rest
    if action in ("think", "store_finding"):
        return (
            f"**[{agent}] Étape {step_num}/{max_steps}** — `{action}` "
            f"*(conf. {confidence}%)*\n"
            f"> {thought}\n"
        )

    return (
        f"**[{agent}] Étape {step_num}/{max_steps}** — `{action}` "
        f"*(conf. {confidence}%)*\n"
        f"> {thought}\n"
        f"> **Params** : `{params_str}`\n"
        f"> **Résultat** : {result_md}\n"
    )


# ---------------------------------------------------------------------------
# LLM actions
# ---------------------------------------------------------------------------

def action_save_llm(
    api_type: str,
    base_url: str,
    api_key: str,
    model: str,
    temperature: float,
    max_tokens: int,
    timeout: int,
) -> str:
    try:
        save_config({
            "llm": {
                "api_type":    api_type.lower(),
                "base_url":    base_url.strip().rstrip("/"),
                "model":       model.strip(),
                "temperature": float(temperature),
                "max_tokens":  int(max_tokens),
                "timeout":     int(timeout),
                "api_key":     api_key.strip() or "not-needed",
            }
        })
        return "✅ Configuration LLM sauvegardée."
    except Exception as e:
        return f"❌ Erreur lors de la sauvegarde : {e}"


def action_test_llm(api_type: str, base_url: str, api_key: str, model: str) -> str:
    try:
        from core.llm_client import LLMClient
        cfg = {
            "api_type":  api_type.lower(),
            "base_url":  base_url.strip().rstrip("/"),
            "model":     model.strip(),
            "api_key":   api_key.strip() or "not-needed",
            "temperature": 0.1,
            "max_tokens":  20,
            "timeout":     15,
        }
        client = LLMClient(cfg)
        ok = client.ping()
        if ok:
            return f"✅ Connexion réussie au LLM ({api_type} — {base_url})"
        return "❌ Le LLM n'a pas répondu. Vérifiez que le service est démarré."
    except Exception as e:
        return f"❌ Erreur : {e}"


def action_list_models(api_type: str, base_url: str, api_key: str) -> str:
    """Récupère la liste des modèles disponibles via l'API locale."""
    base_url = base_url.strip().rstrip("/")
    headers = {"Accept": "application/json"}
    if api_key.strip() and api_key.strip() not in ("not-needed", ""):
        headers["Authorization"] = f"Bearer {api_key.strip()}"

    if api_type.lower() == "ollama":
        url = f"{base_url}/api/tags"
    else:
        url = f"{base_url}/v1/models"

    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))

        models: List[str] = []
        if api_type.lower() == "ollama":
            models = [m.get("name", "") for m in data.get("models", [])]
        else:
            models = [m.get("id", "") for m in data.get("data", [])]

        if not models:
            return "Aucun modèle trouvé (réponse vide)."

        lines = "\n".join(f"- `{m}`" for m in sorted(models))
        return f"**{len(models)} modèle(s) disponible(s) :**\n\n{lines}"

    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        return f"❌ HTTP {e.code} : {body[:300]}"
    except Exception as e:
        return f"❌ Erreur : {e}"


# ---------------------------------------------------------------------------
# Database actions
# ---------------------------------------------------------------------------

def action_test_clickhouse(
    host: str, port: int, database: str, user: str, password: str, secure: bool
) -> str:
    try:
        from core.db_manager import ClickHouseClient
        cfg = {
            "host":     host.strip(),
            "port":     int(port),
            "database": database.strip(),
            "user":     user.strip(),
            "password": password,
            "secure":   bool(secure),
            "timeout":  10,
        }
        client = ClickHouseClient(cfg)
        if client.ping():
            tables = client.get_tables()
            return f"✅ Connexion ClickHouse réussie ! {len(tables)} table(s) trouvée(s)."
        return "❌ Connexion échouée (ping sans réponse)."
    except Exception as e:
        return f"❌ Erreur : {e}"


def action_test_oracle(
    host: str, port: int, service_name: str, user: str, password: str
) -> str:
    try:
        from core.db_manager import OracleClient
        cfg = {
            "host":         host.strip(),
            "port":         int(port),
            "service_name": service_name.strip(),
            "user":         user.strip(),
            "password":     password,
            "thick_mode":   False,
            "timeout":      10,
        }
        client = OracleClient(cfg)
        if client.ping():
            return "✅ Connexion Oracle réussie !"
        return "❌ Connexion échouée (ping sans réponse)."
    except ImportError:
        return "❌ python-oracledb non installé. Décommentez la ligne dans requirements.txt."
    except Exception as e:
        return f"❌ Erreur : {e}"


def action_save_databases(
    ch_enabled: bool, ch_host: str, ch_port: int, ch_database: str,
    ch_user: str, ch_password: str, ch_secure: bool,
    ora_enabled: bool, ora_host: str, ora_port: int, ora_service: str,
    ora_user: str, ora_password: str,
) -> str:
    try:
        save_config({
            "databases": {
                "clickhouse": {
                    "enabled":  bool(ch_enabled),
                    "host":     ch_host.strip(),
                    "port":     int(ch_port),
                    "database": ch_database.strip(),
                    "user":     ch_user.strip(),
                    "password": ch_password,
                    "secure":   bool(ch_secure),
                    "timeout":  30,
                },
                "oracle": {
                    "enabled":      bool(ora_enabled),
                    "host":         ora_host.strip(),
                    "port":         int(ora_port),
                    "service_name": ora_service.strip(),
                    "user":         ora_user.strip(),
                    "password":     ora_password,
                    "thick_mode":   False,
                    "timeout":      30,
                },
            }
        })
        return "✅ Configuration bases de données sauvegardée."
    except Exception as e:
        return f"❌ Erreur lors de la sauvegarde : {e}"


# ---------------------------------------------------------------------------
# Agent / security actions
# ---------------------------------------------------------------------------

def action_save_agents(
    max_steps: int,
    reflection_interval: int,
    parallel_agents: int,
    result_dir: str,
    allow_write: bool,
    max_rows: int,
    query_timeout: int,
) -> str:
    try:
        save_config({
            "agents": {
                "max_steps":           int(max_steps),
                "reflection_interval": int(reflection_interval),
                "parallel_agents":     int(parallel_agents),
                "result_dir":          result_dir.strip(),
            },
            "security": {
                "allow_write_queries": bool(allow_write),
                "max_rows_returned":   int(max_rows),
                "query_timeout":       int(query_timeout),
            },
        })
        return "✅ Configuration agents et sécurité sauvegardée."
    except Exception as e:
        return f"❌ Erreur lors de la sauvegarde : {e}"


def action_save_agent_overrides(*values) -> str:
    """
    Save per-agent overrides.
    Values are interleaved as: enabled, max_steps, reflection_interval — for each agent key.
    """
    keys = list(AGENT_INFO.keys())
    # Each agent gets 3 values: enabled, max_steps, reflection_interval
    if len(values) != len(keys) * 3:
        return f"❌ Nombre de valeurs incorrect ({len(values)} vs {len(keys) * 3} attendues)."
    try:
        overrides = {}
        for i, key in enumerate(keys):
            enabled    = bool(values[i * 3])
            ms         = int(values[i * 3 + 1])
            ri         = int(values[i * 3 + 2])
            overrides[key] = {
                "enabled":             enabled,
                "max_steps":           ms,
                "reflection_interval": ri,
            }
        save_config({"agent_overrides": overrides})
        return "✅ Configuration individuelle des agents sauvegardée."
    except Exception as e:
        return f"❌ Erreur : {e}"


def action_create_custom_agent(
    name: str,
    display_name: str,
    template: str,
    specialization: str,
    mission: str,
    max_steps: int,
) -> Tuple[str, Any]:
    """Create and persist a new custom agent definition."""
    name = name.strip().lower().replace(" ", "_")
    display_name = display_name.strip() or name

    if not name:
        return "❌ Le nom de l'agent est requis.", gr.update()

    # Validate name (alphanum + underscore only)
    if not re.match(r'^[a-z0-9_]+$', name):
        return "❌ Le nom ne peut contenir que des lettres minuscules, chiffres et _.", gr.update()

    # Check not duplicating a built-in agent
    if name in AGENT_INFO or name == "manager":
        return f"❌ '{name}' est un agent intégré — choisissez un autre nom.", gr.update()

    config = load_config()
    custom_agents = config.get("custom_agents", [])

    # Remove existing entry with same name
    custom_agents = [ca for ca in custom_agents if ca.get("name") != name]

    custom_agents.append({
        "name":           name,
        "display_name":   display_name,
        "template":       template,
        "specialization": specialization.strip(),
        "mission":        mission.strip(),
        "max_steps":      int(max_steps),
        "enabled":        True,
    })

    save_config({"custom_agents": custom_agents})
    new_choices = _all_agent_choices()
    return (
        f"✅ Agent **{display_name}** (`{name}`) créé avec le template `{template}`.",
        gr.update(choices=new_choices),
    )


def action_delete_custom_agent(name: str) -> Tuple[str, str, Any]:
    """Delete a custom agent by name."""
    if not name.strip():
        return "❌ Aucun agent sélectionné.", _render_custom_agents_list(), gr.update()
    config = load_config()
    custom_agents = config.get("custom_agents", [])
    new_list = [ca for ca in custom_agents if ca.get("name") != name.strip()]
    if len(new_list) == len(custom_agents):
        return f"❌ Agent '{name}' non trouvé.", _render_custom_agents_list(), gr.update()
    save_config({"custom_agents": new_list})
    new_choices = _all_agent_choices()
    return (
        f"✅ Agent `{name}` supprimé.",
        _render_custom_agents_list(),
        gr.update(choices=new_choices),
    )


def action_load_template_mission(template: str) -> str:
    """Load the default mission for a template into the mission textbox."""
    info = AGENT_INFO.get(template, {})
    return _get_mission(info.get("mission_key", ""))


def _render_custom_agents_list() -> str:
    """Render custom agents as a markdown list."""
    agents = _get_custom_agents()
    if not agents:
        return "*Aucun agent personnalisé créé pour l'instant.*"
    lines = []
    for ca in agents:
        status = "✅" if ca.get("enabled", True) else "⏸️"
        lines.append(
            f"{status} **{ca.get('display_name', ca['name'])}** "
            f"(`{ca['name']}`) — template: `{ca.get('template', '?')}` "
            f"— {ca.get('specialization', '')[:60]}"
        )
    return "\n\n".join(lines)


# ---------------------------------------------------------------------------
# Chat streaming action
# ---------------------------------------------------------------------------

def action_run_task_stream(
    message: str,
    history,
    agent_name: str,
    use_graph: bool,
    allow_write: bool,
    show_steps: bool,
):
    """
    Generator that runs the agent task in a background thread and streams
    step-by-step updates into the chatbot in real time.
    """
    if not message.strip():
        yield history, ""
        return

    config = load_config()
    if allow_write:
        config.setdefault("security", {})["allow_write_queries"] = True

    step_queue: "queue.Queue[Tuple[str, Any]]" = queue.Queue()
    result_holder: Dict[str, Any] = {}

    def on_step(info: dict):
        if show_steps:
            step_queue.put(("step", info))

    def thread_fn():
        try:
            from main import run_task
            result = run_task(
                config=config,
                task=message.strip(),
                agent=agent_name,
                allow_write=allow_write,
                use_graph=use_graph,
                step_callback=on_step if show_steps else None,
            )
            result_holder["result"] = result
        except Exception:
            result_holder["error"] = traceback.format_exc()
        finally:
            step_queue.put(("done", None))

    t = threading.Thread(target=thread_fn, daemon=True)
    t.start()

    history = list(history or [])
    accumulated = ""
    history.append((message.strip(), "⏳ *Agent en cours d'exécution…*"))
    yield history, ""

    # Stream step updates
    while True:
        try:
            typ, data = step_queue.get(timeout=300)  # 5-min hard timeout
        except queue.Empty:
            result_holder["error"] = "Timeout : l'agent n'a pas répondu en 5 minutes."
            break

        if typ == "done":
            break

        if typ == "step" and show_steps:
            accumulated += _format_step_markdown(data) + "\n"
            history[-1] = (
                message.strip(),
                accumulated + "\n\n⏳ *En cours…*",
            )
            yield history, ""

    # Build final message
    if "error" in result_holder:
        final = f"❌ **Erreur lors de l'exécution :**\n\n```\n{result_holder['error']}\n```"
    else:
        result = result_holder.get("result", {})
        answer = (
            result.get("answer")
            or result.get("final_answer")
            or result.get("summary")
            or str(result)
        )
        steps    = result.get("steps_used", "?")
        duration = result.get("duration_s") or result.get("duration", 0)
        try:
            duration_str = f"{float(duration):.1f}s"
        except Exception:
            duration_str = str(duration)

        if show_steps and accumulated:
            final = (
                accumulated
                + "\n---\n## ✅ Réponse finale\n\n"
                + str(answer)
                + f"\n\n---\n*Agent : **{agent_name}** | Étapes : {steps} | Durée : {duration_str}*"
            )
        else:
            final = (
                str(answer)
                + f"\n\n---\n*Agent : **{agent_name}** | Étapes : {steps} | Durée : {duration_str}*"
            )

    history[-1] = (message.strip(), final)
    yield history, ""


# ---------------------------------------------------------------------------
# UI builder
# ---------------------------------------------------------------------------

def build_ui() -> "gr.Blocks":
    config = load_config()
    llm  = config.get("llm", {})
    dbs  = config.get("databases", {})
    ch   = dbs.get("clickhouse", {})
    ora  = dbs.get("oracle", {})
    ag   = config.get("agents", {})
    sec  = config.get("security", {})
    overrides = config.get("agent_overrides", {})

    _theme = _get_theme()
    _css = """
        .gradio-container { max-width: 1200px; margin: auto; }
        .status-box { font-size: 0.95em; }
        .agent-card { border: 1px solid #e0e0e0; border-radius: 8px; padding: 12px; margin: 4px 0; }
        footer { display: none !important; }
    """

    blocks_kwargs = _kw(
        gr.Blocks,
        title="Python Agent — Configuration & Chat",
        theme=_theme,
        css=_css,
    )

    with gr.Blocks(**blocks_kwargs) as demo:

        gr.Markdown(
            "# Python Agent\n"
            "Configurez les connexions, paramétrez les agents et lancez vos instructions via le chat."
        )

        with gr.Tabs():

            # ─────────────────────────────────────────────────────────
            # TAB 1 — LLM
            # ─────────────────────────────────────────────────────────
            with gr.TabItem("🧠 LLM"):
                gr.Markdown("### Configuration du modèle de langage")

                with gr.Row():
                    t_api_type = gr.Dropdown(
                        **_kw(
                            gr.Dropdown,
                            choices=["ollama", "openai"],
                            value=llm.get("api_type", "ollama"),
                            label="Type d'API",
                            scale=1,
                        )
                    )
                    t_model = gr.Textbox(
                        **_kw(
                            gr.Textbox,
                            value=llm.get("model", "llama3.1:8b"),
                            label="Modèle",
                            placeholder="llama3.1:8b, gpt-4o-mini…",
                            scale=2,
                        )
                    )

                t_base_url = gr.Textbox(
                    **_kw(
                        gr.Textbox,
                        value=llm.get("base_url", "http://localhost:11434"),
                        label="URL de base (API locale HTTP)",
                        placeholder="http://localhost:11434",
                    )
                )
                t_api_key = gr.Textbox(
                    **_kw(
                        gr.Textbox,
                        value="" if llm.get("api_key", "not-needed") == "not-needed" else llm.get("api_key", ""),
                        label="Clé API",
                        placeholder="Laisser vide pour Ollama, ou entrer sk-…",
                        type="password",
                    )
                )

                with gr.Row():
                    t_temperature = gr.Slider(
                        **_kw(
                            gr.Slider,
                            minimum=0.0,
                            maximum=1.0,
                            value=llm.get("temperature", 0.1),
                            step=0.05,
                            label="Température",
                        )
                    )
                    t_max_tokens = gr.Number(
                        **_kw(gr.Number, value=llm.get("max_tokens", 4096), label="Max tokens", precision=0)
                    )
                    t_timeout = gr.Number(
                        **_kw(gr.Number, value=llm.get("timeout", 120), label="Timeout (s)", precision=0)
                    )

                with gr.Row():
                    btn_save_llm    = gr.Button(**_kw(gr.Button, value="💾 Sauvegarder", variant="primary"))
                    btn_test_llm    = gr.Button(**_kw(gr.Button, value="🔌 Tester la connexion"))
                    btn_list_models = gr.Button(**_kw(gr.Button, value="📋 Modèles disponibles"))

                llm_status    = gr.Markdown(**_kw(gr.Markdown, value="", elem_classes=["status-box"]))
                models_output = gr.Markdown(**_kw(gr.Markdown, value="", elem_classes=["status-box"]))

                btn_save_llm.click(
                    action_save_llm,
                    inputs=[t_api_type, t_base_url, t_api_key, t_model, t_temperature, t_max_tokens, t_timeout],
                    outputs=[llm_status],
                )
                btn_test_llm.click(
                    action_test_llm,
                    inputs=[t_api_type, t_base_url, t_api_key, t_model],
                    outputs=[llm_status],
                )
                btn_list_models.click(
                    action_list_models,
                    inputs=[t_api_type, t_base_url, t_api_key],
                    outputs=[models_output],
                )

            # ─────────────────────────────────────────────────────────
            # TAB 2 — Databases
            # ─────────────────────────────────────────────────────────
            with gr.TabItem("🗄️ Bases de données"):

                with _Group():
                    gr.Markdown("### ClickHouse")

                    with gr.Row():
                        ch_enabled = gr.Checkbox(value=ch.get("enabled", True), label="Activé")
                        ch_secure  = gr.Checkbox(value=ch.get("secure", False), label="HTTPS/TLS")

                    with gr.Row():
                        ch_host     = gr.Textbox(value=ch.get("host", "localhost"), label="Hôte")
                        ch_port     = gr.Number(**_kw(gr.Number, value=ch.get("port", 8123), label="Port", precision=0))
                        ch_database = gr.Textbox(value=ch.get("database", "default"), label="Base de données")

                    with gr.Row():
                        ch_user     = gr.Textbox(value=ch.get("user", "default"), label="Utilisateur")
                        ch_password = gr.Textbox(**_kw(gr.Textbox, value=ch.get("password", ""), label="Mot de passe", type="password"))

                    btn_test_ch = gr.Button(**_kw(gr.Button, value="🔌 Tester la connexion ClickHouse"))
                    ch_status   = gr.Markdown(**_kw(gr.Markdown, value="", elem_classes=["status-box"]))
                    btn_test_ch.click(
                        action_test_clickhouse,
                        inputs=[ch_host, ch_port, ch_database, ch_user, ch_password, ch_secure],
                        outputs=[ch_status],
                    )

                gr.Markdown("---")

                with _Group():
                    gr.Markdown("### Oracle")

                    ora_enabled = gr.Checkbox(value=ora.get("enabled", False), label="Activé")

                    with gr.Row():
                        ora_host    = gr.Textbox(value=ora.get("host", "localhost"), label="Hôte")
                        ora_port    = gr.Number(**_kw(gr.Number, value=ora.get("port", 1521), label="Port", precision=0))
                        ora_service = gr.Textbox(value=ora.get("service_name", "ORCL"), label="Service")

                    with gr.Row():
                        ora_user     = gr.Textbox(value=ora.get("user", "system"), label="Utilisateur")
                        ora_password = gr.Textbox(**_kw(gr.Textbox, value=ora.get("password", ""), label="Mot de passe", type="password"))

                    btn_test_ora = gr.Button(**_kw(gr.Button, value="🔌 Tester la connexion Oracle"))
                    ora_status   = gr.Markdown(**_kw(gr.Markdown, value="", elem_classes=["status-box"]))
                    btn_test_ora.click(
                        action_test_oracle,
                        inputs=[ora_host, ora_port, ora_service, ora_user, ora_password],
                        outputs=[ora_status],
                    )

                gr.Markdown("---")

                btn_save_db    = gr.Button(**_kw(gr.Button, value="💾 Sauvegarder la configuration", variant="primary"))
                db_save_status = gr.Markdown(**_kw(gr.Markdown, value="", elem_classes=["status-box"]))

                btn_save_db.click(
                    action_save_databases,
                    inputs=[
                        ch_enabled, ch_host, ch_port, ch_database, ch_user, ch_password, ch_secure,
                        ora_enabled, ora_host, ora_port, ora_service, ora_user, ora_password,
                    ],
                    outputs=[db_save_status],
                )

            # ─────────────────────────────────────────────────────────
            # TAB 3 — Agents
            # ─────────────────────────────────────────────────────────
            with gr.TabItem("⚙️ Agents"):

                # ── Section 1 : Global parameters ────────────────────
                gr.Markdown("### ⚡ Paramètres globaux d'exécution")

                with gr.Row():
                    ag_max_steps  = gr.Number(**_kw(gr.Number, value=ag.get("max_steps", 20), label="Étapes max par agent", precision=0))
                    ag_reflection = gr.Number(**_kw(gr.Number, value=ag.get("reflection_interval", 5), label="Intervalle de réflexion", precision=0))
                    ag_parallel   = gr.Number(**_kw(gr.Number, value=ag.get("parallel_agents", 1), label="Agents parallèles", precision=0))

                ag_result_dir = gr.Textbox(value=ag.get("result_dir", "./results"), label="Dossier de résultats")

                gr.Markdown("### 🔒 Sécurité & limites")

                sec_allow_write = gr.Checkbox(
                    value=sec.get("allow_write_queries", False),
                    label="Autoriser les requêtes en écriture (INSERT / UPDATE / CREATE…)",
                )

                with gr.Row():
                    sec_max_rows      = gr.Number(**_kw(gr.Number, value=sec.get("max_rows_returned", 1000), label="Lignes max retournées", precision=0))
                    sec_query_timeout = gr.Number(**_kw(gr.Number, value=sec.get("query_timeout", 30), label="Timeout requête (s)", precision=0))

                btn_save_global  = gr.Button(**_kw(gr.Button, value="💾 Sauvegarder paramètres globaux", variant="primary"))
                global_ag_status = gr.Markdown(**_kw(gr.Markdown, value="", elem_classes=["status-box"]))

                btn_save_global.click(
                    action_save_agents,
                    inputs=[ag_max_steps, ag_reflection, ag_parallel, ag_result_dir,
                            sec_allow_write, sec_max_rows, sec_query_timeout],
                    outputs=[global_ag_status],
                )

                gr.Markdown("---")

                # ── Section 2 : Per-agent configuration ──────────────
                gr.Markdown(
                    "### 🤖 Configuration individuelle des agents\n"
                    "Personnalisez chaque agent : activez/désactivez, ajustez les étapes max et l'intervalle de réflexion."
                )

                # We build one accordion per agent and collect all inputs
                per_agent_inputs: List[Any] = []

                for agent_key, info in AGENT_INFO.items():
                    ov = overrides.get(agent_key, {})
                    default_enabled    = ov.get("enabled", True)
                    default_ms         = ov.get("max_steps", info["default_max_steps"])
                    default_ri         = ov.get("reflection_interval", info["default_reflection"])

                    try:
                        with gr.Accordion(
                            label=f"{info['display']} — {info['description']}",
                            open=False,
                        ):
                            with gr.Row():
                                w_enabled = gr.Checkbox(
                                    value=default_enabled,
                                    label="✅ Activé",
                                    scale=1,
                                )
                                w_ms = gr.Number(
                                    **_kw(gr.Number,
                                          value=default_ms,
                                          label="Étapes max",
                                          precision=0,
                                          scale=1,
                                          minimum=1,
                                          maximum=100)
                                )
                                w_ri = gr.Number(
                                    **_kw(gr.Number,
                                          value=default_ri,
                                          label="Intervalle réflexion",
                                          precision=0,
                                          scale=1,
                                          minimum=1,
                                          maximum=50)
                                )
                    except Exception:
                        # Fallback if Accordion not available
                        gr.Markdown(f"**{info['display']}** — {info['description']}")
                        with gr.Row():
                            w_enabled = gr.Checkbox(value=default_enabled, label="Activé", scale=1)
                            w_ms = gr.Number(**_kw(gr.Number, value=default_ms, label="Étapes max", precision=0, scale=1))
                            w_ri = gr.Number(**_kw(gr.Number, value=default_ri, label="Intervalle réflexion", precision=0, scale=1))

                    per_agent_inputs.extend([w_enabled, w_ms, w_ri])

                btn_save_overrides  = gr.Button(**_kw(gr.Button, value="💾 Sauvegarder la configuration des agents", variant="primary"))
                overrides_status    = gr.Markdown(**_kw(gr.Markdown, value="", elem_classes=["status-box"]))

                btn_save_overrides.click(
                    action_save_agent_overrides,
                    inputs=per_agent_inputs,
                    outputs=[overrides_status],
                )

                gr.Markdown("---")

                # ── Section 3 : Create new agent ─────────────────────
                gr.Markdown(
                    "### ➕ Créer un nouvel agent\n"
                    "Créez un agent personnalisé basé sur un template existant. "
                    "Il apparaîtra dans le chat et pourra être dispatché par le Manager."
                )

                with gr.Row():
                    new_agent_template = gr.Dropdown(
                        **_kw(gr.Dropdown,
                              choices=TEMPLATE_CHOICES,
                              value="analyst",
                              label="Template de base",
                              scale=1)
                    )
                    new_agent_name = gr.Textbox(
                        **_kw(gr.Textbox,
                              placeholder="mon_agent_vente",
                              label="Nom (identifiant unique, snake_case)",
                              scale=2)
                    )
                    new_agent_display = gr.Textbox(
                        **_kw(gr.Textbox,
                              placeholder="Mon Agent Vente",
                              label="Nom affiché",
                              scale=2)
                    )

                new_agent_specialization = gr.Textbox(
                    **_kw(gr.Textbox,
                          placeholder="analyse des données de vente par région et segment client",
                          label="Spécialisation (courte description métier)",
                          lines=1)
                )
                new_agent_mission = gr.Textbox(
                    **_kw(gr.Textbox,
                          placeholder="Le template sera chargé automatiquement. Modifiez selon vos besoins.",
                          label="Mission personnalisée (optionnel — laissez vide pour utiliser celle du template)",
                          lines=6)
                )

                with gr.Row():
                    new_agent_max_steps = gr.Number(
                        **_kw(gr.Number, value=20, label="Étapes max", precision=0, scale=1)
                    )
                    btn_load_template = gr.Button(
                        **_kw(gr.Button, value="📋 Charger mission du template", scale=2)
                    )
                    btn_create_agent = gr.Button(
                        **_kw(gr.Button, value="➕ Créer l'agent", variant="primary", scale=2)
                    )

                create_agent_status = gr.Markdown(**_kw(gr.Markdown, value="", elem_classes=["status-box"]))

                btn_load_template.click(
                    action_load_template_mission,
                    inputs=[new_agent_template],
                    outputs=[new_agent_mission],
                )

                gr.Markdown("---")

                # ── Section 4 : Existing custom agents ───────────────
                gr.Markdown("### 📋 Agents personnalisés existants")

                custom_agents_display = gr.Markdown(
                    value=_render_custom_agents_list(),
                    elem_classes=["status-box"],
                )

                with gr.Row():
                    del_agent_name = gr.Textbox(
                        **_kw(gr.Textbox,
                              placeholder="nom_de_l_agent_a_supprimer",
                              label="Nom de l'agent à supprimer",
                              scale=3)
                    )
                    btn_delete_agent = gr.Button(
                        **_kw(gr.Button, value="🗑️ Supprimer", variant="stop", scale=1)
                    )

                delete_status = gr.Markdown(**_kw(gr.Markdown, value="", elem_classes=["status-box"]))

                # We need a reference to the chat dropdown to update it when agents change.
                # We create it inside the Chat tab but forward-declare a placeholder here.
                # The actual dropdown is wired below after creation.

            # ─────────────────────────────────────────────────────────
            # TAB 4 — Chat
            # ─────────────────────────────────────────────────────────
            with gr.TabItem("💬 Chat"):
                gr.Markdown(
                    "### Lancer une instruction\n"
                    "Les étapes de raisonnement de l'agent sont affichées en temps réel si *Afficher les étapes* est activé."
                )

                with gr.Row():
                    chat_agent = gr.Dropdown(
                        **_kw(
                            gr.Dropdown,
                            choices=_all_agent_choices(),
                            value="manager",
                            label="Agent",
                            scale=2,
                        )
                    )
                    chat_use_graph = gr.Checkbox(
                        **_kw(gr.Checkbox, value=False, label="LangGraph (multi-agents)", scale=1)
                    )
                    chat_allow_write = gr.Checkbox(
                        **_kw(gr.Checkbox, value=False, label="Autoriser écritures SQL", scale=1)
                    )
                    chat_show_steps = gr.Checkbox(
                        **_kw(gr.Checkbox, value=True, label="Afficher les étapes", scale=1)
                    )

                chatbot = gr.Chatbot(
                    **_kw(
                        gr.Chatbot,
                        label="Conversation",
                        height=560,
                        render_markdown=True,
                        bubble_full_width=False,
                    )
                )

                with gr.Row():
                    msg_input = gr.Textbox(
                        **_kw(
                            gr.Textbox,
                            placeholder="Entrez votre instruction ici… (Entrée pour envoyer, Shift+Entrée pour nouvelle ligne)",
                            label="",
                            show_label=False,
                            lines=3,
                            scale=5,
                        )
                    )
                    send_btn = gr.Button(
                        **_kw(gr.Button, value="Envoyer ▶", variant="primary", scale=1)
                    )

                clear_btn = gr.Button(**_kw(gr.Button, value="🗑️ Effacer la conversation", size="sm"))

                # Wire streaming events
                send_btn.click(
                    action_run_task_stream,
                    inputs=[msg_input, chatbot, chat_agent, chat_use_graph, chat_allow_write, chat_show_steps],
                    outputs=[chatbot, msg_input],
                )
                msg_input.submit(
                    action_run_task_stream,
                    inputs=[msg_input, chatbot, chat_agent, chat_use_graph, chat_allow_write, chat_show_steps],
                    outputs=[chatbot, msg_input],
                )
                clear_btn.click(lambda: ([], ""), outputs=[chatbot, msg_input])

        # ── Wire create/delete agent → update chat dropdown ───────────
        btn_create_agent.click(
            action_create_custom_agent,
            inputs=[
                new_agent_name, new_agent_display, new_agent_template,
                new_agent_specialization, new_agent_mission, new_agent_max_steps,
            ],
            outputs=[create_agent_status, chat_agent],
        )

        btn_delete_agent.click(
            action_delete_custom_agent,
            inputs=[del_agent_name],
            outputs=[delete_status, custom_agents_display, chat_agent],
        )

    # Enable queueing for generator (streaming) support
    try:
        demo.queue()
    except Exception:
        pass  # very old Gradio without queue support

    return demo


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Python Agent — Interface graphique")
    parser.add_argument("--host",  default="127.0.0.1", help="Adresse d'écoute (défaut: 127.0.0.1)")
    parser.add_argument("--port",  type=int, default=7860, help="Port HTTP (défaut: 7860)")
    parser.add_argument("--share", action="store_true", help="Créer un lien public temporaire via Gradio")
    args = parser.parse_args()

    demo = build_ui()
    print(f"\n Python Agent UI — http://{args.host}:{args.port}\n")

    launch_kwargs = _kw(
        demo.launch,
        server_name=args.host,
        server_port=args.port,
        share=args.share,
        inbrowser=True,
        show_error=True,
    )
    demo.launch(**launch_kwargs)


if __name__ == "__main__":
    main()
