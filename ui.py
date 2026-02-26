#!/usr/bin/env python3
"""
ui.py — Interface graphique Gradio pour Python Agent
=====================================================
Permet de configurer les bases de données, le LLM, les agents,
et de lancer des instructions via un chat.

Usage:
  python ui.py
  python ui.py --port 8080
  python ui.py --host 0.0.0.0 --share
"""
import json
import os
import re
import sys
import argparse
import traceback
import urllib.request
import urllib.error
from pathlib import Path
from typing import Any, Dict, List, Tuple

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
# Helpers config
# ---------------------------------------------------------------------------

def _strip_config_comments(raw: str) -> str:
    """Supprime les commentaires // et les clés _comment du JSON."""
    # Supprime // hors des strings (après whitespace)
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
    # Preserve existing config to avoid overwriting sections we don't manage
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
    else:  # openai-compatible (LM Studio, vLLM, LocalAI…)
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


# ---------------------------------------------------------------------------
# Chat actions
# ---------------------------------------------------------------------------

def action_run_task(
    message: str,
    history: List[Tuple[str, str]],
    agent_name: str,
    use_graph: bool,
    allow_write: bool,
) -> Tuple[List[Tuple[str, str]], str]:
    """Exécute une instruction via l'agent sélectionné et retourne l'historique."""
    if not message.strip():
        return history, ""

    config = load_config()
    if allow_write:
        config.setdefault("security", {})["allow_write_queries"] = True

    try:
        from main import run_task
        result = run_task(
            config=config,
            task=message.strip(),
            agent=agent_name,
            allow_write=allow_write,
            use_graph=use_graph,
        )

        # Extract the best available answer text
        answer = (
            result.get("answer")
            or result.get("final_answer")
            or result.get("summary")
            or str(result)
        )

        # Append metadata summary
        steps = result.get("steps_used", "?")
        duration = result.get("duration", 0)
        meta = f"\n\n---\n*Agent : **{agent_name}** | Étapes : {steps} | Durée : {duration:.1f}s*"
        answer = str(answer) + meta

    except Exception as e:
        answer = f"❌ **Erreur lors de l'exécution :**\n\n```\n{traceback.format_exc()}\n```"

    history = list(history or [])
    history.append((message.strip(), answer))
    return history, ""


# ---------------------------------------------------------------------------
# UI builder
# ---------------------------------------------------------------------------

def build_ui() -> gr.Blocks:
    config = load_config()
    llm = config.get("llm", {})
    dbs = config.get("databases", {})
    ch  = dbs.get("clickhouse", {})
    ora = dbs.get("oracle", {})
    ag  = config.get("agents", {})
    sec = config.get("security", {})

    with gr.Blocks(
        title="Python Agent — Configuration & Chat",
        theme=gr.themes.Soft(),
        css="""
            .gradio-container { max-width: 1100px; margin: auto; }
            .status-box { font-size: 0.95em; }
            footer { display: none !important; }
        """,
    ) as demo:

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
                        choices=["ollama", "openai"],
                        value=llm.get("api_type", "ollama"),
                        label="Type d'API",
                        scale=1,
                    )
                    t_model = gr.Textbox(
                        value=llm.get("model", "llama3.1:8b"),
                        label="Modèle",
                        placeholder="llama3.1:8b, gpt-4o-mini…",
                        scale=2,
                    )

                t_base_url = gr.Textbox(
                    value=llm.get("base_url", "http://localhost:11434"),
                    label="URL de base (API locale HTTP)",
                    placeholder="http://localhost:11434",
                )
                t_api_key = gr.Textbox(
                    value="" if llm.get("api_key", "not-needed") == "not-needed" else llm.get("api_key", ""),
                    label="Clé API",
                    placeholder="Laisser vide pour Ollama, ou entrer sk-…",
                    type="password",
                )

                with gr.Row():
                    t_temperature = gr.Slider(
                        minimum=0.0, maximum=1.0,
                        value=llm.get("temperature", 0.1),
                        step=0.05, label="Température",
                    )
                    t_max_tokens = gr.Number(
                        value=llm.get("max_tokens", 4096),
                        label="Max tokens", precision=0,
                    )
                    t_timeout = gr.Number(
                        value=llm.get("timeout", 120),
                        label="Timeout (s)", precision=0,
                    )

                with gr.Row():
                    btn_save_llm   = gr.Button("💾 Sauvegarder", variant="primary")
                    btn_test_llm   = gr.Button("🔌 Tester la connexion")
                    btn_list_models = gr.Button("📋 Modèles disponibles")

                llm_status     = gr.Markdown(elem_classes=["status-box"])
                models_output  = gr.Markdown(elem_classes=["status-box"])

                btn_save_llm.click(
                    action_save_llm,
                    inputs=[t_api_type, t_base_url, t_api_key, t_model,
                            t_temperature, t_max_tokens, t_timeout],
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

                # ── ClickHouse ──────────────────────────────────────
                with gr.Group():
                    gr.Markdown("### ClickHouse")

                    with gr.Row():
                        ch_enabled = gr.Checkbox(
                            value=ch.get("enabled", True), label="Activé"
                        )
                        ch_secure = gr.Checkbox(
                            value=ch.get("secure", False), label="HTTPS/TLS"
                        )

                    with gr.Row():
                        ch_host = gr.Textbox(
                            value=ch.get("host", "localhost"), label="Hôte"
                        )
                        ch_port = gr.Number(
                            value=ch.get("port", 8123), label="Port", precision=0
                        )
                        ch_database = gr.Textbox(
                            value=ch.get("database", "default"), label="Base de données"
                        )

                    with gr.Row():
                        ch_user = gr.Textbox(
                            value=ch.get("user", "default"), label="Utilisateur"
                        )
                        ch_password = gr.Textbox(
                            value=ch.get("password", ""),
                            label="Mot de passe", type="password"
                        )

                    btn_test_ch = gr.Button("🔌 Tester la connexion ClickHouse")
                    ch_status   = gr.Markdown(elem_classes=["status-box"])

                    btn_test_ch.click(
                        action_test_clickhouse,
                        inputs=[ch_host, ch_port, ch_database, ch_user, ch_password, ch_secure],
                        outputs=[ch_status],
                    )

                gr.Markdown("---")

                # ── Oracle ──────────────────────────────────────────
                with gr.Group():
                    gr.Markdown("### Oracle")

                    ora_enabled = gr.Checkbox(
                        value=ora.get("enabled", False), label="Activé"
                    )

                    with gr.Row():
                        ora_host = gr.Textbox(
                            value=ora.get("host", "localhost"), label="Hôte"
                        )
                        ora_port = gr.Number(
                            value=ora.get("port", 1521), label="Port", precision=0
                        )
                        ora_service = gr.Textbox(
                            value=ora.get("service_name", "ORCL"), label="Service"
                        )

                    with gr.Row():
                        ora_user = gr.Textbox(
                            value=ora.get("user", "system"), label="Utilisateur"
                        )
                        ora_password = gr.Textbox(
                            value=ora.get("password", ""),
                            label="Mot de passe", type="password"
                        )

                    btn_test_ora = gr.Button("🔌 Tester la connexion Oracle")
                    ora_status   = gr.Markdown(elem_classes=["status-box"])

                    btn_test_ora.click(
                        action_test_oracle,
                        inputs=[ora_host, ora_port, ora_service, ora_user, ora_password],
                        outputs=[ora_status],
                    )

                gr.Markdown("---")

                btn_save_db  = gr.Button("💾 Sauvegarder la configuration", variant="primary")
                db_save_status = gr.Markdown(elem_classes=["status-box"])

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
                gr.Markdown("### Paramètres d'exécution des agents")

                with gr.Row():
                    ag_max_steps = gr.Number(
                        value=ag.get("max_steps", 20),
                        label="Étapes max par agent", precision=0,
                    )
                    ag_reflection = gr.Number(
                        value=ag.get("reflection_interval", 5),
                        label="Intervalle de réflexion", precision=0,
                    )
                    ag_parallel = gr.Number(
                        value=ag.get("parallel_agents", 1),
                        label="Agents parallèles", precision=0,
                    )

                ag_result_dir = gr.Textbox(
                    value=ag.get("result_dir", "./results"),
                    label="Dossier de résultats",
                )

                gr.Markdown("### Sécurité & limites")

                with gr.Row():
                    sec_allow_write = gr.Checkbox(
                        value=sec.get("allow_write_queries", False),
                        label="Autoriser les requêtes en écriture (INSERT / UPDATE / CREATE…)",
                    )

                with gr.Row():
                    sec_max_rows = gr.Number(
                        value=sec.get("max_rows_returned", 1000),
                        label="Lignes max retournées", precision=0,
                    )
                    sec_query_timeout = gr.Number(
                        value=sec.get("query_timeout", 30),
                        label="Timeout requête (s)", precision=0,
                    )

                btn_save_agents  = gr.Button("💾 Sauvegarder", variant="primary")
                agents_status    = gr.Markdown(elem_classes=["status-box"])

                btn_save_agents.click(
                    action_save_agents,
                    inputs=[
                        ag_max_steps, ag_reflection, ag_parallel, ag_result_dir,
                        sec_allow_write, sec_max_rows, sec_query_timeout,
                    ],
                    outputs=[agents_status],
                )

            # ─────────────────────────────────────────────────────────
            # TAB 4 — Chat
            # ─────────────────────────────────────────────────────────
            with gr.TabItem("💬 Chat"):
                gr.Markdown("### Lancer une instruction")

                with gr.Row():
                    chat_agent = gr.Dropdown(
                        choices=[
                            "manager",
                            "analyst",
                            "quality",
                            "pattern",
                            "query",
                            "sql_analyst",
                            "clickhouse_generic",
                            "clickhouse_table_manager",
                            "clickhouse_writer",
                            "clickhouse_specific",
                            "text_to_sql_translator",
                        ],
                        value="manager",
                        label="Agent",
                        scale=2,
                    )
                    chat_use_graph = gr.Checkbox(
                        value=False,
                        label="Utiliser LangGraph (multi-agents)",
                        scale=1,
                    )
                    chat_allow_write = gr.Checkbox(
                        value=False,
                        label="Autoriser écritures SQL",
                        scale=1,
                    )

                chatbot = gr.Chatbot(
                    label="Conversation",
                    height=480,
                    render_markdown=True,
                    bubble_full_width=False,
                )

                with gr.Row():
                    msg_input = gr.Textbox(
                        placeholder="Entrez votre instruction ici… (Shift+Entrée pour nouvelle ligne)",
                        label="",
                        show_label=False,
                        lines=3,
                        scale=5,
                    )
                    send_btn = gr.Button("Envoyer ▶", variant="primary", scale=1)

                clear_btn = gr.Button("🗑️ Effacer la conversation", size="sm")

                # Wire events
                send_btn.click(
                    action_run_task,
                    inputs=[msg_input, chatbot, chat_agent, chat_use_graph, chat_allow_write],
                    outputs=[chatbot, msg_input],
                )
                msg_input.submit(
                    action_run_task,
                    inputs=[msg_input, chatbot, chat_agent, chat_use_graph, chat_allow_write],
                    outputs=[chatbot, msg_input],
                )
                clear_btn.click(lambda: ([], ""), outputs=[chatbot, msg_input])

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
    demo.launch(
        server_name=args.host,
        server_port=args.port,
        share=args.share,
        inbrowser=True,
        show_error=True,
    )


if __name__ == "__main__":
    main()
