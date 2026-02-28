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
        "detailed_description": (
            "Analyse statistique approfondie des données en base de données. "
            "Calcule KPIs, moyennes, médianes, percentiles, tendances temporelles et comparaisons entre groupes. "
            "À choisir quand : 'Quelle est la tendance des ventes ?', 'Calcule la moyenne par catégorie', "
            "'Analyse l'évolution sur les 30 derniers jours', 'Quels sont les KPIs principaux ?'."
        ),
        "default_max_steps": 15,
        "default_reflection": 5,
        "category": "base",
        "mission_key": "ANALYST_MISSION",
    },
    "quality": {
        "display": "🔍 Quality",
        "description": "Audit qualité des données (nulls, doublons, outliers)",
        "detailed_description": (
            "Audit complet de la qualité des données : valeurs nulles, doublons, outliers, "
            "incohérences de types et violations de contraintes. "
            "À choisir quand : 'Vérifie la qualité de la table X', 'Y a-t-il des doublons dans les commandes ?', "
            "'Trouve les valeurs manquantes ou aberrantes', 'Rapport de qualité sur ce dataset'."
        ),
        "default_max_steps": 15,
        "default_reflection": 5,
        "category": "base",
        "mission_key": "QUALITY_MISSION",
    },
    "pattern": {
        "display": "🔮 Pattern",
        "description": "Découverte de patterns, corrélations, anomalies",
        "detailed_description": (
            "Découverte de patterns cachés dans les données : corrélations entre colonnes, segmentation en clusters, "
            "anomalies statistiques, distributions et séquences répétitives. "
            "À choisir quand : 'Quels facteurs sont corrélés aux achats ?', 'Identifie des comportements anormaux', "
            "'Trouve des groupes d'utilisateurs similaires', 'Analyse de la distribution des valeurs'."
        ),
        "default_max_steps": 15,
        "default_reflection": 5,
        "category": "base",
        "mission_key": "PATTERN_MISSION",
    },
    "query": {
        "display": "⚡ Query",
        "description": "Construction et optimisation SQL",
        "detailed_description": (
            "Construction et optimisation de requêtes SQL complexes pour ClickHouse ou Oracle. "
            "Génère du SQL performant, optimise les jointures et index, reformule des requêtes lentes. "
            "À choisir quand : 'Écris une requête SQL pour...', 'Optimise cette requête', "
            "'Comment écrire ce calcul en SQL ?', 'Génère du SQL pour ce besoin métier'."
        ),
        "default_max_steps": 15,
        "default_reflection": 5,
        "category": "base",
        "mission_key": "QUERY_MISSION",
    },
    "sql_analyst": {
        "display": "🐘 SQL Analyst",
        "description": "Expert SQL ClickHouse senior avec EXPLAIN preflight",
        "detailed_description": (
            "Expert SQL ClickHouse senior : génère des requêtes optimisées avec les fonctions natives ClickHouse "
            "(uniqHLL12, quantileTDigest, groupArray, combinators…), effectue un EXPLAIN preflight et corrige "
            "automatiquement les erreurs. "
            "À choisir quand : 'Écris une requête ClickHouse optimisée pour...', "
            "'Utilise les fonctions natives CH', analyse ClickHouse avancée nécessitant des optimisations moteur."
        ),
        "default_max_steps": 20,
        "default_reflection": 5,
        "category": "clickhouse",
        "mission_key": "CH_SQL_ANALYST_MISSION",
    },
    "clickhouse_generic": {
        "display": "🔄 CH Generic",
        "description": "Analyste ClickHouse polyvalent avec décomposition DAG",
        "detailed_description": (
            "Analyste ClickHouse polyvalent capable de décomposer des tâches complexes en sous-requêtes "
            "via un graphe d'exécution (DAG). Explore le schéma, enchaîne des analyses multi-tables, "
            "adapte sa stratégie en cours de route. "
            "À choisir quand : 'Analyse complexe en plusieurs étapes', 'Croise plusieurs tables ClickHouse', "
            "'Exploration globale du schéma avec analyses multiples', tâche d'analyse ouverte sur ClickHouse."
        ),
        "default_max_steps": 20,
        "default_reflection": 5,
        "category": "clickhouse",
        "mission_key": "CH_GENERIC_MISSION",
    },
    "clickhouse_table_manager": {
        "display": "🗂️ Table Manager",
        "description": "Administrateur DDL ClickHouse (CREATE/ALTER avec garde-fous)",
        "detailed_description": (
            "Administrateur DDL ClickHouse avec garde-fous intégrés : choisit le bon moteur de stockage "
            "(MergeTree, ReplicatedMergeTree, AggregatingMergeTree…), configure TTL, ORDER BY, PARTITION BY. "
            "DROP et TRUNCATE toujours bloqués. "
            "À choisir quand : 'Crée une table pour stocker ces données', 'Modifie le schéma de la table X', "
            "'Ajoute une colonne', 'Conseille sur la meilleure structure de table ClickHouse'."
        ),
        "default_max_steps": 15,
        "default_reflection": 5,
        "category": "clickhouse",
        "mission_key": "CH_TABLE_MANAGER_MISSION",
    },
    "clickhouse_writer": {
        "display": "✍️ Writer",
        "description": "DML sécurisé — INSERT restreint aux tables agent_*",
        "detailed_description": (
            "Agent DML sécurisé pour l'écriture dans ClickHouse. Limité aux opérations INSERT avec "
            "préfixe de table obligatoire (agent_*). Vérifie la compatibilité des schémas avant insertion. "
            "À choisir quand : 'Insère ces données dans ClickHouse', "
            "'Charge ce dataset dans la table agent_X', 'Écris les résultats de l'analyse dans ClickHouse'."
        ),
        "default_max_steps": 15,
        "default_reflection": 5,
        "category": "clickhouse",
        "mission_key": "CH_WRITER_MISSION",
    },
    "clickhouse_specific": {
        "display": "📋 Specific",
        "description": "Exécution de templates paramétrés (P1-P5)",
        "detailed_description": (
            "Exécuteur de templates ClickHouse paramétrés. Connaît les templates intégrés P1–P5 "
            "(P1: DAU, P2: funnel de conversion, P3: rétention, P4: top événements, P5: percentiles de sessions) "
            "et les templates personnalisés définis dans la configuration. "
            "À choisir quand : 'Calcule le DAU', 'Analyse le funnel de conversion', "
            "'Taux de rétention sur 30 jours', 'Exécute le template X avec les paramètres Y'."
        ),
        "default_max_steps": 15,
        "default_reflection": 5,
        "category": "clickhouse",
        "mission_key": "CH_SPECIFIC_MISSION",
    },
    "text_to_sql_translator": {
        "display": "🗣️ Text-to-SQL",
        "description": "Traduction langage naturel → SQL ClickHouse optimisé",
        "detailed_description": (
            "Traducteur langage naturel → SQL ClickHouse, alimenté par une couche sémantique configurable "
            "(termes métier, alias de colonnes, formules KPI comme DAU, ARPU, CVR). "
            "Corrige automatiquement les erreurs de syntaxe. "
            "À choisir quand : 'Transforme cette phrase en SQL', 'Requête pour [description business]', "
            "'Comment exprimer cela en SQL ClickHouse ?', traduction d'un besoin métier en requête."
        ),
        "default_max_steps": 15,
        "default_reflection": 5,
        "category": "clickhouse",
        "mission_key": "CH_TEXT_TO_SQL_MISSION",
    },
    # ── File agents ───────────────────────────────────────────────────────────
    "excel": {
        "display": "📊 Excel",
        "description": "Créer, lire et modifier des fichiers Excel (.xlsx) — formules, formats, feuilles",
        "detailed_description": (
            "Création, lecture et modification de classeurs Excel (.xlsx) avec openpyxl. "
            "Peut créer des feuilles, des formules, des graphiques, des styles et des mises en forme conditionnelles. "
            "À choisir quand : 'Génère un rapport Excel', 'Ajoute une feuille à ce fichier', "
            "'Crée un tableau avec des formules', 'Exporte ces données vers Excel avec mise en forme'."
        ),
        "default_max_steps": 25,
        "default_reflection": 5,
        "category": "file",
        "mission_key": "EXCEL_AGENT_MISSION",
    },
    "textfile": {
        "display": "📝 TextFile",
        "description": "Créer, lire et modifier des fichiers texte (.txt, .csv, .log, .json…)",
        "detailed_description": (
            "Création, lecture et édition de fichiers texte : .txt, .csv, .log, .json, .xml, .md "
            "via la bibliothèque standard Python (pas de dépendances). "
            "À choisir quand : 'Sauvegarde ces données en CSV', 'Lis ce fichier JSON', "
            "'Crée un rapport en Markdown', 'Écris dans un fichier texte', 'Modifie ce fichier de configuration'."
        ),
        "default_max_steps": 25,
        "default_reflection": 5,
        "category": "file",
        "mission_key": "TEXT_AGENT_MISSION",
    },
    "filesystem": {
        "display": "🗂️ FileSystem",
        "description": "Navigation, recherche cross-répertoires, ouverture OS, ingestion ClickHouse",
        "detailed_description": (
            "Navigation cross-plateforme des systèmes de fichiers : recherche de fichiers par nom, type ou contenu, "
            "analyse de répertoires, lecture de fichiers, ingestion directe vers ClickHouse. "
            "À choisir quand : 'Liste les fichiers dans /data', 'Trouve tous les CSV modifiés aujourd'hui', "
            "'Charge ce fichier dans ClickHouse', 'Quelle est la taille de ce dossier ?', "
            "'Recherche récursive d'un fichier contenant ce texte'."
        ),
        "default_max_steps": 30,
        "default_reflection": 5,
        "category": "file",
        "mission_key": "FILESYSTEM_AGENT_MISSION",
    },
    # ── RAG agent ─────────────────────────────────────────────────────────────
    "rag_json": {
        "display": "🔎 RAG JSON",
        "description": "Recherche par similarité TF-IDF dans une base de connaissances JSON",
        "detailed_description": (
            "Moteur de recherche sémantique par similarité TF-IDF dans une base de connaissances JSON locale. "
            "Retrouve les entrées les plus pertinentes par rapport à une requête en langage naturel. "
            "À choisir quand : 'Recherche dans la documentation', 'Que signifie ce terme selon la base de connaissance ?', "
            "'Trouve les entrées similaires à...', toute requête portant sur le fichier JSON de connaissances configuré."
        ),
        "default_max_steps": 15,
        "default_reflection": 5,
        "category": "rag",
        "mission_key": "RAG_JSON_MISSION",
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
        client.complete([{"role": "user", "content": "ping"}])
        return f"✅ Connexion réussie au LLM ({api_type} — {base_url})"
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
    allow_delete: bool = False,
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
                "allow_delete":        bool(allow_delete),
            },
        })
        return "✅ Configuration agents et sécurité sauvegardée."
    except Exception as e:
        return f"❌ Erreur lors de la sauvegarde : {e}"


def action_save_agent_overrides(*values) -> str:
    """
    Save per-agent overrides.
    Values are interleaved as: enabled, max_steps, reflection_interval, description — for each agent key.
    """
    keys = list(AGENT_INFO.keys())
    # Each agent gets 4 values: enabled, max_steps, reflection_interval, description
    if len(values) != len(keys) * 4:
        return f"❌ Nombre de valeurs incorrect ({len(values)} vs {len(keys) * 4} attendues)."
    try:
        overrides = {}
        for i, key in enumerate(keys):
            enabled = bool(values[i * 4])
            ms      = int(values[i * 4 + 1])
            ri      = int(values[i * 4 + 2])
            desc    = str(values[i * 4 + 3]).strip()
            overrides[key] = {
                "enabled":             enabled,
                "max_steps":           ms,
                "reflection_interval": ri,
                "description":         desc,
            }
        save_config({"agent_overrides": overrides})
        return "✅ Configuration individuelle des agents sauvegardée (descriptions incluses)."
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
    history.append({"role": "user", "content": message.strip()})
    history.append({"role": "assistant", "content": "⏳ *Agent en cours d'exécution…*"})
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
            history[-1] = {
                "role": "assistant",
                "content": accumulated + "\n\n⏳ *En cours…*",
            }
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

    history[-1] = {"role": "assistant", "content": final}
    yield history, ""


# ---------------------------------------------------------------------------
# Feature 1: Prompt Library actions
# ---------------------------------------------------------------------------

def action_prompt_save(name: str, prompt: str, description: str, agent: str, tags: str) -> str:
    try:
        from core.prompt_library import PromptLibrary
        lib = PromptLibrary()
        tag_list = [t.strip() for t in tags.split(",") if t.strip()] if tags else []
        entry = lib.save(name=name.strip(), prompt=prompt.strip(),
                         description=description.strip(), agent=agent, tags=tag_list)
        return f"✅ Prompt **{entry['name']}** sauvegardé (agent: {entry['agent']}, tags: {tag_list or '—'})."
    except Exception as e:
        return f"❌ Erreur : {e}"


def action_prompt_list() -> str:
    try:
        from core.prompt_library import PromptLibrary
        lib = PromptLibrary()
        prompts = lib.list_all()
        if not prompts:
            return "*Aucun prompt sauvegardé.*"
        lines = []
        for p in prompts:
            tags = ", ".join(p.get("tags", [])) or "—"
            lines.append(
                f"**[{p['name']}]** agent=`{p['agent']}` runs={p.get('run_count',0)}\n"
                f"> {p.get('description') or p['prompt'][:80]}\n"
                f"> Tags: {tags}"
            )
        return "\n\n---\n".join(lines)
    except Exception as e:
        return f"❌ Erreur : {e}"


def action_prompt_delete(name: str) -> str:
    try:
        from core.prompt_library import PromptLibrary
        lib = PromptLibrary()
        if lib.delete(name.strip()):
            return f"✅ Prompt **{name}** supprimé."
        return f"❌ Prompt **{name}** introuvable."
    except Exception as e:
        return f"❌ Erreur : {e}"


def action_prompt_run(name: str, agent_override: str, variables_str: str, history) -> Tuple[Any, str]:
    """Run a saved prompt from the library and stream results to chat."""
    try:
        from core.prompt_library import PromptLibrary
        lib = PromptLibrary()
        entry = lib.get(name.strip())
        if not entry:
            return history, f"❌ Prompt '{name}' introuvable."
        variables = {}
        for line in (variables_str or "").splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                variables[k.strip()] = v.strip()
        rendered = lib.render(name.strip(), variables)
        agent = agent_override.strip() or entry["agent"]
        lib.increment_run_count(name.strip())
        # Return the rendered prompt to the chat input
        return history, rendered
    except Exception as e:
        return history, f"❌ Erreur : {e}"


# ---------------------------------------------------------------------------
# Feature 2: Prompt Chaining actions
# ---------------------------------------------------------------------------

def action_chain_run(chain_text: str, agent: str, pass_result: bool, history) -> Tuple[Any, str, str]:
    """Parse chain text and run it sequentially, return combined result."""
    try:
        from core.prompt_queue import PromptQueue, PromptChainItem
        from main import run_task, load_config

        lines = [l.strip() for l in chain_text.strip().splitlines() if l.strip()]
        if not lines:
            return history, "❌ Aucun prompt dans la chaîne.", ""

        config = load_config()
        pq = PromptQueue()
        for i, prompt in enumerate(lines):
            pq.add(PromptChainItem(
                prompt=prompt,
                agent=agent,
                pass_result=pass_result and i > 0,
                label=f"Étape {i+1}",
            ))

        results = pq.run(run_task, config)

        # Build summary
        parts = []
        for r in results:
            status = "✅" if r["status"] == "ok" else "❌"
            answer = ""
            if isinstance(r["result"], dict):
                answer = r["result"].get("answer") or r["result"].get("summary", "")
            parts.append(
                f"{status} **{r['label']}** ({r['duration']}s)\n> {str(answer)[:300]}"
            )

        summary = f"**Chaîne terminée — {len(results)} étapes**\n\n" + "\n\n".join(parts)
        history = list(history or [])
        history.append({"role": "user", "content": f"[Chaîne] {len(lines)} prompts"})
        history.append({"role": "assistant", "content": summary})
        return history, summary, ""
    except Exception as e:
        return history, f"❌ Erreur : {e}", ""


# ---------------------------------------------------------------------------
# Feature 3: Scheduler actions
# ---------------------------------------------------------------------------

def action_scheduler_list() -> str:
    try:
        from core.scheduler import PromptScheduler
        sched = PromptScheduler()
        jobs = sched.list_jobs()
        if not jobs:
            return "*Aucun job planifié.*"
        lines = []
        for j in jobs:
            status = "🟢" if j["enabled"] else "🔴"
            lines.append(
                f"{status} **[{j['job_id']}] {j['name']}**\n"
                f"> Type: `{j['schedule_type']}` | Valeur: `{j['schedule_value']}`\n"
                f"> Exécutions: {j['run_count']} | Prochain: {j['next_run'] or 'N/A'}\n"
                f"> Prompts: {len(j['prompts'])} prompt(s)"
            )
        return "\n\n---\n".join(lines)
    except Exception as e:
        return f"❌ Erreur : {e}"


def action_scheduler_add(
    job_name: str, schedule_type: str, schedule_value: str,
    prompt_text: str, agent: str
) -> str:
    try:
        import uuid
        from core.scheduler import PromptScheduler, ScheduledJob
        from core.prompt_queue import PromptChainItem

        if not prompt_text.strip():
            return "❌ Le texte du prompt est requis."
        if not schedule_value.strip():
            return "❌ La valeur de planification est requise."

        job_id = str(uuid.uuid4())[:8]
        prompts = []
        for line in prompt_text.strip().splitlines():
            line = line.strip()
            if line:
                prompts.append(PromptChainItem(prompt=line, agent=agent).to_dict())

        job = ScheduledJob(
            job_id=job_id,
            name=job_name.strip() or f"job_{job_id}",
            prompts=prompts,
            schedule_type=schedule_type,
            schedule_value=schedule_value.strip(),
            agent=agent,
        )
        sched = PromptScheduler()
        sched.add_job(job)
        return (
            f"✅ Job **{job.name}** ajouté (ID: `{job_id}`).\n"
            f"> Type: `{schedule_type}` | Valeur: `{schedule_value}`\n"
            f"> Prochain: {job.next_run}"
        )
    except Exception as e:
        return f"❌ Erreur : {e}"


def action_scheduler_remove(job_id: str) -> str:
    try:
        from core.scheduler import PromptScheduler
        sched = PromptScheduler()
        if sched.remove_job(job_id.strip()):
            return f"✅ Job `{job_id}` supprimé."
        return f"❌ Job `{job_id}` introuvable."
    except Exception as e:
        return f"❌ Erreur : {e}"


def action_scheduler_toggle(job_id: str, enable: bool) -> str:
    try:
        from core.scheduler import PromptScheduler
        sched = PromptScheduler()
        sched.enable_job(job_id.strip(), enable)
        state = "activé" if enable else "désactivé"
        return f"✅ Job `{job_id}` {state}."
    except Exception as e:
        return f"❌ Erreur : {e}"


# ---------------------------------------------------------------------------
# Feature 4: Event Watcher actions
# ---------------------------------------------------------------------------

def action_watcher_list() -> str:
    try:
        from core.event_watcher import EventWatcherManager
        mgr = EventWatcherManager()
        triggers = mgr.list_triggers()
        if not triggers:
            return "*Aucun trigger de surveillance configuré.*"
        lines = []
        for t in triggers:
            status = "🟢" if t["enabled"] else "🔴"
            lines.append(
                f"{status} **[{t['trigger_id']}] {t['name']}**\n"
                f"> Dossier: `{t['watch_path']}`\n"
                f"> Patterns: `{', '.join(t['patterns'])}`\n"
                f"> Événements: {', '.join(t['event_types'])} | Déclenchements: {t['run_count']}"
            )
        return "\n\n---\n".join(lines)
    except Exception as e:
        return f"❌ Erreur : {e}"


def action_watcher_add(
    name: str, watch_path: str, patterns_str: str,
    agent: str, prompt_text: str, recursive: bool
) -> str:
    try:
        import uuid
        from core.event_watcher import EventWatcherManager, EventTrigger
        from core.prompt_queue import PromptChainItem

        if not watch_path.strip():
            return "❌ Le chemin du dossier est requis."
        if not prompt_text.strip():
            return "❌ Le prompt est requis."

        patterns = [p.strip() for p in patterns_str.split(",") if p.strip()] or ["*"]
        trigger_id = str(uuid.uuid4())[:8]
        prompts = []
        for line in prompt_text.strip().splitlines():
            line = line.strip()
            if line:
                prompts.append(PromptChainItem(prompt=line, agent=agent).to_dict())

        trigger = EventTrigger(
            trigger_id=trigger_id,
            name=name.strip() or f"watch_{trigger_id}",
            watch_path=watch_path.strip(),
            patterns=patterns,
            prompts=prompts,
            agent=agent,
            recursive=recursive,
        )
        mgr = EventWatcherManager()
        mgr.add_trigger(trigger)
        return (
            f"✅ Trigger **{trigger.name}** ajouté (ID: `{trigger_id}`).\n"
            f"> Dossier: `{watch_path}` | Patterns: `{', '.join(patterns)}`"
        )
    except Exception as e:
        return f"❌ Erreur : {e}"


def action_watcher_remove(trigger_id: str) -> str:
    try:
        from core.event_watcher import EventWatcherManager
        mgr = EventWatcherManager()
        if mgr.remove_trigger(trigger_id.strip()):
            return f"✅ Trigger `{trigger_id}` supprimé."
        return f"❌ Trigger `{trigger_id}` introuvable."
    except Exception as e:
        return f"❌ Erreur : {e}"


# ---------------------------------------------------------------------------
# Feature 5 & 6: Working Directories and RAG JSON config actions
# ---------------------------------------------------------------------------

def action_save_working_dirs(dirs_json: str) -> str:
    try:
        dirs = json.loads(dirs_json)
        if not isinstance(dirs, list):
            return "❌ Format invalide. Attendu: une liste JSON d'objets {path, mode, label, description}."
        # Validate entries
        for d in dirs:
            if "path" not in d:
                return f"❌ Champ 'path' manquant dans: {d}"
            if d.get("mode", "read") not in ("read", "write", "readwrite"):
                return f"❌ Mode invalide '{d.get('mode')}'. Valeurs acceptées: read, write, readwrite."
        save_config({"working_directories": dirs})
        return f"✅ {len(dirs)} répertoire(s) de travail sauvegardé(s)."
    except json.JSONDecodeError as e:
        return f"❌ JSON invalide : {e}"
    except Exception as e:
        return f"❌ Erreur : {e}"


def action_list_working_dirs() -> str:
    try:
        from core.working_dirs import WorkingDirManager
        config = load_config()
        wdm = WorkingDirManager(config)
        dirs = wdm.list_directories()
        if not dirs:
            return "*Aucun répertoire de travail configuré.*"
        lines = []
        for d in dirs:
            exists = "✅" if d["exists"] else "⚠️ MANQUANT"
            perms = []
            if d["can_read"]:
                perms.append("Lecture")
            if d["can_write"]:
                perms.append("Écriture")
            lines.append(
                f"{exists} **{d['label']}** (`{d['mode']}`)\n"
                f"> Chemin: `{d['path']}`\n"
                f"> Permissions: {' + '.join(perms)}"
                + (f"\n> Description: {d['description']}" if d.get("description") else "")
            )
        return "\n\n---\n".join(lines)
    except Exception as e:
        return f"❌ Erreur : {e}"


def action_save_rag_config(json_path: str, list_key: str, max_steps: int) -> str:
    try:
        rag_cfg: Dict[str, Any] = {
            "json_path": json_path.strip(),
            "max_steps": int(max_steps),
        }
        if list_key.strip():
            rag_cfg["list_key"] = list_key.strip()
        else:
            rag_cfg["list_key"] = None
        save_config({"rag_json": rag_cfg})
        return f"✅ Configuration RAG JSON sauvegardée.\n> Fichier: `{json_path}`"
    except Exception as e:
        return f"❌ Erreur : {e}"


def action_rag_test(json_path: str, list_key: str) -> str:
    try:
        from core.rag_tools import load_json_as_records, TFIDFIndex
        import os
        path = json_path.strip()
        if not os.path.exists(path):
            return f"❌ Fichier introuvable : `{path}`"
        records = load_json_as_records(path, list_key.strip() or None)
        idx = TFIDFIndex()
        n = idx.build(records)
        fields: set = set()
        for r in records[:10]:
            if isinstance(r, dict):
                fields.update(r.keys())
        return (
            f"✅ Fichier JSON chargé avec succès.\n"
            f"> {n} enregistrement(s) indexé(s)\n"
            f"> Champs détectés: `{', '.join(sorted(fields))}`"
        )
    except Exception as e:
        return f"❌ Erreur lors du chargement : {e}"


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
            "Configurez les connexions, paramétrez les agents et lancez vos instructions via le chat.\n\n"
            "**Nouvelles fonctionnalités** : "
            "📚 Bibliothèque de prompts | ⛓️ Chaining | 🕐 Planification | 👁️ Surveillance de dossiers | 📁 Répertoires & RAG"
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
                    label="Autoriser les requêtes en écriture SQL (INSERT / UPDATE / CREATE…)",
                )
                sec_allow_delete = gr.Checkbox(
                    value=sec.get("allow_delete", False),
                    label="🗂️ Autoriser la suppression de fichiers (FileSystemAgent — delete_path)",
                )

                with gr.Row():
                    sec_max_rows      = gr.Number(**_kw(gr.Number, value=sec.get("max_rows_returned", 1000), label="Lignes max retournées", precision=0))
                    sec_query_timeout = gr.Number(**_kw(gr.Number, value=sec.get("query_timeout", 30), label="Timeout requête (s)", precision=0))

                btn_save_global  = gr.Button(**_kw(gr.Button, value="💾 Sauvegarder paramètres globaux", variant="primary"))
                global_ag_status = gr.Markdown(**_kw(gr.Markdown, value="", elem_classes=["status-box"]))

                btn_save_global.click(
                    action_save_agents,
                    inputs=[ag_max_steps, ag_reflection, ag_parallel, ag_result_dir,
                            sec_allow_write, sec_max_rows, sec_query_timeout, sec_allow_delete],
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
                    default_desc       = ov.get("description", info.get("detailed_description", info["description"]))

                    try:
                        with gr.Accordion(
                            label=f"{info['display']} — {info['description']}",
                            open=False,
                        ):
                            w_desc = gr.Textbox(
                                value=default_desc,
                                label="📝 Description (utilisée par le Manager pour choisir cet agent)",
                                lines=3,
                                info="Modifiez cette description pour affiner le choix automatique du Manager.",
                            )
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
                        w_desc = gr.Textbox(value=default_desc, label="Description", lines=2)
                        with gr.Row():
                            w_enabled = gr.Checkbox(value=default_enabled, label="Activé", scale=1)
                            w_ms = gr.Number(**_kw(gr.Number, value=default_ms, label="Étapes max", precision=0, scale=1))
                            w_ri = gr.Number(**_kw(gr.Number, value=default_ri, label="Intervalle réflexion", precision=0, scale=1))

                    per_agent_inputs.extend([w_enabled, w_ms, w_ri, w_desc])

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
            # TAB 4 — Prompt Library
            # ─────────────────────────────────────────────────────────
            with gr.TabItem("📚 Prompts"):
                gr.Markdown(
                    "### Bibliothèque de prompts\n"
                    "Sauvegardez, gérez et exécutez vos prompts réutilisables. "
                    "Supportez les variables `{nom}` dans les textes."
                )

                with gr.Row():
                    with gr.Column(scale=2):
                        gr.Markdown("#### Ajouter / Modifier un prompt")
                        pl_name = gr.Textbox(
                            **_kw(gr.Textbox, label="Nom (identifiant unique)", placeholder="audit_qualite")
                        )
                        pl_prompt = gr.Textbox(
                            **_kw(gr.Textbox, label="Texte du prompt (variables: {table}, {date}…)", lines=4,
                                  placeholder="Analyse la qualité des données de la table {table}")
                        )
                        pl_desc = gr.Textbox(
                            **_kw(gr.Textbox, label="Description courte (optionnel)", placeholder="Audit qualité d'une table")
                        )
                        with gr.Row():
                            pl_agent = gr.Dropdown(
                                **_kw(gr.Dropdown, choices=["manager"] + list(AGENT_INFO.keys()),
                                      value="manager", label="Agent par défaut", scale=2)
                            )
                            pl_tags = gr.Textbox(
                                **_kw(gr.Textbox, label="Tags (virgule)", placeholder="audit,qualite", scale=2)
                            )
                        btn_pl_save = gr.Button(**_kw(gr.Button, value="💾 Sauvegarder le prompt", variant="primary"))
                        pl_save_status = gr.Markdown(**_kw(gr.Markdown, value="", elem_classes=["status-box"]))
                        btn_pl_save.click(
                            action_prompt_save,
                            inputs=[pl_name, pl_prompt, pl_desc, pl_agent, pl_tags],
                            outputs=[pl_save_status],
                        )

                        gr.Markdown("---\n#### Exécuter un prompt sauvegardé")
                        pl_run_name = gr.Textbox(
                            **_kw(gr.Textbox, label="Nom du prompt à exécuter")
                        )
                        pl_run_vars = gr.Textbox(
                            **_kw(gr.Textbox, label="Variables (une par ligne: table=users)", lines=3,
                                  placeholder="table=users\ndate=2024-01-01")
                        )
                        pl_run_agent = gr.Textbox(
                            **_kw(gr.Textbox, label="Agent (laisser vide = agent par défaut du prompt)")
                        )
                        btn_pl_run = gr.Button(**_kw(gr.Button, value="▶ Charger dans le chat", variant="secondary"))
                        pl_run_status = gr.Markdown(**_kw(gr.Markdown, value="", elem_classes=["status-box"]))

                        gr.Markdown("---\n#### Supprimer un prompt")
                        pl_del_name = gr.Textbox(**_kw(gr.Textbox, label="Nom du prompt à supprimer"))
                        btn_pl_del = gr.Button(**_kw(gr.Button, value="🗑️ Supprimer", variant="stop"))
                        pl_del_status = gr.Markdown(**_kw(gr.Markdown, value="", elem_classes=["status-box"]))
                        btn_pl_del.click(action_prompt_delete, inputs=[pl_del_name], outputs=[pl_del_status])

                    with gr.Column(scale=2):
                        gr.Markdown("#### Prompts sauvegardés")
                        btn_pl_refresh = gr.Button(**_kw(gr.Button, value="🔄 Rafraîchir la liste"))
                        pl_list_display = gr.Markdown(value=action_prompt_list(), elem_classes=["status-box"])
                        btn_pl_refresh.click(action_prompt_list, outputs=[pl_list_display])

            # ─────────────────────────────────────────────────────────
            # TAB 5 — Prompt Chaining
            # ─────────────────────────────────────────────────────────
            with gr.TabItem("⛓️ Chaining"):
                gr.Markdown(
                    "### Enchaînement de prompts\n"
                    "Définissez plusieurs prompts à exécuter en séquence. "
                    "Activez *Passer le résultat* pour injecter la réponse de chaque étape "
                    "dans la suivante via `{previous_result}`."
                )

                chain_prompts = gr.Textbox(
                    **_kw(gr.Textbox,
                          label="Prompts (un par ligne — exécutés dans l'ordre)",
                          lines=8,
                          placeholder="Analyse la table users\nDétecte les anomalies dans les résultats précédents: {previous_result}\nGénère un rapport de synthèse")
                )
                with gr.Row():
                    chain_agent = gr.Dropdown(
                        **_kw(gr.Dropdown, choices=["manager"] + list(AGENT_INFO.keys()),
                              value="manager", label="Agent pour toute la chaîne", scale=2)
                    )
                    chain_pass = gr.Checkbox(
                        **_kw(gr.Checkbox, value=False,
                              label="Passer le résultat au prompt suivant via {previous_result}", scale=2)
                    )
                btn_chain_run = gr.Button(**_kw(gr.Button, value="▶ Exécuter la chaîne", variant="primary"))
                chain_status = gr.Markdown(**_kw(gr.Markdown, value="", elem_classes=["status-box"]))

            # ─────────────────────────────────────────────────────────
            # TAB 6 — Scheduler
            # ─────────────────────────────────────────────────────────
            with gr.TabItem("🕐 Planification"):
                gr.Markdown(
                    "### Planification de prompts\n"
                    "Programmez l'exécution automatique de prompts selon une expression cron, "
                    "un intervalle ou une date unique.\n\n"
                    "**Note** : Pour que les jobs s'exécutent, lancez le daemon avec `python main.py --schedule-daemon`."
                )

                gr.Markdown("#### Jobs planifiés")
                btn_sched_refresh = gr.Button(**_kw(gr.Button, value="🔄 Rafraîchir"))
                sched_list_display = gr.Markdown(value=action_scheduler_list(), elem_classes=["status-box"])
                btn_sched_refresh.click(action_scheduler_list, outputs=[sched_list_display])

                gr.Markdown("---\n#### Ajouter un job")
                with gr.Row():
                    sched_name = gr.Textbox(**_kw(gr.Textbox, label="Nom du job", placeholder="rapport_quotidien", scale=2))
                    sched_type = gr.Dropdown(
                        **_kw(gr.Dropdown, choices=["cron", "interval", "once"],
                              value="cron", label="Type", scale=1)
                    )
                    sched_value = gr.Textbox(
                        **_kw(gr.Textbox, label="Valeur (cron: '0 9 * * 1-5' | interval: '3600' s | once: 'YYYY-MM-DD HH:MM:SS')",
                              placeholder="0 9 * * 1-5", scale=3)
                    )
                sched_agent = gr.Dropdown(
                    **_kw(gr.Dropdown, choices=["manager"] + list(AGENT_INFO.keys()),
                          value="manager", label="Agent")
                )
                sched_prompt = gr.Textbox(
                    **_kw(gr.Textbox, label="Prompt(s) à exécuter (un par ligne)", lines=4,
                          placeholder="Génère le rapport qualité quotidien\nEnvoie une alerte si des anomalies sont détectées")
                )
                btn_sched_add = gr.Button(**_kw(gr.Button, value="➕ Ajouter le job", variant="primary"))
                sched_add_status = gr.Markdown(**_kw(gr.Markdown, value="", elem_classes=["status-box"]))
                btn_sched_add.click(
                    action_scheduler_add,
                    inputs=[sched_name, sched_type, sched_value, sched_prompt, sched_agent],
                    outputs=[sched_add_status],
                )

                gr.Markdown("---\n#### Supprimer un job")
                with gr.Row():
                    sched_del_id = gr.Textbox(**_kw(gr.Textbox, label="ID du job", scale=3))
                    btn_sched_del = gr.Button(**_kw(gr.Button, value="🗑️ Supprimer", variant="stop", scale=1))
                sched_del_status = gr.Markdown(**_kw(gr.Markdown, value="", elem_classes=["status-box"]))
                btn_sched_del.click(action_scheduler_remove, inputs=[sched_del_id], outputs=[sched_del_status])

            # ─────────────────────────────────────────────────────────
            # TAB 7 — Event Watcher
            # ─────────────────────────────────────────────────────────
            with gr.TabItem("👁️ Surveillance"):
                gr.Markdown(
                    "### Surveillance de dossiers\n"
                    "Déclenchez automatiquement des prompts quand de nouveaux fichiers "
                    "arrivent dans un dossier surveillé.\n\n"
                    "Les variables `{filepath}`, `{filename}` et `{directory}` sont "
                    "automatiquement injectées dans vos prompts.\n\n"
                    "**Note** : Pour démarrer la surveillance, lancez `python main.py --watch-start`."
                )

                gr.Markdown("#### Triggers actifs")
                btn_watch_refresh = gr.Button(**_kw(gr.Button, value="🔄 Rafraîchir"))
                watch_list_display = gr.Markdown(value=action_watcher_list(), elem_classes=["status-box"])
                btn_watch_refresh.click(action_watcher_list, outputs=[watch_list_display])

                gr.Markdown("---\n#### Ajouter un trigger")
                with gr.Row():
                    watch_name = gr.Textbox(**_kw(gr.Textbox, label="Nom du trigger", placeholder="nouveau_csv", scale=2))
                    watch_path = gr.Textbox(**_kw(gr.Textbox, label="Dossier à surveiller", placeholder="/data/inbox", scale=3))
                with gr.Row():
                    watch_patterns = gr.Textbox(
                        **_kw(gr.Textbox, label="Patterns de fichiers (virgule)", placeholder="*.csv, *.json", scale=3)
                    )
                    watch_recursive = gr.Checkbox(**_kw(gr.Checkbox, value=False, label="Récursif", scale=1))
                watch_agent = gr.Dropdown(
                    **_kw(gr.Dropdown, choices=["manager"] + list(AGENT_INFO.keys()),
                          value="manager", label="Agent")
                )
                watch_prompt = gr.Textbox(
                    **_kw(gr.Textbox,
                          label="Prompt déclenché sur nouveau fichier (variables: {filepath}, {filename})",
                          lines=4,
                          placeholder="Analyse le fichier {filename} qui vient d'arriver dans {directory}")
                )
                btn_watch_add = gr.Button(**_kw(gr.Button, value="➕ Ajouter le trigger", variant="primary"))
                watch_add_status = gr.Markdown(**_kw(gr.Markdown, value="", elem_classes=["status-box"]))
                btn_watch_add.click(
                    action_watcher_add,
                    inputs=[watch_name, watch_path, watch_patterns, watch_agent, watch_prompt, watch_recursive],
                    outputs=[watch_add_status],
                )

                gr.Markdown("---\n#### Supprimer un trigger")
                with gr.Row():
                    watch_del_id = gr.Textbox(**_kw(gr.Textbox, label="ID du trigger", scale=3))
                    btn_watch_del = gr.Button(**_kw(gr.Button, value="🗑️ Supprimer", variant="stop", scale=1))
                watch_del_status = gr.Markdown(**_kw(gr.Markdown, value="", elem_classes=["status-box"]))
                btn_watch_del.click(action_watcher_remove, inputs=[watch_del_id], outputs=[watch_del_status])

            # ─────────────────────────────────────────────────────────
            # TAB 8 — Working Directories & RAG JSON
            # ─────────────────────────────────────────────────────────
            with gr.TabItem("📁 Répertoires & RAG"):
                gr.Markdown("### Répertoires de travail")
                gr.Markdown(
                    "Définissez les répertoires locaux auxquels les agents ont accès. "
                    "Chaque entrée doit préciser le mode d'accès : `read`, `write` ou `readwrite`."
                )

                btn_wd_refresh = gr.Button(**_kw(gr.Button, value="🔄 Afficher les répertoires configurés"))
                wd_list_display = gr.Markdown(value=action_list_working_dirs(), elem_classes=["status-box"])
                btn_wd_refresh.click(action_list_working_dirs, outputs=[wd_list_display])

                wd_raw = load_config().get("working_directories", [])
                wd_json_str = json.dumps(
                    [d for d in wd_raw if not isinstance(d, str) or not d.startswith("_comment")],
                    indent=2, ensure_ascii=False
                )
                wd_json = gr.Textbox(
                    **_kw(gr.Textbox,
                          label='Répertoires (JSON — liste d\'objets {path, mode, label, description})',
                          value=wd_json_str,
                          lines=10,
                          placeholder='[\n  {"path": "/data", "mode": "read", "label": "data", "description": "Données sources"},\n  {"path": "./results", "mode": "readwrite", "label": "results"}\n]')
                )
                btn_wd_save = gr.Button(**_kw(gr.Button, value="💾 Sauvegarder les répertoires", variant="primary"))
                wd_save_status = gr.Markdown(**_kw(gr.Markdown, value="", elem_classes=["status-box"]))
                btn_wd_save.click(
                    action_save_working_dirs,
                    inputs=[wd_json],
                    outputs=[wd_save_status],
                )

                gr.Markdown("---\n### Configuration de l'agent RAG JSON")
                gr.Markdown(
                    "L'agent **RAG JSON** effectue des recherches par similarité TF-IDF "
                    "dans un fichier JSON local. Configurez ici le chemin vers votre base de connaissances."
                )

                rag_cfg = load_config().get("rag_json", {})
                with gr.Row():
                    rag_path = gr.Textbox(
                        **_kw(gr.Textbox,
                              value=rag_cfg.get("json_path", ""),
                              label="Chemin vers le fichier JSON",
                              placeholder="/home/user/knowledge_base.json",
                              scale=4)
                    )
                    rag_list_key = gr.Textbox(
                        **_kw(gr.Textbox,
                              value=rag_cfg.get("list_key") or "",
                              label="Clé de liste (optionnel, ex: 'items')",
                              placeholder="items",
                              scale=2)
                    )
                rag_max_steps = gr.Number(
                    **_kw(gr.Number, value=rag_cfg.get("max_steps", 15), label="Étapes max", precision=0)
                )
                with gr.Row():
                    btn_rag_test = gr.Button(**_kw(gr.Button, value="🔍 Tester le chargement JSON", scale=2))
                    btn_rag_save = gr.Button(**_kw(gr.Button, value="💾 Sauvegarder la config RAG", variant="primary", scale=2))
                rag_status = gr.Markdown(**_kw(gr.Markdown, value="", elem_classes=["status-box"]))
                btn_rag_test.click(action_rag_test, inputs=[rag_path, rag_list_key], outputs=[rag_status])
                btn_rag_save.click(
                    action_save_rag_config,
                    inputs=[rag_path, rag_list_key, rag_max_steps],
                    outputs=[rag_status],
                )

            # ─────────────────────────────────────────────────────────
            # TAB 9 — Chat
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
                        type="messages",
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

        # ── Wire chaining tab → chatbot ───────────────────────────────
        btn_chain_run.click(
            action_chain_run,
            inputs=[chain_prompts, chain_agent, chain_pass, chatbot],
            outputs=[chatbot, chain_status, msg_input],
        )

        # ── Wire prompt library run → chat input ──────────────────────
        btn_pl_run.click(
            action_prompt_run,
            inputs=[pl_run_name, pl_run_agent, pl_run_vars, chatbot],
            outputs=[chatbot, msg_input],
        )

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
