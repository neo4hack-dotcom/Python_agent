#!/usr/bin/env python3
"""
AI Manager Agent — Point d'entrée principal
============================================
Usage:
  python main.py "Analyse la qualité des données dans ma base ClickHouse"
  python main.py --task "Trouve les patterns dans la table events" --config custom_config.json
  python main.py --check-connections
  python main.py --list-tools
  python main.py --interactive

Compatibilité : Python 3.8+ | Windows | Linux | macOS
Dépendances   : colorama (optionnel), python-oracledb (si Oracle)
"""
import sys
import os
import json
import argparse
import time
from typing import Dict, Any, Optional


# --------------------------------------------------------------------------- #
#  Ensure Python 3.8+                                                          #
# --------------------------------------------------------------------------- #
if sys.version_info < (3, 8):
    print("ERREUR: Python 3.8 minimum requis. Version actuelle:", sys.version)
    sys.exit(1)


# --------------------------------------------------------------------------- #
#  Config loader                                                               #
# --------------------------------------------------------------------------- #

DEFAULT_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")


def load_config(path: str = DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    """Load and validate config.json."""
    if not os.path.exists(path):
        print(f"[ERROR] Config file not found: {path}")
        print(f"        Copy config.json.example → config.json and edit it.")
        sys.exit(1)

    with open(path, encoding="utf-8") as f:
        raw = f.read()

    # Strip // comments (JSON doesn't support them natively).
    # Only strip // that appear after whitespace (not inside strings like http://)
    import re
    raw = re.sub(r'(?m)(?<=\s)//[^\n]*', '', raw)
    # Strip _comment keys
    config = json.loads(raw)

    def strip_comments(obj):
        if isinstance(obj, dict):
            return {k: strip_comments(v) for k, v in obj.items() if not k.startswith("_comment")}
        if isinstance(obj, list):
            return [strip_comments(i) for i in obj]
        return obj

    return strip_comments(config)


# --------------------------------------------------------------------------- #
#  CLI                                                                         #
# --------------------------------------------------------------------------- #

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python main.py",
        description="AI Manager Agent — Analyse autonome de données ClickHouse/Oracle",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemples:
  python main.py "Analyse la qualité des données"
  python main.py --task "Trouve les anomalies dans events" --db clickhouse
  python main.py --task "Audit complet" --agent quality
  python main.py --check-connections
  python main.py --interactive
  python main.py --list-tools
        """,
    )
    parser.add_argument(
        "task",
        nargs="?",
        help="Tâche à exécuter (peut aussi être passée avec --task)",
    )
    parser.add_argument(
        "--task", "-t",
        dest="task_flag",
        help="Tâche à exécuter",
    )
    parser.add_argument(
        "--config", "-c",
        default=DEFAULT_CONFIG_PATH,
        help=f"Chemin vers config.json (défaut: {DEFAULT_CONFIG_PATH})",
    )
    parser.add_argument(
        "--db",
        choices=["clickhouse", "oracle", "auto"],
        default="auto",
        help="Base de données cible (défaut: auto)",
    )
    parser.add_argument(
        "--agent", "-a",
        choices=[
            "manager", "analyst", "quality", "pattern", "query",
            # ClickHouse specialist agents
            "sql_analyst", "clickhouse_generic", "clickhouse_table_manager",
            "clickhouse_writer", "clickhouse_specific", "text_to_sql_translator",
            # File agents
            "excel", "textfile", "filesystem",
        ],
        default="manager",
        help=(
            "Agent à utiliser (défaut: manager). "
            "Agents ClickHouse: sql_analyst, clickhouse_generic, "
            "clickhouse_table_manager, clickhouse_writer, "
            "clickhouse_specific, text_to_sql_translator. "
            "Agents fichiers: excel, textfile, filesystem"
        ),
    )
    parser.add_argument(
        "--graph",
        action="store_true",
        help="Utiliser LangGraph pour l'orchestration multi-agents (ignores --agent)",
    )
    parser.add_argument(
        "--max-steps",
        type=int,
        default=None,
        help="Override du nombre max d'étapes",
    )
    parser.add_argument(
        "--check-connections",
        action="store_true",
        help="Tester les connexions aux bases de données et au LLM",
    )
    parser.add_argument(
        "--list-tools",
        action="store_true",
        help="Afficher tous les outils disponibles",
    )
    parser.add_argument(
        "--interactive", "-i",
        action="store_true",
        help="Mode interactif (saisie de tâches en boucle)",
    )
    parser.add_argument(
        "--allow-write",
        action="store_true",
        help="Autoriser les requêtes en écriture (INSERT/UPDATE/CREATE…)",
    )
    parser.add_argument(
        "--output", "-o",
        help="Fichier de sortie pour le résultat (JSON)",
    )
    return parser


# --------------------------------------------------------------------------- #
#  Modes                                                                       #
# --------------------------------------------------------------------------- #

def mode_check_connections(config: dict):
    """Test DB and LLM connectivity."""
    from core.db_manager import DBManager
    from core.llm_client import LLMClient

    print("\n=== CONNECTION CHECK ===\n")

    # LLM
    print(f"LLM  [{config['llm']['api_type']}] {config['llm']['base_url']} model={config['llm']['model']}")
    try:
        llm = LLMClient(config["llm"])
        if llm.ping():
            print("  → OK")
        else:
            print("  → FAILED (no response)")
    except Exception as e:
        print(f"  → ERROR: {e}")

    print()

    # Databases
    db = DBManager(config["databases"])
    status = db.status()
    if not status:
        print("No databases enabled in config.")
    for name, ok in status.items():
        cfg = config["databases"].get(name, {})
        print(f"DB   [{name}] {cfg.get('host','?')}:{cfg.get('port','?')}")
        print(f"  → {'OK' if ok else 'FAILED'}")

    print()


def mode_list_tools():
    """Print all available tools (base + ClickHouse-specific)."""
    from core.tools import TOOL_DEFINITIONS
    from core.clickhouse_tools import CH_TOOL_DEFINITIONS
    # CH_TOOL_DEFINITIONS includes base tools + CH-only ones; deduplicate by name
    base_names = {t["name"] for t in TOOL_DEFINITIONS}
    ch_only    = [t for t in CH_TOOL_DEFINITIONS if t["name"] not in base_names]

    def _print_tools(tool_list):
        for t in tool_list:
            print(f"  {t['name']}")
            print(f"    {t['description']}")
            if t.get("params"):
                for p, desc in t["params"].items():
                    req = "(required)" if p in t.get("required", []) else "(optional)"
                    print(f"      {p}: {desc} {req}")
            print()

    print("\n=== AVAILABLE TOOLS (Base) ===\n")
    _print_tools(TOOL_DEFINITIONS)

    print("\n=== AVAILABLE TOOLS (ClickHouse-Specific) ===\n")
    _print_tools(ch_only)


def mode_interactive(config: dict, default_agent: str = "manager", use_graph: bool = False):
    """REPL loop for interactive mode."""
    mode = "LangGraph" if use_graph else default_agent
    print("\n=== MODE INTERACTIF ===")
    print(f"Mode : {mode}")
    print("Tapez votre tâche et appuyez sur Entrée. 'quit' pour quitter.\n")
    while True:
        try:
            task = input("Tâche > ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nAu revoir.")
            break
        if task.lower() in ("quit", "exit", "q"):
            print("Au revoir.")
            break
        if not task:
            continue
        run_task(config, task, agent=default_agent, use_graph=use_graph)


def run_task(
    config:        dict,
    task:          str,
    agent:         str = "manager",
    output:        Optional[str] = None,
    max_steps:     Optional[int] = None,
    allow_write:   bool = False,
    use_graph:     bool = False,
    step_callback  = None,   # callable(dict) for UI streaming
) -> Dict[str, Any]:
    """Execute a task with the chosen agent or LangGraph pipeline."""

    # Apply CLI overrides
    if max_steps is not None:
        config.setdefault("agents", {})["max_steps"] = max_steps
    if allow_write:
        config.setdefault("security", {})["allow_write_queries"] = True

    if use_graph:
        from core.graph      import run_graph
        from core.llm_client import LLMClient
        from core.db_manager import DBManager
        from utils.logger    import AgentLogger

        llm    = LLMClient(config["llm"])
        db     = DBManager(config["databases"])
        logger = AgentLogger(
            name="LangGraph",
            log_file=config.get("logging", {}).get("file"),
            level=config.get("logging", {}).get("level", "INFO"),
            colors=config.get("logging", {}).get("colors", True),
        )
        result = run_graph(task=task, llm_client=llm, db_manager=db,
                           config=config, logger=logger)

    elif agent == "manager":
        from agents.manager_agent import ManagerAgent
        runner = ManagerAgent(config, step_callback=step_callback)
        result = runner.run(task)
    else:
        # Run a specific sub-agent directly
        from core.llm_client import LLMClient
        from core.db_manager import DBManager
        from utils.logger    import AgentLogger
        from agents.analyst_agent    import AnalystAgent
        from agents.quality_agent    import QualityAgent
        from agents.pattern_agent    import PatternAgent
        from agents.query_agent      import QueryAgent
        from agents.base_agent       import CustomAgent
        from agents.clickhouse       import AGENT_REGISTRY as CH_REGISTRY
        from agents.excel_agent      import ExcelAgent
        from agents.text_agent       import TextFileAgent
        from agents.filesystem_agent import FileSystemAgent

        agent_map = {
            "analyst": AnalystAgent,
            "quality": QualityAgent,
            "pattern": PatternAgent,
            "query":   QueryAgent,
            **CH_REGISTRY,
        }

        # File agents have custom constructors (no allow_write/max_rows)
        file_agent_map = {
            "excel":      ExcelAgent,
            "textfile":   TextFileAgent,
            "filesystem": FileSystemAgent,
        }

        llm    = LLMClient(config["llm"])
        db     = DBManager(config["databases"])
        logger = AgentLogger(
            name=agent,
            log_file=config.get("logging", {}).get("file"),
            colors=config.get("logging", {}).get("colors", True),
        )

        # Check if it's a custom agent defined in config
        custom_agents = config.get("custom_agents", [])
        custom_def = next(
            (ca for ca in custom_agents if ca.get("name", "").lower() == agent.lower()),
            None
        )

        if custom_def:
            template_key = custom_def.get("template", "analyst").lower()
            template_cls = agent_map.get(template_key, AnalystAgent)
            instance = CustomAgent(
                llm=llm, db=db, logger=logger,
                name=custom_def.get("display_name", agent),
                specialization=custom_def.get("specialization", template_cls.specialization),
                mission=custom_def.get("mission", template_cls.mission),
                max_steps=int(custom_def.get(
                    "max_steps",
                    config.get("agents", {}).get("max_steps", 20)
                )),
                allow_write=allow_write,
                max_rows=config.get("security", {}).get("max_rows_returned", 1000),
                step_callback=step_callback,
            )
        elif agent in CH_REGISTRY:
            instance = CH_REGISTRY[agent].from_config(
                llm=llm, db=db, logger=logger, config=config,
                step_callback=step_callback,
            )
            if allow_write:
                instance.tool_executor.allow_write = True
        elif agent in file_agent_map:
            # File agents: no DB queries, custom parameters
            FileAgentClass = file_agent_map[agent]
            override = config.get("agent_overrides", {}).get(agent, {})
            default_steps = {"excel": 25, "textfile": 25, "filesystem": 30}
            ms = int(override.get(
                "max_steps",
                config.get("agents", {}).get("max_steps", default_steps.get(agent, 25))
            ))
            init_kwargs = dict(
                llm=llm, db=db, logger=logger,
                max_steps=ms,
                step_callback=step_callback,
            )
            if agent == "filesystem":
                # allow_delete driven by security config (separate from allow_write)
                init_kwargs["allow_delete"] = bool(
                    config.get("security", {}).get("allow_delete", False)
                )
            instance = FileAgentClass(**init_kwargs)
        else:
            AgentClass = agent_map.get(agent)
            if AgentClass is None:
                print(f"Agent inconnu: {agent}")
                sys.exit(1)
            # Apply per-agent override if present
            override = config.get("agent_overrides", {}).get(agent, {})
            instance = AgentClass(
                llm=llm, db=db, logger=logger,
                max_steps=int(override.get(
                    "max_steps",
                    config.get("agents", {}).get("max_steps", 20)
                )),
                allow_write=allow_write,
                max_rows=config.get("security", {}).get("max_rows_returned", 1000),
                step_callback=step_callback,
            )
        result = instance.run(task)

    # Save to file if requested
    if output:
        try:
            with open(output, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2, ensure_ascii=False, default=str)
            print(f"\nRésultat sauvegardé → {output}")
        except Exception as e:
            print(f"Erreur lors de la sauvegarde: {e}")

    return result


# --------------------------------------------------------------------------- #
#  Entry point                                                                 #
# --------------------------------------------------------------------------- #

def main():
    parser = build_parser()
    args   = parser.parse_args()
    config = load_config(args.config)

    # Override allow_write from CLI
    if args.allow_write:
        config.setdefault("security", {})["allow_write_queries"] = True

    # --- Special modes ---
    if args.check_connections:
        mode_check_connections(config)
        return

    if args.list_tools:
        mode_list_tools()
        return

    if args.interactive:
        mode_interactive(config, default_agent=args.agent, use_graph=args.graph)
        return

    # --- Task mode ---
    task = args.task or args.task_flag
    if not task:
        print("ERREUR: Spécifiez une tâche.")
        print('  Exemple: python main.py "Analyse la qualité des données dans ClickHouse"')
        print('  Avec LangGraph : python main.py --graph "Analyse la qualité des données"')
        print("  Ou utilisez --interactive pour le mode interactif.")
        sys.exit(1)

    run_task(
        config=config,
        task=task,
        agent=args.agent,
        output=args.output,
        max_steps=args.max_steps,
        allow_write=args.allow_write,
        use_graph=args.graph,
    )


if __name__ == "__main__":
    main()
