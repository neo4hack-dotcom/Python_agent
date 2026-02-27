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

  # Gestion des prompts sauvegardés
  python main.py --save-prompt "nom" "texte du prompt" [--prompt-agent manager] [--prompt-tags tag1,tag2]
  python main.py --list-prompts
  python main.py --run-prompt "nom" [--prompt-vars key=val]
  python main.py --delete-prompt "nom"

  # Enchaînement de prompts (chaining)
  python main.py --chain "prompt1" "prompt2" "prompt3" [--agent manager]
  python main.py --chain-file ma_chaine.json

  # Planification (scheduler)
  python main.py --schedule-list
  python main.py --schedule-add --job-name "rapport" --cron "0 9 * * 1-5" "Génère le rapport quotidien"
  python main.py --schedule-daemon   # Lance le scheduler en mode daemon

  # Surveillance de dossiers (event watcher)
  python main.py --watch-list
  python main.py --watch-add --watch-path /dossier --watch-patterns "*.csv" "Analyse le fichier {filename}"
  python main.py --watch-start   # Lance la surveillance en mode daemon

  # Répertoires de travail
  python main.py --list-working-dirs

Compatibilité : Python 3.8+ | Windows | Linux | macOS
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

  # Prompts sauvegardés
  python main.py --save-prompt "audit_quality" "Fais un audit qualité complet" --prompt-agent quality
  python main.py --list-prompts
  python main.py --run-prompt "audit_quality"

  # Enchaînement de prompts
  python main.py --chain "Analyse la table users" "Détecte les anomalies" "Génère un rapport"

  # Planification
  python main.py --schedule-add --job-name "rapport_quotidien" --cron "0 9 * * 1-5" "Rapport du jour"
  python main.py --schedule-list
  python main.py --schedule-daemon

  # Surveillance de dossiers
  python main.py --watch-add --watch-path /data/inbox --watch-patterns "*.csv" "Analyse {filename}"
  python main.py --watch-start

  # Agent RAG JSON
  python main.py --agent rag_json "Recherche des articles sur le machine learning"
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
        default="manager",
        help=(
            "Agent à utiliser (défaut: manager). "
            "Agents base: analyst, quality, pattern, query. "
            "Agents ClickHouse: sql_analyst, clickhouse_generic, "
            "clickhouse_table_manager, clickhouse_writer, "
            "clickhouse_specific, text_to_sql_translator. "
            "Agents fichiers: excel, textfile, filesystem. "
            "Agent RAG: rag_json."
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

    # ------------------------------------------------------------------ #
    #  Feature 1: Prompt Library                                           #
    # ------------------------------------------------------------------ #
    prompt_group = parser.add_argument_group("Bibliothèque de prompts")
    prompt_group.add_argument(
        "--save-prompt",
        metavar=("NOM", "TEXTE"),
        nargs=2,
        help="Sauvegarder un prompt: --save-prompt NOM 'texte du prompt'",
    )
    prompt_group.add_argument(
        "--prompt-agent",
        default="manager",
        help="Agent par défaut pour le prompt sauvegardé (défaut: manager)",
    )
    prompt_group.add_argument(
        "--prompt-tags",
        help="Tags pour le prompt sauvegardé (séparés par virgules)",
    )
    prompt_group.add_argument(
        "--prompt-desc",
        help="Description courte du prompt sauvegardé",
    )
    prompt_group.add_argument(
        "--list-prompts",
        action="store_true",
        help="Lister tous les prompts sauvegardés",
    )
    prompt_group.add_argument(
        "--run-prompt",
        metavar="NOM",
        help="Exécuter un prompt sauvegardé par son nom",
    )
    prompt_group.add_argument(
        "--prompt-vars",
        metavar="KEY=VAL",
        nargs="*",
        help="Variables pour le prompt: --prompt-vars table=users date=2024-01-01",
    )
    prompt_group.add_argument(
        "--delete-prompt",
        metavar="NOM",
        help="Supprimer un prompt sauvegardé",
    )

    # ------------------------------------------------------------------ #
    #  Feature 2: Prompt Chaining                                          #
    # ------------------------------------------------------------------ #
    chain_group = parser.add_argument_group("Enchaînement de prompts")
    chain_group.add_argument(
        "--chain",
        metavar="PROMPT",
        nargs="+",
        help="Enchaîner plusieurs prompts: --chain 'prompt1' 'prompt2' 'prompt3'",
    )
    chain_group.add_argument(
        "--chain-file",
        metavar="FICHIER",
        help="Charger une chaîne de prompts depuis un fichier JSON",
    )
    chain_group.add_argument(
        "--chain-pass-result",
        action="store_true",
        help="Passer le résultat de chaque prompt au suivant via {previous_result}",
    )

    # ------------------------------------------------------------------ #
    #  Feature 3: Scheduler                                                #
    # ------------------------------------------------------------------ #
    sched_group = parser.add_argument_group("Planification de prompts")
    sched_group.add_argument(
        "--schedule-list",
        action="store_true",
        help="Lister les jobs planifiés",
    )
    sched_group.add_argument(
        "--schedule-add",
        action="store_true",
        help="Ajouter un job planifié (utiliser avec --job-name, --cron ou --interval)",
    )
    sched_group.add_argument(
        "--job-name",
        help="Nom du job planifié",
    )
    sched_group.add_argument(
        "--cron",
        metavar="EXPR",
        help="Expression cron: '0 9 * * 1-5' (lun-ven à 9h)",
    )
    sched_group.add_argument(
        "--interval",
        metavar="SECONDES",
        type=int,
        help="Intervalle en secondes entre deux exécutions",
    )
    sched_group.add_argument(
        "--once-at",
        metavar="DATETIME",
        help="Exécution unique à une date/heure: '2026-01-01 10:00:00'",
    )
    sched_group.add_argument(
        "--max-runs",
        type=int,
        default=0,
        help="Nombre max d'exécutions (0=illimité, défaut: 0)",
    )
    sched_group.add_argument(
        "--schedule-remove",
        metavar="JOB_ID",
        help="Supprimer un job planifié par son ID",
    )
    sched_group.add_argument(
        "--schedule-daemon",
        action="store_true",
        help="Lancer le scheduler en mode daemon (boucle infinie)",
    )

    # ------------------------------------------------------------------ #
    #  Feature 4: Event Watcher                                            #
    # ------------------------------------------------------------------ #
    watch_group = parser.add_argument_group("Surveillance de dossiers")
    watch_group.add_argument(
        "--watch-list",
        action="store_true",
        help="Lister les triggers d'événements configurés",
    )
    watch_group.add_argument(
        "--watch-add",
        action="store_true",
        help="Ajouter un trigger de surveillance de dossier",
    )
    watch_group.add_argument(
        "--watch-path",
        metavar="CHEMIN",
        help="Chemin du dossier à surveiller",
    )
    watch_group.add_argument(
        "--watch-patterns",
        metavar="PATTERN",
        nargs="+",
        default=["*"],
        help="Patterns de fichiers à surveiller (ex: *.csv *.json)",
    )
    watch_group.add_argument(
        "--watch-recursive",
        action="store_true",
        help="Surveillance récursive des sous-dossiers",
    )
    watch_group.add_argument(
        "--watch-name",
        help="Nom du trigger de surveillance",
    )
    watch_group.add_argument(
        "--watch-remove",
        metavar="TRIGGER_ID",
        help="Supprimer un trigger par son ID",
    )
    watch_group.add_argument(
        "--watch-start",
        action="store_true",
        help="Démarrer la surveillance des dossiers en mode daemon",
    )

    # ------------------------------------------------------------------ #
    #  Feature 5: Working Directories                                      #
    # ------------------------------------------------------------------ #
    dir_group = parser.add_argument_group("Répertoires de travail")
    dir_group.add_argument(
        "--list-working-dirs",
        action="store_true",
        help="Afficher les répertoires de travail configurés",
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

    # Working directories
    from core.working_dirs import WorkingDirManager
    wdm = WorkingDirManager(config)
    print(wdm.summary())
    print()


def mode_list_tools():
    """Print all available tools (base + ClickHouse-specific + RAG)."""
    from core.tools import TOOL_DEFINITIONS
    from core.clickhouse_tools import CH_TOOL_DEFINITIONS
    from core.rag_tools import RAG_TOOL_DEFINITIONS

    base_names = {t["name"] for t in TOOL_DEFINITIONS}
    ch_only = [t for t in CH_TOOL_DEFINITIONS if t["name"] not in base_names]
    rag_only = [t for t in RAG_TOOL_DEFINITIONS if t["name"] not in base_names]

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

    print("\n=== AVAILABLE TOOLS (RAG JSON) ===\n")
    _print_tools(rag_only)


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


# --------------------------------------------------------------------------- #
#  Feature 1: Prompt Library modes                                             #
# --------------------------------------------------------------------------- #

def mode_save_prompt(args, config):
    from core.prompt_library import PromptLibrary
    lib = PromptLibrary()
    name, prompt = args.save_prompt
    tags = [t.strip() for t in args.prompt_tags.split(",")] if args.prompt_tags else []
    entry = lib.save(
        name=name,
        prompt=prompt,
        description=args.prompt_desc or "",
        agent=args.prompt_agent or "manager",
        tags=tags,
    )
    print(f"\n✓ Prompt '{name}' sauvegardé.")
    print(f"  Agent    : {entry['agent']}")
    print(f"  Tags     : {', '.join(entry['tags']) or '(aucun)'}")
    print(f"  Créé le  : {entry['created_at']}")


def mode_list_prompts(config):
    from core.prompt_library import PromptLibrary
    lib = PromptLibrary()
    prompts = lib.list_all()
    if not prompts:
        print("\n(Aucun prompt sauvegardé)")
        return
    print(f"\n=== BIBLIOTHÈQUE DE PROMPTS ({len(prompts)}) ===\n")
    for p in prompts:
        tags = ", ".join(p.get("tags", [])) or "-"
        runs = p.get("run_count", 0)
        print(f"  [{p['name']}]")
        print(f"    Agent   : {p['agent']}")
        print(f"    Tags    : {tags}")
        print(f"    Exécut. : {runs}x")
        if p.get("description"):
            print(f"    Desc.   : {p['description']}")
        print(f"    Prompt  : {p['prompt'][:80]}{'...' if len(p['prompt']) > 80 else ''}")
        print()


def mode_run_prompt(args, config):
    from core.prompt_library import PromptLibrary
    lib = PromptLibrary()
    name = args.run_prompt
    entry = lib.get(name)
    if not entry:
        print(f"Prompt '{name}' introuvable. Utilisez --list-prompts pour voir la liste.")
        sys.exit(1)

    # Parse variables
    variables = {}
    if args.prompt_vars:
        for kv in args.prompt_vars:
            if "=" in kv:
                k, v = kv.split("=", 1)
                variables[k.strip()] = v.strip()

    rendered = lib.render(name, variables)
    agent = args.agent if args.agent != "manager" else entry["agent"]

    print(f"\nExécution du prompt '{name}' avec l'agent '{agent}'...")
    print(f"Prompt : {rendered[:100]}{'...' if len(rendered) > 100 else ''}\n")

    lib.increment_run_count(name)
    result = run_task(
        config=config,
        task=rendered,
        agent=agent,
        output=args.output,
        max_steps=args.max_steps,
        allow_write=args.allow_write,
    )
    return result


def mode_delete_prompt(args, config):
    from core.prompt_library import PromptLibrary
    lib = PromptLibrary()
    name = args.delete_prompt
    if lib.delete(name):
        print(f"✓ Prompt '{name}' supprimé.")
    else:
        print(f"Prompt '{name}' introuvable.")


# --------------------------------------------------------------------------- #
#  Feature 2: Prompt Chaining modes                                            #
# --------------------------------------------------------------------------- #

def mode_chain(args, config):
    from core.prompt_queue import PromptQueue, PromptChainItem

    pq = PromptQueue()

    if args.chain_file:
        # Load chain from JSON file
        with open(args.chain_file, encoding="utf-8") as f:
            items = json.load(f)
        for item in items:
            pq.add(item, agent=args.agent)
        print(f"\nCharge {pq.size()} prompt(s) depuis '{args.chain_file}'")
    else:
        # Build chain from CLI args
        for i, prompt in enumerate(args.chain):
            item = PromptChainItem(
                prompt=prompt,
                agent=args.agent,
                pass_result=args.chain_pass_result and i > 0,
                label=f"Étape {i+1}",
            )
            pq.add(item)
        print(f"\nEnchaînement de {pq.size()} prompt(s)...")

    def on_start(idx, item, processed):
        print(f"\n[{idx+1}] Exécution : {item.label}")

    def on_done(idx, item, entry, duration):
        status = "OK" if entry["status"] == "ok" else "ERREUR"
        print(f"    → {status} en {duration:.1f}s")

    pq.on_item_start(on_start).on_item_done(on_done)

    results = pq.run(
        run_task,
        config,
        output=None,
        max_steps=args.max_steps,
        allow_write=args.allow_write,
    )

    print(f"\n=== RÉSULTATS DE LA CHAÎNE ({len(results)} étapes) ===")
    for entry in results:
        print(f"\n[Étape {entry['index']+1}] {entry['label']} — {entry['status'].upper()}")
        if isinstance(entry["result"], dict):
            answer = entry["result"].get("answer") or entry["result"].get("summary", "")
            if answer:
                print(f"  {answer[:200]}{'...' if len(str(answer)) > 200 else ''}")

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False, default=str)
        print(f"\nRésultats sauvegardés → {args.output}")

    return results


# --------------------------------------------------------------------------- #
#  Feature 3: Scheduler modes                                                  #
# --------------------------------------------------------------------------- #

def mode_schedule_list(config):
    from core.scheduler import PromptScheduler
    sched = PromptScheduler()
    jobs = sched.list_jobs()
    if not jobs:
        print("\n(Aucun job planifié)")
        return
    print(f"\n=== JOBS PLANIFIÉS ({len(jobs)}) ===\n")
    for j in jobs:
        status = "ACTIF" if j["enabled"] else "DÉSACTIVÉ"
        print(f"  [{j['job_id']}] {j['name']} — {status}")
        print(f"    Type       : {j['schedule_type']}")
        print(f"    Valeur     : {j['schedule_value']}")
        print(f"    Exécut.    : {j['run_count']}x")
        print(f"    Prochain   : {j['next_run'] or 'N/A'}")
        print(f"    Dernier    : {j['last_run'] or 'jamais'}")
        print(f"    Prompts    : {len(j['prompts'])} prompt(s)")
        print()


def mode_schedule_add(args, config):
    import uuid
    from core.scheduler import PromptScheduler, ScheduledJob
    from core.prompt_queue import PromptChainItem

    task = args.task or args.task_flag
    if not task:
        print("ERREUR: Spécifiez un prompt à planifier (argument positional ou --task).")
        sys.exit(1)

    # Determine schedule type and value
    if args.cron:
        stype, svalue = "cron", args.cron
    elif args.interval:
        stype, svalue = "interval", str(args.interval)
    elif args.once_at:
        stype, svalue = "once", args.once_at
    else:
        print("ERREUR: Spécifiez --cron, --interval ou --once-at.")
        sys.exit(1)

    job_id = str(uuid.uuid4())[:8]
    job = ScheduledJob(
        job_id=job_id,
        name=args.job_name or f"job_{job_id}",
        prompts=[PromptChainItem(prompt=task, agent=args.agent).to_dict()],
        schedule_type=stype,
        schedule_value=svalue,
        agent=args.agent,
        max_runs=args.max_runs,
    )

    sched = PromptScheduler()
    sched.add_job(job)
    print(f"\n✓ Job planifié ajouté.")
    print(f"  ID         : {job_id}")
    print(f"  Nom        : {job.name}")
    print(f"  Type       : {stype}")
    print(f"  Valeur     : {svalue}")
    print(f"  Prochain   : {job.next_run}")
    print(f"  Prompt     : {task[:80]}{'...' if len(task) > 80 else ''}")


def mode_schedule_remove(args, config):
    from core.scheduler import PromptScheduler
    sched = PromptScheduler()
    if sched.remove_job(args.schedule_remove):
        print(f"✓ Job '{args.schedule_remove}' supprimé.")
    else:
        print(f"Job '{args.schedule_remove}' introuvable.")


def mode_schedule_daemon(config):
    from core.scheduler import PromptScheduler
    sched = PromptScheduler()
    jobs = sched.list_jobs()
    active = [j for j in jobs if j["enabled"]]

    print(f"\n=== SCHEDULER DAEMON ===")
    print(f"Jobs actifs : {len(active)}/{len(jobs)}")
    for j in active:
        print(f"  • {j['name']} ({j['schedule_type']}: {j['schedule_value']}) → prochain: {j['next_run']}")
    print("\nCtrl+C pour arrêter.\n")

    sched.setup(run_task, config)

    def on_start(job):
        print(f"[{time.strftime('%H:%M:%S')}] Job '{job['name']}' démarré...")

    def on_done(job, results):
        ok = sum(1 for r in results if r.get("status") == "ok")
        print(f"[{time.strftime('%H:%M:%S')}] Job '{job['name']}' terminé ({ok}/{len(results)} OK)")

    sched.on_job_start(on_start).on_job_done(on_done)
    sched.start_daemon()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nArrêt du scheduler.")
        sched.stop_daemon()


# --------------------------------------------------------------------------- #
#  Feature 4: Event Watcher modes                                              #
# --------------------------------------------------------------------------- #

def mode_watch_list(config):
    from core.event_watcher import EventWatcherManager
    mgr = EventWatcherManager()
    triggers = mgr.list_triggers()
    if not triggers:
        print("\n(Aucun trigger de surveillance configuré)")
        return
    print(f"\n=== TRIGGERS DE SURVEILLANCE ({len(triggers)}) ===\n")
    for t in triggers:
        status = "ACTIF" if t["enabled"] else "DÉSACTIVÉ"
        print(f"  [{t['trigger_id']}] {t['name']} — {status}")
        print(f"    Dossier    : {t['watch_path']}")
        print(f"    Patterns   : {', '.join(t['patterns'])}")
        print(f"    Récursif   : {'Oui' if t['recursive'] else 'Non'}")
        print(f"    Événements : {', '.join(t['event_types'])}")
        print(f"    Déclench.  : {t['run_count']}x")
        print(f"    Prompts    : {len(t['prompts'])} prompt(s)")
        print()


def mode_watch_add(args, config):
    import uuid
    from core.event_watcher import EventWatcherManager, EventTrigger
    from core.prompt_queue import PromptChainItem

    if not args.watch_path:
        print("ERREUR: Spécifiez --watch-path.")
        sys.exit(1)

    task = args.task or args.task_flag
    if not task:
        print("ERREUR: Spécifiez un prompt à exécuter sur événement.")
        sys.exit(1)

    trigger_id = str(uuid.uuid4())[:8]
    trigger = EventTrigger(
        trigger_id=trigger_id,
        name=args.watch_name or f"watch_{trigger_id}",
        watch_path=args.watch_path,
        patterns=args.watch_patterns,
        prompts=[PromptChainItem(prompt=task, agent=args.agent).to_dict()],
        agent=args.agent,
        recursive=args.watch_recursive,
    )

    mgr = EventWatcherManager()
    mgr.add_trigger(trigger)
    print(f"\n✓ Trigger de surveillance ajouté.")
    print(f"  ID         : {trigger_id}")
    print(f"  Nom        : {trigger.name}")
    print(f"  Dossier    : {trigger.watch_path}")
    print(f"  Patterns   : {', '.join(trigger.patterns)}")
    print(f"  Prompt     : {task[:80]}{'...' if len(task) > 80 else ''}")


def mode_watch_remove(args, config):
    from core.event_watcher import EventWatcherManager
    mgr = EventWatcherManager()
    if mgr.remove_trigger(args.watch_remove):
        print(f"✓ Trigger '{args.watch_remove}' supprimé.")
    else:
        print(f"Trigger '{args.watch_remove}' introuvable.")


def mode_watch_daemon(config):
    from core.event_watcher import EventWatcherManager
    mgr = EventWatcherManager()
    mgr.setup(run_task, config)
    triggers = mgr.list_triggers()
    active = [t for t in triggers if t["enabled"]]

    print(f"\n=== SURVEILLANCE DAEMON ===")
    print(f"Triggers actifs : {len(active)}/{len(triggers)}")
    for t in active:
        print(f"  • {t['name']} → {t['watch_path']} ({', '.join(t['patterns'])})")
    print("\nCtrl+C pour arrêter.\n")

    def on_fire(trigger, event):
        print(f"[{time.strftime('%H:%M:%S')}] Événement détecté: {event['type']} → {event['filename']}")

    def on_done(trigger, event, results):
        ok = sum(1 for r in results if r.get("status") == "ok")
        print(f"[{time.strftime('%H:%M:%S')}] Trigger '{trigger['name']}' terminé ({ok}/{len(results)} OK)")

    mgr.on_trigger_fire(on_fire).on_trigger_done(on_done)
    mgr.start_all()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nArrêt de la surveillance.")
        mgr.stop_all()


# --------------------------------------------------------------------------- #
#  Feature 5: Working directories mode                                         #
# --------------------------------------------------------------------------- #

def mode_list_working_dirs(config):
    from core.working_dirs import WorkingDirManager
    wdm = WorkingDirManager(config)
    print()
    print(wdm.summary())
    dirs = wdm.list_directories()
    if dirs:
        print()
        for d in dirs:
            print(f"  [{d['label']}] {d['path']}")
            print(f"    Mode   : {d['mode']}")
            print(f"    Statut : {'OK' if d['exists'] else 'DOSSIER INTROUVABLE'}")
            if d.get("description"):
                print(f"    Desc.  : {d['description']}")
            print()


# --------------------------------------------------------------------------- #
#  Core task runner                                                            #
# --------------------------------------------------------------------------- #

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

    elif agent == "rag_json":
        from core.llm_client        import LLMClient
        from core.db_manager        import DBManager
        from utils.logger           import AgentLogger
        from agents.rag_json_agent  import RAGJsonAgent

        llm    = LLMClient(config["llm"])
        db     = DBManager(config["databases"])
        logger = AgentLogger(
            name="RAGJsonAgent",
            log_file=config.get("logging", {}).get("file"),
            colors=config.get("logging", {}).get("colors", True),
        )
        instance = RAGJsonAgent.from_config(
            llm=llm, db=db, logger=logger,
            config=config, step_callback=step_callback,
        )
        result = instance.run(task)

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
                init_kwargs["allow_delete"] = bool(
                    config.get("security", {}).get("allow_delete", False)
                )
            instance = FileAgentClass(**init_kwargs)
        else:
            AgentClass = agent_map.get(agent)
            if AgentClass is None:
                print(f"Agent inconnu: {agent}")
                sys.exit(1)
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

    # --- Special modes (no task required) ---

    if args.check_connections:
        mode_check_connections(config)
        return

    if args.list_tools:
        mode_list_tools()
        return

    if args.list_working_dirs:
        mode_list_working_dirs(config)
        return

    if args.list_prompts:
        mode_list_prompts(config)
        return

    if args.delete_prompt:
        mode_delete_prompt(args, config)
        return

    if args.save_prompt:
        mode_save_prompt(args, config)
        return

    if args.schedule_list:
        mode_schedule_list(config)
        return

    if args.schedule_remove:
        mode_schedule_remove(args, config)
        return

    if args.watch_list:
        mode_watch_list(config)
        return

    if args.watch_remove:
        mode_watch_remove(args, config)
        return

    # --- Daemon modes ---

    if args.schedule_daemon:
        mode_schedule_daemon(config)
        return

    if args.watch_start:
        mode_watch_daemon(config)
        return

    # --- Modes requiring a task ---

    if args.schedule_add:
        mode_schedule_add(args, config)
        return

    if args.watch_add:
        mode_watch_add(args, config)
        return

    if args.interactive:
        mode_interactive(config, default_agent=args.agent, use_graph=args.graph)
        return

    if args.run_prompt:
        mode_run_prompt(args, config)
        return

    if args.chain or args.chain_file:
        mode_chain(args, config)
        return

    # --- Standard task mode ---
    task = args.task or args.task_flag
    if not task:
        print("ERREUR: Spécifiez une tâche.")
        print('  Exemple: python main.py "Analyse la qualité des données dans ClickHouse"')
        print("  Ou utilisez --interactive pour le mode interactif.")
        print("  Ou --list-prompts pour voir les prompts sauvegardés.")
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
