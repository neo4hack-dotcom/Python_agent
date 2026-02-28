"""
Manager Agent — orchestre les sous-agents, agrège les résultats.

Fonctionnement :
  1. Pré-analyse la tâche → génère un plan d'exécution (stratégie + phases)
  2. Dispatche le(s) sous-agent(s) en parallèle ou en séquence selon le plan
  3. Auto-évalue les résultats et relance si la qualité est insuffisante
  4. Agrège les résultats et produit un rapport final

Stratégies d'orchestration :
  - single     : un seul agent pour les tâches simples
  - parallel   : plusieurs agents indépendants en simultané (ThreadPoolExecutor)
  - sequential : agents chainés, la sortie de l'un alimente le suivant

Auto-évaluation :
  - Après chaque dispatch, un appel LLM rapide note la qualité (0–1)
  - Si score < eval_threshold → relance automatique avec un hint d'amélioration
  - Nombre de retries configurable (eval_max_retries)

Agents disponibles :
  analyst, quality, pattern, query            → analyse de données DB
  excel                                        → fichiers Excel (.xlsx)
  text                                         → fichiers texte
  filesystem                                   → système de fichiers
  web                                          → navigation internet
  sql_analyst, clickhouse_generic,             → spécialistes ClickHouse
  clickhouse_table_manager, clickhouse_writer,
  clickhouse_specific, text_to_sql_translator
  rag_json                                     → recherche sémantique JSON
"""
import json
import time
import os
import threading
import concurrent.futures
from typing import Any, Callable, Dict, List, Optional, Tuple
from datetime import datetime

from core.llm_client  import LLMClient
from core.db_manager  import DBManager
from core.memory      import MemoryManager
from core.tools       import ToolExecutor
from core.engine      import AgentEngine
from agents.base_agent    import BaseAgent, CustomAgent
from agents.analyst_agent import AnalystAgent
from agents.quality_agent import QualityAgent
from agents.pattern_agent import PatternAgent
from agents.query_agent   import QueryAgent
from agents.excel_agent   import ExcelAgent
from agents.text_agent    import TextFileAgent
from agents.filesystem_agent import FileSystemAgent
from agents.web_agent     import WebAgent
from agents.clickhouse    import (
    SQLAnalystAgent,
    ClickHouseGenericAgent,
    ClickHouseTableManagerAgent,
    ClickHouseWriterAgent,
    ClickHouseSpecificAgent,
    TextToSQLAgent,
)
from utils.logger     import AgentLogger
from utils.prompts    import MANAGER_MISSION


# --------------------------------------------------------------------------- #
#  Registre global de tous les agents                                          #
# --------------------------------------------------------------------------- #

AGENT_REGISTRY: Dict[str, Any] = {
    # Agents d'analyse de données (BaseAgent)
    "analyst":   AnalystAgent,
    "quality":   QualityAgent,
    "pattern":   PatternAgent,
    "query":     QueryAgent,
    # Agents fichiers / web
    "excel":      ExcelAgent,
    "text":       TextFileAgent,
    "filesystem": FileSystemAgent,
    "web":        WebAgent,
    # Spécialistes ClickHouse
    "sql_analyst":              SQLAnalystAgent,
    "clickhouse_generic":       ClickHouseGenericAgent,
    "clickhouse_table_manager": ClickHouseTableManagerAgent,
    "clickhouse_writer":        ClickHouseWriterAgent,
    "clickhouse_specific":      ClickHouseSpecificAgent,
    "text_to_sql_translator":   TextToSQLAgent,
}

# Agents qui NE prennent PAS allow_write / max_rows dans leur constructeur
_SIMPLE_CONSTRUCTOR_AGENTS = {"excel", "text", "filesystem", "web"}

# Agents ClickHouse qui prennent des paramètres supplémentaires
_CH_SPECIALIST_AGENTS = {
    "sql_analyst", "clickhouse_generic", "clickhouse_table_manager",
    "clickhouse_writer", "clickhouse_specific", "text_to_sql_translator",
}

# --------------------------------------------------------------------------- #
#  Prompts internes                                                            #
# --------------------------------------------------------------------------- #

_ORCHESTRATION_PLAN_PROMPT = """Tu es un orchestrateur d'agents IA expert. Analyse la tâche suivante et génère un plan d'exécution optimal.

TÂCHE : {task}

AGENTS DISPONIBLES :
- analyst           : Analyse statistique, tendances, KPIs sur des données en base de données
- quality           : Audit qualité des données (nulls, doublons, outliers)
- pattern           : Découverte de patterns, corrélations, anomalies dans les données
- query             : Construction et optimisation de requêtes SQL complexes
- excel             : Création, lecture, modification de fichiers Excel (.xlsx)
- text              : Création, lecture, édition de fichiers texte (.txt .csv .log .json)
- filesystem        : Navigation de répertoires, recherche de fichiers, lecture de contenu
- web               : Recherche internet, navigation web, scraping, remplissage de formulaires
- sql_analyst       : Expert SQL ClickHouse avancé, optimisation de requêtes
- clickhouse_generic: Analyses ClickHouse complexes avec décomposition en DAG
- clickhouse_table_manager: Création/modification de schémas de tables ClickHouse (DDL)
- clickhouse_writer : Insertion de données dans ClickHouse (DML/INSERT)
- clickhouse_specific: Exécution de templates ClickHouse (DAU, funnel, rétention)
- text_to_sql_translator: Traduction langage naturel → SQL ClickHouse
- rag_json          : Recherche sémantique dans une base de connaissances JSON

STRATÉGIES :
- "single"     : un seul agent suffit
- "parallel"   : plusieurs sous-tâches INDÉPENDANTES → exécuter simultanément
- "sequential" : sous-tâches DÉPENDANTES → la sortie de l'une alimente la suivante
- "hybrid"     : combinaison de phases parallèles et séquentielles

EXEMPLES DE STRATÉGIE :
- "Analyse la qualité ET les patterns de la table orders" → parallel (quality + pattern)
- "Génère un rapport Excel avec les données ClickHouse" → sequential (analyst → excel)
- "Recherche info sur le web puis enregistre dans un fichier" → sequential (web → text)
- "Analyse qualité, patterns ET tendances" → parallel (quality + pattern + analyst)
- "Simple question sur les données" → single (analyst)

Réponds UNIQUEMENT avec un objet JSON valide (sans markdown) :
{{
  "strategy": "single",
  "primary_agent": "<agent principal si strategy=single>",
  "execution_plan": [
    {{
      "phase": 1,
      "mode": "parallel",
      "agents": [
        {{"agent_type": "<agent>", "task": "<sous-tâche précise>", "context": ""}}
      ]
    }}
  ],
  "success_criteria": "<description de ce qui constitue une réponse complète>",
  "reasoning": "<explication du choix de stratégie en 1-2 phrases>"
}}"""


_EVALUATION_PROMPT = """Tu es un évaluateur expert de qualité de réponses d'agents IA.

TÂCHE ORIGINALE : {task}

AGENT : {agent_type}
RÉPONSE DE L'AGENT :
{answer}

Évalue si cette réponse répond COMPLÈTEMENT et CORRECTEMENT à la tâche.
Réponds UNIQUEMENT en JSON valide :
{{
  "score": 0.0,
  "complete": false,
  "missing": ["aspect manquant 1", "aspect manquant 2"],
  "retry": false,
  "retry_hint": "instruction précise pour améliorer la réponse au prochain essai"
}}

Critères de scoring :
- 0.9–1.0 : Réponse complète, précise, bien structurée
- 0.7–0.9 : Réponse correcte mais manque quelques détails mineurs
- 0.5–0.7 : Réponse partielle, plusieurs éléments importants manquants
- 0.0–0.5 : Réponse insuffisante, hors sujet ou vide

Mets retry=true SEULEMENT si score < 0.6 ET qu'il y a des éléments clairement manquants et récupérables."""


class ManagerAgent:
    """
    Orchestrateur principal avec :
    - Pré-analyse et plan d'exécution (single / parallel / sequential / hybrid)
    - Dispatch parallèle via ThreadPoolExecutor
    - Dispatch séquentiel avec passage de contexte
    - Auto-évaluation des résultats + retry automatique
    """

    def __init__(self, config: dict, step_callback: Optional[Callable] = None):
        self.config = config
        self._step_callback = step_callback

        self.logger = AgentLogger(
            name="Manager",
            log_file=config.get("logging", {}).get("file"),
            level=config.get("logging", {}).get("level", "INFO"),
            colors=config.get("logging", {}).get("colors", True),
        )

        self.llm = LLMClient(config["llm"])
        self.db  = DBManager(config["databases"])

        agent_cfg = config.get("agents", {})
        sec_cfg   = config.get("security", {})
        ch_cfg    = config.get("clickhouse_agents", {})
        orch_cfg  = config.get("orchestration", {})

        self.max_steps       = int(agent_cfg.get("max_steps", 20))
        self.allow_write     = bool(sec_cfg.get("allow_write_queries", False))
        self.max_rows        = int(sec_cfg.get("max_rows_returned", 1000))
        self.result_dir      = agent_cfg.get("result_dir", "./results")
        self.parallel_agents = int(agent_cfg.get("parallel_agents", 4))

        # Orchestration settings
        self._eval_enabled    = bool(orch_cfg.get("eval_enabled", True))
        self._eval_threshold  = float(orch_cfg.get("eval_threshold", 0.6))
        self._eval_max_retries = int(orch_cfg.get("eval_max_retries", 1))
        self._parallel_max_workers = int(
            orch_cfg.get("parallel_max_workers", self.parallel_agents)
        )

        # Config ClickHouse spécialistes
        self._ch_cfg = {
            "allow_ddl":      bool(ch_cfg.get("allow_ddl", False)),
            "table_prefix":   ch_cfg.get("table_prefix", "agent_"),
            "templates":      ch_cfg.get("templates", {}),
            "semantic_layer": ch_cfg.get("semantic_layer", {}),
        }

        # Config RAG JSON
        self._rag_cfg = config.get("rag_json", {})

        # Per-agent overrides
        self._agent_overrides: Dict[str, Dict] = config.get("agent_overrides", {})

        # Custom agents définis par l'utilisateur
        self._custom_agents: List[Dict] = config.get("custom_agents", [])
        self._build_custom_registry()

        # Mémoire partagée du manager
        self.memory = MemoryManager(
            working_window=6,
            compress_threshold=10,
            agent_name="Manager",
        )

        self.tool_executor = ToolExecutor(
            db_manager=self.db,
            memory=self.memory,
            allow_write=self.allow_write,
            max_rows=self.max_rows,
            dispatch_callback=self._dispatch_agent,
        )

        self.engine = AgentEngine(
            llm=self.llm,
            memory=self.memory,
            tool_executor=self.tool_executor,
            logger=self.logger,
            agent_name="ManagerAgent",
            specialization="multi-agent orchestration, parallelization and adaptive task routing",
            mission=MANAGER_MISSION,
            max_steps=self.max_steps,
            reflection_interval=5,
            step_callback=step_callback,
        )

        self._sub_agent_results: Dict[str, Any] = {}
        # Lock protects both _sub_agent_results AND memory writes from sub-agents
        self._sub_agent_lock = threading.Lock()

    # ------------------------------------------------------------------ #
    #  Registre des agents personnalisés                                   #
    # ------------------------------------------------------------------ #

    def _build_custom_registry(self):
        """Construit le registre dynamique des agents définis par l'utilisateur."""
        self._custom_registry: Dict[str, Dict] = {}
        for ca in self._custom_agents:
            key = ca.get("name", "").strip().lower()
            if key:
                self._custom_registry[key] = ca

    # ------------------------------------------------------------------ #
    #  Point d'entrée principal                                            #
    # ------------------------------------------------------------------ #

    def run(self, task: str) -> Dict[str, Any]:
        """
        Exécute le pipeline multi-agents pour la tâche donnée.
        Retourne un dict avec answer, findings, sous-agents.
        """
        start = time.time()

        # 1. Vérification des connexions DB
        self._check_connections()

        # 2. Pré-chargement du schéma
        self._preload_schema()

        # 3. Pré-analyse → plan d'orchestration
        routing_hint, orch_plan = self._pre_analyze_task(task)
        if routing_hint:
            self.logger.info(f"Routage pré-analyse → {routing_hint}")
            self.memory.store_fact(
                "routing_hint",
                routing_hint,
                source="pre-analysis",
                category="finding",
            )
        if orch_plan:
            strategy = orch_plan.get("strategy", "single")
            criteria = orch_plan.get("success_criteria", "")
            self.memory.store_fact(
                "orchestration_plan",
                json.dumps(orch_plan, ensure_ascii=False),
                source="pre-analysis",
                category="finding",
            )
            if criteria:
                self.memory.store_fact(
                    "success_criteria",
                    criteria,
                    source="pre-analysis",
                    category="finding",
                )
            self.logger.info(
                f"Plan d'orchestration: stratégie={strategy}, "
                f"phases={len(orch_plan.get('execution_plan', []))}"
            )

        # 4. Boucle ReAct du manager (orchestration)
        self.logger.section("Manager Agent Starting")
        result = self.engine.run(task)

        # 5. Attache les résultats des sous-agents
        result["sub_agents"] = self._sub_agent_results
        result["total_duration_s"] = round(time.time() - start, 2)

        # 6. Sauvegarde sur disque
        saved_path = self._save_result(task, result)
        if saved_path:
            result["saved_to"] = saved_path

        # 7. Affichage de la réponse finale
        self.logger.final_answer(
            result.get("answer", "(pas de réponse)"),
            result.get("summary", ""),
        )

        return result

    # ------------------------------------------------------------------ #
    #  Pré-analyse intelligente du routage et plan d'orchestration         #
    # ------------------------------------------------------------------ #

    def _pre_analyze_task(self, task: str) -> Tuple[str, Optional[Dict]]:
        """
        Appel LLM rapide pour identifier la stratégie d'exécution optimale.
        Retourne (routing_hint: str, orchestration_plan: dict | None).
        """
        try:
            prompt = _ORCHESTRATION_PLAN_PROMPT.format(task=task)
            messages = [
                {
                    "role": "system",
                    "content": (
                        "Tu es un orchestrateur d'agents IA. "
                        "Réponds uniquement en JSON valide, sans markdown."
                    ),
                },
                {"role": "user", "content": prompt},
            ]
            raw      = self.llm.complete(messages)
            decision = self.llm._extract_json(raw)

            if not decision:
                return "", None

            strategy      = decision.get("strategy", "single")
            primary       = decision.get("primary_agent", "")
            reasoning     = decision.get("reasoning", "")
            exec_plan     = decision.get("execution_plan", [])

            # Valide que l'agent primaire existe (pour single)
            all_agents = set(AGENT_REGISTRY.keys()) | set(self._custom_registry.keys())
            if primary and primary not in all_agents:
                for name in all_agents:
                    if primary.lower() in name or name in primary.lower():
                        primary = name
                        break
                else:
                    primary = ""

            # Valide et nettoie le plan d'exécution
            cleaned_plan: List[Dict] = []
            for phase in exec_plan:
                valid_agents = []
                for a in phase.get("agents", []):
                    atype = a.get("agent_type", "").lower()
                    if atype in all_agents:
                        valid_agents.append(a)
                    else:
                        # Matching partiel
                        for name in all_agents:
                            if atype in name or name in atype:
                                a["agent_type"] = name
                                valid_agents.append(a)
                                break
                if valid_agents:
                    cleaned_plan.append({
                        "phase": phase.get("phase", len(cleaned_plan) + 1),
                        "mode":  phase.get("mode", "sequential"),
                        "agents": valid_agents,
                    })

            decision["execution_plan"] = cleaned_plan

            # Construit le hint textuel pour la mémoire du manager
            if strategy == "single" and primary:
                hint = (
                    f"PRÉ-ANALYSE : stratégie '{strategy}'. "
                    f"Agent recommandé : '{primary}'. "
                    f"Raison : {reasoning}. "
                    f"→ Utilise dispatch_agent('{primary}', task=...)."
                )
            elif strategy in ("parallel",) and cleaned_plan:
                agents_in_plan = [
                    a["agent_type"]
                    for ph in cleaned_plan
                    for a in ph.get("agents", [])
                ]
                hint = (
                    f"PRÉ-ANALYSE : stratégie '{strategy}'. "
                    f"Agents recommandés en parallèle : {agents_in_plan}. "
                    f"Raison : {reasoning}. "
                    f"→ Utilise dispatch_agents_parallel([...]) pour les lancer simultanément."
                )
            elif strategy == "sequential" and cleaned_plan:
                agents_in_order = [
                    a["agent_type"]
                    for ph in cleaned_plan
                    for a in ph.get("agents", [])
                ]
                hint = (
                    f"PRÉ-ANALYSE : stratégie '{strategy}'. "
                    f"Chaîne d'agents : {agents_in_order}. "
                    f"Raison : {reasoning}. "
                    f"→ Utilise dispatch_agents_sequential([...]) pour les enchaîner."
                )
            else:
                hint = (
                    f"PRÉ-ANALYSE : stratégie '{strategy}'. Raison : {reasoning}."
                )

            return hint, decision

        except Exception as e:
            self.logger.warn(f"Pré-analyse échouée (non bloquant) : {e}")
            return "", None

    # ------------------------------------------------------------------ #
    #  Auto-évaluation de la qualité d'un résultat                        #
    # ------------------------------------------------------------------ #

    def _evaluate_agent_result(
        self,
        task: str,
        agent_type: str,
        answer: str,
    ) -> Dict[str, Any]:
        """
        Appel LLM rapide pour noter la qualité de la réponse d'un agent.
        Retourne {score, complete, missing, retry, retry_hint}.
        """
        default = {"score": 1.0, "complete": True, "retry": False, "retry_hint": ""}
        if not self._eval_enabled:
            return default

        # Heuristique rapide : si la réponse est longue et non vide → OK probable
        if len(str(answer).strip()) > 300:
            return default

        try:
            prompt = _EVALUATION_PROMPT.format(
                task=task,
                agent_type=agent_type,
                answer=str(answer)[:3000],
            )
            messages = [
                {
                    "role": "system",
                    "content": (
                        "Tu es un évaluateur de qualité. "
                        "Réponds uniquement en JSON valide, sans markdown."
                    ),
                },
                {"role": "user", "content": prompt},
            ]
            raw    = self.llm.complete(messages)
            parsed = self.llm._extract_json(raw)
            if parsed:
                return parsed
        except Exception as e:
            self.logger.warn(f"Évaluation échouée (non bloquant) : {e}")

        return default

    # ------------------------------------------------------------------ #
    #  Dispatch des sous-agents                                            #
    # ------------------------------------------------------------------ #

    def _dispatch_agent(
        self,
        agent_type: str,
        task: str,
        context: Optional[str] = None,
        _retry: int = 0,
    ) -> Any:
        """
        Instancie et exécute un sous-agent.
        Si l'auto-évaluation détecte une réponse insuffisante, relance
        automatiquement avec un hint d'amélioration (configurable).
        """
        agent_type_lower = agent_type.lower().strip()

        # --- Vérifie les agents personnalisés en premier ---
        custom_def = self._custom_registry.get(agent_type_lower)

        # --- Vérifie si l'agent est désactivé ---
        override = self._agent_overrides.get(agent_type_lower, {})
        if not override.get("enabled", True):
            msg = f"Agent '{agent_type}' est désactivé dans la configuration."
            self.logger.warn(msg)
            return msg

        # --- Émet l'événement dispatch vers l'UI ---
        if self._step_callback:
            try:
                self._step_callback({
                    "type":       "dispatch",
                    "agent_type": agent_type,
                    "task":       task,
                    "retry":      _retry,
                })
            except Exception:
                pass

        self.logger.manager_dispatch(agent_type, task)
        if _retry > 0:
            self.logger.info(f"  → Retry #{_retry} pour '{agent_type}'")

        # Contexte partagé depuis la mémoire du manager
        shared_context = self._build_shared_context(context)

        # Paramètres de steps par agent
        agent_max_steps  = int(override.get("max_steps",  max(10, self.max_steps // 2)))
        agent_reflection = int(override.get("reflection_interval", 5))

        # --- Instanciation de l'agent ---
        if custom_def:
            agent = self._build_custom_agent(custom_def, agent_type, agent_max_steps)
        else:
            agent_class = AGENT_REGISTRY.get(agent_type_lower)
            if agent_class is None:
                available = sorted(
                    list(AGENT_REGISTRY.keys()) + list(self._custom_registry.keys())
                )
                return (
                    f"Type d'agent inconnu '{agent_type}'. "
                    f"Agents disponibles : {available}"
                )
            agent = self._build_builtin_agent(agent_type_lower, agent_class, agent_max_steps)
            if hasattr(agent, "engine"):
                agent.engine.reflection_interval = agent_reflection

        # --- Exécution ---
        sub_result = agent.run(task, context=shared_context)
        answer     = sub_result.get("answer", "Sous-agent terminé sans réponse explicite.")

        self.logger.manager_result(agent_type, sub_result.get("summary", "done"))

        # --- Émet l'événement dispatch_done vers l'UI ---
        if self._step_callback:
            try:
                self._step_callback({
                    "type":       "dispatch_done",
                    "agent_type": agent_type,
                    "summary":    sub_result.get("summary", ""),
                    "steps":      sub_result.get("steps_used", 0),
                    "retry":      _retry,
                })
            except Exception:
                pass

        # --- Auto-évaluation + retry ---
        if _retry < self._eval_max_retries:
            eval_res = self._evaluate_agent_result(task, agent_type, answer)
            score = float(eval_res.get("score", 1.0))
            if eval_res.get("retry") and score < self._eval_threshold:
                hint = eval_res.get("retry_hint", "")
                missing = eval_res.get("missing", [])
                self.logger.info(
                    f"Auto-évaluation: score={score:.2f} < {self._eval_threshold} "
                    f"pour '{agent_type}'. Relance avec hint: {hint[:100]}"
                )
                retry_ctx = (
                    f"{context or ''}\n\n"
                    f"[ÉVALUATION PRÉCÉDENTE — score {score:.2f}]\n"
                    f"Éléments manquants : {', '.join(missing)}\n"
                    f"Amélioration requise : {hint}"
                ).strip()

                # Stocke l'évaluation dans la mémoire avant le retry
                with self._sub_agent_lock:
                    self.memory.store_fact(
                        f"eval_{agent_type}_retry{_retry}",
                        {"score": score, "missing": missing, "hint": hint},
                        source="evaluator",
                        category="finding",
                    )

                return self._dispatch_agent(
                    agent_type=agent_type,
                    task=task,
                    context=retry_ctx,
                    _retry=_retry + 1,
                )

        # --- Stockage thread-safe des résultats et findings ---
        with self._sub_agent_lock:
            # Findings du sous-agent → mémoire du manager
            for cat, items in sub_result.get("findings", {}).items():
                for item in items:
                    key = f"{agent_type}_{item['key']}"
                    self.memory.store_fact(
                        key, item["value"],
                        source=f"sub-agent:{agent_type}",
                        category=cat,
                        confidence=item.get("confidence", 0.8),
                    )

            # Cache du résultat
            ts  = datetime.now().strftime("%H%M%S%f")
            key = f"{agent_type}_{ts}"
            self._sub_agent_results[key] = {
                "agent":    agent_type,
                "task":     task,
                "summary":  sub_result.get("summary", ""),
                "findings": sub_result.get("findings", {}),
                "steps":    sub_result.get("steps_used", 0),
                "retry":    _retry,
            }

        return answer

    def _build_custom_agent(
        self,
        custom_def: Dict,
        agent_type: str,
        max_steps: int,
    ) -> CustomAgent:
        """Construit un agent personnalisé défini par l'utilisateur."""
        template_key = custom_def.get("template", "analyst").lower()
        template_cls = AGENT_REGISTRY.get(template_key, AnalystAgent)
        return CustomAgent(
            llm=self.llm,
            db=self.db,
            logger=self.logger,
            name=custom_def.get("display_name", agent_type),
            specialization=custom_def.get(
                "specialization", getattr(template_cls, "specialization", "")
            ),
            mission=custom_def.get(
                "mission", getattr(template_cls, "mission", "")
            ),
            max_steps=int(custom_def.get("max_steps", max_steps)),
            allow_write=self.allow_write,
            max_rows=self.max_rows,
            step_callback=self._step_callback,
        )

    def _build_builtin_agent(
        self,
        agent_type: str,
        agent_class,
        max_steps: int,
    ):
        """Construit un agent natif en respectant son constructeur."""

        # Agents avec constructeur simple (pas de allow_write / max_rows)
        if agent_type in _SIMPLE_CONSTRUCTOR_AGENTS:
            return agent_class(
                llm=self.llm,
                db=self.db,
                logger=self.logger,
                max_steps=max_steps,
                step_callback=self._step_callback,
            )

        # Agents spécialistes ClickHouse
        if agent_type in _CH_SPECIALIST_AGENTS:
            return agent_class(
                llm=self.llm,
                db=self.db,
                logger=self.logger,
                max_steps=max_steps,
                allow_write=self.allow_write,
                allow_ddl=self._ch_cfg["allow_ddl"],
                max_rows=self.max_rows,
                table_prefix=self._ch_cfg["table_prefix"],
                templates=self._ch_cfg["templates"],
                semantic_layer=self._ch_cfg["semantic_layer"],
                step_callback=self._step_callback,
            )

        # Agents BaseAgent standard (analyst, quality, pattern, query)
        return agent_class(
            llm=self.llm,
            db=self.db,
            logger=self.logger,
            max_steps=max_steps,
            allow_write=self.allow_write,
            max_rows=self.max_rows,
            step_callback=self._step_callback,
        )

    # ------------------------------------------------------------------ #
    #  Helpers                                                             #
    # ------------------------------------------------------------------ #

    def _check_connections(self):
        self.logger.section("Vérification des connexions base de données")
        status = self.db.status()
        if not status:
            self.logger.warn("Aucune base de données configurée ou activée.")
        for db_name, ok in status.items():
            if ok:
                self.logger.info(f"{db_name}: connecté")
            else:
                self.logger.warn(f"{db_name}: INJOIGNABLE — les requêtes échoueront")

    def _preload_schema(self):
        """Pré-charge le schéma DB dans la mémoire du manager."""
        for db_name in self.db.available_databases():
            try:
                tables = self.db.get_tables(db=db_name)
                self.memory.store_fact(
                    f"tables_{db_name}", tables,
                    source="preload", category="schema",
                )
                self.logger.info(
                    f"Schéma pré-chargé pour {db_name} : {len(tables)} tables"
                )
            except Exception as e:
                self.logger.warn(f"Pré-chargement schéma échoué pour {db_name} : {e}")

    def _build_shared_context(self, extra: Optional[str] = None) -> str:
        """Construit le contexte partagé à passer aux sous-agents."""
        parts = []
        schema_facts = self.memory.get_facts_by_category("schema")
        if schema_facts:
            parts.append("=== SCHÉMA CONNU ===")
            for f in schema_facts[:5]:
                parts.append(
                    f"{f.key}: {json.dumps(f.value, ensure_ascii=False, default=str)[:300]}"
                )

        findings = self.memory.get_facts_by_category("finding")
        if findings:
            parts.append("\n=== RÉSULTATS MANAGER JUSQU'ICI ===")
            for f in findings[:10]:
                parts.append(str(f))

        if extra:
            parts.append(f"\n=== CONTEXTE ADDITIONNEL ===\n{extra}")

        return "\n".join(parts)

    def _save_result(self, task: str, result: Dict) -> Optional[str]:
        """Sauvegarde le résultat dans un fichier JSON horodaté."""
        try:
            os.makedirs(self.result_dir, exist_ok=True)
            ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"result_{ts}.json"
            path     = os.path.join(self.result_dir, filename)

            def default_serial(obj):
                if hasattr(obj, "__dict__"):
                    return obj.__dict__
                return str(obj)

            with open(path, "w", encoding="utf-8") as f:
                json.dump(
                    {"task": task, "timestamp": ts, **result},
                    f, indent=2, ensure_ascii=False, default=default_serial,
                )
            self.logger.info(f"Résultat sauvegardé → {path}")
            return path
        except Exception as e:
            self.logger.warn(f"Impossible de sauvegarder le résultat : {e}")
            return None
