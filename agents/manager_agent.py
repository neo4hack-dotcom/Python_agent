"""
Manager Agent — orchestre les sous-agents, agrège les résultats.

Fonctionnement :
  1. Analyse la tâche utilisateur
  2. Planifie les sous-agents nécessaires
  3. Dispatche chaque sous-agent séquentiellement (ou en parallèle si threading activé)
  4. Agrège les résultats
  5. Produit un rapport final

Optimisations :
  - Cache des résultats de sous-agents
  - Passer le contexte (schéma, findings) entre agents
  - Exécution parallèle optionnelle via threading
"""
import json
import time
import os
import threading
from typing import Any, Dict, List, Optional
from datetime import datetime

from core.llm_client  import LLMClient
from core.db_manager  import DBManager
from core.memory      import MemoryManager
from core.tools       import ToolExecutor
from core.engine      import AgentEngine
from agents.base_agent    import BaseAgent
from agents.analyst_agent import AnalystAgent
from agents.quality_agent import QualityAgent
from agents.pattern_agent import PatternAgent
from agents.query_agent   import QueryAgent
from utils.logger     import AgentLogger
from utils.prompts    import MANAGER_MISSION


AGENT_REGISTRY = {
    "analyst": AnalystAgent,
    "quality": QualityAgent,
    "pattern": PatternAgent,
    "query":   QueryAgent,
}


class ManagerAgent:
    """
    Top-level orchestrator.
    Peut être utilisé directement ou via main.py.
    """

    def __init__(self, config: dict):
        self.config = config
        self.logger = AgentLogger(
            name="Manager",
            log_file=config.get("logging", {}).get("file"),
            level=config.get("logging", {}).get("level", "INFO"),
            colors=config.get("logging", {}).get("colors", True),
        )

        self.llm = LLMClient(config["llm"])
        self.db  = DBManager(config["databases"])

        agent_cfg   = config.get("agents", {})
        sec_cfg     = config.get("security", {})
        self.max_steps       = int(agent_cfg.get("max_steps", 20))
        self.allow_write     = bool(sec_cfg.get("allow_write_queries", False))
        self.max_rows        = int(sec_cfg.get("max_rows_returned", 1000))
        self.result_dir      = agent_cfg.get("result_dir", "./results")
        self.parallel_agents = int(agent_cfg.get("parallel_agents", 1))

        # Shared memory for the manager itself
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
            specialization="multi-agent orchestration and data analysis coordination",
            mission=MANAGER_MISSION,
            max_steps=self.max_steps,
            reflection_interval=5,
        )

        self._sub_agent_results: Dict[str, Any] = {}
        self._sub_agent_lock = threading.Lock()

    # ------------------------------------------------------------------ #
    #  Main entry point                                                    #
    # ------------------------------------------------------------------ #

    def run(self, task: str) -> Dict[str, Any]:
        """
        Execute the full multi-agent pipeline for the given task.
        Returns a result dict with answer, findings, sub-agent results.
        """
        start = time.time()

        # 1. Check DB connectivity
        self._check_connections()

        # 2. Pre-load schema into manager memory (cheap, avoids first steps being wasted)
        self._preload_schema()

        # 3. Run manager engine (orchestration loop)
        self.logger.section("Manager Agent Starting")
        result = self.engine.run(task)

        # 4. Attach sub-agent results
        result["sub_agents"] = self._sub_agent_results
        result["total_duration_s"] = round(time.time() - start, 2)

        # 5. Save to disk
        saved_path = self._save_result(task, result)
        if saved_path:
            result["saved_to"] = saved_path

        # 6. Print final answer
        self.logger.final_answer(
            result.get("answer", "(no answer)"),
            result.get("summary", ""),
        )

        return result

    # ------------------------------------------------------------------ #
    #  Sub-agent dispatch (called via ToolExecutor dispatch_callback)      #
    # ------------------------------------------------------------------ #

    def _dispatch_agent(
        self,
        agent_type: str,
        task: str,
        context: Optional[str] = None,
    ) -> Any:
        agent_class = AGENT_REGISTRY.get(agent_type.lower())
        if agent_class is None:
            return f"Unknown agent type '{agent_type}'. Available: {list(AGENT_REGISTRY.keys())}"

        self.logger.manager_dispatch(agent_type, task)

        # Build context from manager memory + passed context
        shared_context = self._build_shared_context(context)

        agent = agent_class(
            llm=self.llm,
            db=self.db,
            logger=self.logger,
            max_steps=max(10, self.max_steps // 2),
            allow_write=self.allow_write,
            max_rows=self.max_rows,
        )

        sub_result = agent.run(task, context=shared_context)

        self.logger.manager_result(agent_type, sub_result.get("summary", "done"))

        # Store sub-agent findings in manager memory
        for cat, items in sub_result.get("findings", {}).items():
            for item in items:
                key = f"{agent_type}_{item['key']}"
                self.memory.store_fact(key, item["value"],
                                       source=f"sub-agent:{agent_type}",
                                       category=cat,
                                       confidence=item.get("confidence", 0.8))

        # Cache result
        with self._sub_agent_lock:
            ts = datetime.now().strftime("%H%M%S")
            key = f"{agent_type}_{ts}"
            self._sub_agent_results[key] = {
                "agent":    agent_type,
                "task":     task,
                "summary":  sub_result.get("summary", ""),
                "findings": sub_result.get("findings", {}),
                "steps":    sub_result.get("steps_used", 0),
            }

        return sub_result.get("answer", "Sub-agent completed without explicit answer.")

    # ------------------------------------------------------------------ #
    #  Helpers                                                             #
    # ------------------------------------------------------------------ #

    def _check_connections(self):
        self.logger.section("Checking database connections")
        status = self.db.status()
        if not status:
            self.logger.warn("No databases configured or none enabled.")
        for db_name, ok in status.items():
            if ok:
                self.logger.info(f"{db_name}: connected")
            else:
                self.logger.warn(f"{db_name}: UNREACHABLE — queries will fail")

    def _preload_schema(self):
        """Pre-load schema info into manager memory so agents don't waste steps on it."""
        for db_name in self.db.available_databases():
            try:
                tables = self.db.get_tables(db=db_name)
                self.memory.store_fact(
                    f"tables_{db_name}", tables,
                    source="preload", category="schema",
                )
                self.logger.info(
                    f"Schema preloaded for {db_name}: {len(tables)} tables"
                )
            except Exception as e:
                self.logger.warn(f"Schema preload failed for {db_name}: {e}")

    def _build_shared_context(self, extra: Optional[str] = None) -> str:
        """Build context to pass to sub-agents: known schema + findings so far."""
        parts = []
        schema_facts = self.memory.get_facts_by_category("schema")
        if schema_facts:
            parts.append("=== KNOWN SCHEMA ===")
            for f in schema_facts[:5]:
                parts.append(f"{f.key}: {json.dumps(f.value, ensure_ascii=False, default=str)[:300]}")

        findings = self.memory.get_facts_by_category("finding")
        if findings:
            parts.append("\n=== MANAGER FINDINGS SO FAR ===")
            for f in findings[:10]:
                parts.append(str(f))

        if extra:
            parts.append(f"\n=== ADDITIONAL CONTEXT ===\n{extra}")

        return "\n".join(parts)

    def _save_result(self, task: str, result: Dict) -> Optional[str]:
        """Save result to a timestamped JSON file in results/ dir."""
        try:
            os.makedirs(self.result_dir, exist_ok=True)
            ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"result_{ts}.json"
            path     = os.path.join(self.result_dir, filename)

            # Make result JSON-serializable
            def default_serial(obj):
                if hasattr(obj, "__dict__"):
                    return obj.__dict__
                return str(obj)

            with open(path, "w", encoding="utf-8") as f:
                json.dump(
                    {"task": task, "timestamp": ts, **result},
                    f, indent=2, ensure_ascii=False, default=default_serial
                )
            self.logger.info(f"Result saved → {path}")
            return path
        except Exception as e:
            self.logger.warn(f"Could not save result: {e}")
            return None
