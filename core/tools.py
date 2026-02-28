"""
Tool Registry — Définit et exécute tous les outils disponibles pour les agents.

Outils disponibles :
  DB         : execute_sql, list_tables, describe_table, get_sample, get_schema
  Analysis   : compute_stats, detect_nulls, detect_duplicates, detect_outliers
  Memory     : store_finding, recall_facts
  Agent      : dispatch_agent, dispatch_agents_parallel, dispatch_agents_sequential
  System     : save_result, think, final_answer
"""
import json
import time
import re
import concurrent.futures
from typing import Any, Dict, List, Optional, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from core.db_manager import DBManager
    from core.memory import MemoryManager


# --------------------------------------------------------------------------- #
#  Tool definition schema                                                      #
# --------------------------------------------------------------------------- #

TOOL_DEFINITIONS: List[Dict] = [
    {
        "name": "execute_sql",
        "description": (
            "Execute a read-only SQL query on ClickHouse or Oracle. "
            "Returns a list of rows (dicts). Use for all data fetching."
        ),
        "params": {
            "query": "SQL query string",
            "db": "Target database: 'clickhouse' (default) or 'oracle'",
            "max_rows": "Maximum rows to return (default 1000)",
            "use_cache": "Use cached result if available (default true)",
        },
        "required": ["query"],
    },
    {
        "name": "execute_write_sql",
        "description": "Execute a write SQL query (INSERT/UPDATE/CREATE…). Requires allow_write=true in config.",
        "params": {
            "query": "SQL query string",
            "db": "Target database: 'clickhouse' or 'oracle'",
        },
        "required": ["query"],
    },
    {
        "name": "list_tables",
        "description": "List all tables in a database.",
        "params": {
            "db": "Target database: 'clickhouse' (default) or 'oracle'",
        },
        "required": [],
    },
    {
        "name": "describe_table",
        "description": "Get column definitions (name, type, nullable) for a specific table.",
        "params": {
            "table": "Table name",
            "db": "Target database: 'clickhouse' (default) or 'oracle'",
        },
        "required": ["table"],
    },
    {
        "name": "get_sample",
        "description": "Get a sample of N rows from a table.",
        "params": {
            "table": "Table name",
            "db": "Target database: 'clickhouse' (default) or 'oracle'",
            "n": "Number of sample rows (default 5)",
        },
        "required": ["table"],
    },
    {
        "name": "get_schema",
        "description": "Get the full schema (all tables and their columns) for a database.",
        "params": {
            "db": "Target database: 'clickhouse' (default) or 'oracle'",
        },
        "required": [],
    },
    {
        "name": "compute_stats",
        "description": (
            "Compute statistics (count, nulls, min, max, avg, stddev, distinct count) "
            "for a column in a table."
        ),
        "params": {
            "table":  "Table name",
            "column": "Column name",
            "db":     "Target database: 'clickhouse' (default) or 'oracle'",
        },
        "required": ["table", "column"],
    },
    {
        "name": "detect_nulls",
        "description": "Report null/missing value rates for all columns (or specific columns) in a table.",
        "params": {
            "table":   "Table name",
            "columns": "List of column names (optional, default = all columns)",
            "db":      "Target database: 'clickhouse' (default) or 'oracle'",
        },
        "required": ["table"],
    },
    {
        "name": "detect_duplicates",
        "description": "Detect duplicate rows based on a set of key columns.",
        "params": {
            "table":      "Table name",
            "key_columns": "List of column names forming the key",
            "db":         "Target database: 'clickhouse' (default) or 'oracle'",
            "limit":      "Return top N duplicate groups (default 20)",
        },
        "required": ["table", "key_columns"],
    },
    {
        "name": "detect_outliers",
        "description": (
            "Detect statistical outliers in a numeric column using IQR method. "
            "Returns rows where value is beyond Q1-1.5*IQR or Q3+1.5*IQR."
        ),
        "params": {
            "table":  "Table name",
            "column": "Numeric column name",
            "db":     "Target database: 'clickhouse' (default) or 'oracle'",
            "limit":  "Max outlier rows to return (default 50)",
        },
        "required": ["table", "column"],
    },
    {
        "name": "store_finding",
        "description": "Store an important finding in semantic memory for later retrieval.",
        "params": {
            "key":        "Short identifier for this finding",
            "value":      "The finding content (text or structured data)",
            "category":   "Category: 'anomaly', 'pattern', 'metric', 'schema', 'finding'",
            "confidence": "Confidence score 0.0-1.0 (default 1.0)",
        },
        "required": ["key", "value"],
    },
    {
        "name": "recall_facts",
        "description": "Retrieve stored facts/findings from semantic memory, optionally filtered by category.",
        "params": {
            "category": "Filter by category: 'anomaly', 'pattern', 'metric', 'schema', 'finding' (optional)",
        },
        "required": [],
    },
    {
        "name": "dispatch_agent",
        "description": (
            "Dispatch ONE specialized sub-agent to handle a specific subtask. "
            "Available agent types: analyst, quality, pattern, query, excel, text, "
            "filesystem, web, sql_analyst, clickhouse_generic, clickhouse_table_manager, "
            "clickhouse_writer, clickhouse_specific, text_to_sql_translator, rag_json."
        ),
        "params": {
            "agent_type": "Sub-agent type (e.g. 'analyst', 'quality', 'web', 'excel'…)",
            "task":       "Detailed task description for the sub-agent",
            "context":    "Optional context to pass to the sub-agent",
        },
        "required": ["agent_type", "task"],
    },
    {
        "name": "dispatch_agents_parallel",
        "description": (
            "Dispatch MULTIPLE sub-agents IN PARALLEL simultaneously. "
            "All agents run concurrently — use when subtasks are INDEPENDENT of each other. "
            "Much faster than dispatching one by one. "
            "Example use: run 'quality' + 'pattern' + 'analyst' at the same time on the same data."
        ),
        "params": {
            "agents": (
                "List of agent dispatch specs: "
                "[{'agent_type': 'analyst', 'task': 'describe the subtask', 'context': 'optional'}, ...]"
            ),
            "aggregation_hint": "Optional hint on how to combine/interpret the parallel results",
        },
        "required": ["agents"],
    },
    {
        "name": "dispatch_agents_sequential",
        "description": (
            "Dispatch multiple sub-agents IN SEQUENCE, where each agent's output "
            "is passed as context to the next agent. "
            "Use when subtasks are DEPENDENT (output of one feeds the next). "
            "Example: run 'analyst' first, then pass its findings to 'excel' to create a report."
        ),
        "params": {
            "agents": (
                "Ordered list of agent specs: "
                "[{'agent_type': 'analyst', 'task': 'desc', 'context': 'optional'}, ...]"
            ),
            "pass_context": "Pass each agent's result as context to the next agent (default: true)",
        },
        "required": ["agents"],
    },
    {
        "name": "think",
        "description": (
            "Pure reasoning step — no external action. Use to plan, reason, "
            "or decompose a complex problem before acting."
        ),
        "params": {
            "reasoning": "Your detailed reasoning",
        },
        "required": ["reasoning"],
    },
    {
        "name": "final_answer",
        "description": "Emit the final answer/report when the task is complete.",
        "params": {
            "answer":  "The complete answer, analysis, or report",
            "summary": "One-sentence executive summary",
        },
        "required": ["answer"],
    },
]

TOOL_NAMES = {t["name"] for t in TOOL_DEFINITIONS}


# --------------------------------------------------------------------------- #
#  Tool executor                                                               #
# --------------------------------------------------------------------------- #

class ToolExecutor:
    """
    Executes tools, routing to the correct backend.
    Some tools (dispatch_agent) require a callback injected at runtime.
    """

    def __init__(
        self,
        db_manager: "DBManager",
        memory: "MemoryManager",
        allow_write: bool = False,
        max_rows: int = 1000,
        dispatch_callback: Optional[Callable] = None,
    ):
        self.db          = db_manager
        self.memory      = memory
        self.allow_write = allow_write
        self.max_rows    = max_rows
        self._dispatch   = dispatch_callback  # injected by engine

    def execute(self, tool_name: str, params: Dict[str, Any]) -> Any:
        if tool_name not in TOOL_NAMES:
            raise ValueError(f"Unknown tool: {tool_name}")

        method = getattr(self, f"_tool_{tool_name}", None)
        if method is None:
            raise ValueError(f"Tool '{tool_name}' is defined but not implemented")
        return method(**params)

    # ------------------------------------------------------------------ #
    #  DB tools                                                            #
    # ------------------------------------------------------------------ #

    def _tool_execute_sql(
        self,
        query: str,
        db: str = "clickhouse",
        max_rows: int = None,
        use_cache: bool = True,
    ) -> List[Dict]:
        rows = max_rows or self.max_rows
        return self.db.query(query, db=db, max_rows=rows,
                              allow_write=False, use_cache=use_cache)

    def _tool_execute_write_sql(self, query: str, db: str = "clickhouse") -> str:
        if not self.allow_write:
            raise PermissionError(
                "Write queries are disabled. Set security.allow_write_queries=true in config."
            )
        return self.db.execute_write(query, db=db)

    def _tool_list_tables(self, db: str = "clickhouse") -> List[str]:
        return self.db.get_tables(db=db)

    def _tool_describe_table(self, table: str, db: str = "clickhouse") -> List[Dict]:
        return self.db.describe_table(table, db=db)

    def _tool_get_sample(self, table: str, db: str = "clickhouse", n: int = 5) -> List[Dict]:
        return self.db.get_sample(table, db=db, n=n)

    def _tool_get_schema(self, db: str = "clickhouse") -> Dict:
        return self.db.get_schema(db=db)

    # ------------------------------------------------------------------ #
    #  Analysis tools (built on top of execute_sql)                        #
    # ------------------------------------------------------------------ #

    def _tool_compute_stats(
        self, table: str, column: str, db: str = "clickhouse"
    ) -> Dict[str, Any]:
        if db == "clickhouse":
            q = (
                f"SELECT "
                f"  count() AS total_rows, "
                f"  countIf({column} IS NULL) AS null_count, "
                f"  min({column}) AS min_val, "
                f"  max({column}) AS max_val, "
                f"  avg({column}) AS avg_val, "
                f"  stddevPop({column}) AS stddev, "
                f"  uniq({column}) AS distinct_count "
                f"FROM {table}"
            )
        else:
            q = (
                f"SELECT "
                f"  COUNT(*) AS total_rows, "
                f"  SUM(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) AS null_count, "
                f"  MIN({column}) AS min_val, "
                f"  MAX({column}) AS max_val, "
                f"  AVG({column}) AS avg_val, "
                f"  STDDEV({column}) AS stddev, "
                f"  COUNT(DISTINCT {column}) AS distinct_count "
                f"FROM {table}"
            )
        rows = self.db.query(q, db=db, max_rows=1)
        result = rows[0] if rows else {}
        if result.get("total_rows") and result.get("null_count"):
            total = float(result["total_rows"]) or 1
            result["null_rate"] = round(float(result["null_count"]) / total, 4)
        return result

    def _tool_detect_nulls(
        self,
        table: str,
        columns: Optional[List[str]] = None,
        db: str = "clickhouse",
    ) -> List[Dict[str, Any]]:
        # Get column list if not specified
        if not columns:
            schema = self.db.describe_table(table, db=db)
            columns = [c["name"] for c in schema]

        results = []
        for col in columns:
            try:
                stats = self._tool_compute_stats(table=table, column=col, db=db)
                total = float(stats.get("total_rows", 0) or 0)
                nulls = float(stats.get("null_count", 0) or 0)
                results.append({
                    "column":     col,
                    "total_rows": int(total),
                    "null_count": int(nulls),
                    "null_rate":  round(nulls / total, 4) if total else 0,
                })
            except Exception as e:
                results.append({"column": col, "error": str(e)})
        # Sort by null_rate descending
        results.sort(key=lambda r: r.get("null_rate", 0), reverse=True)
        return results

    def _tool_detect_duplicates(
        self,
        table: str,
        key_columns: List[str],
        db: str = "clickhouse",
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        cols = ", ".join(key_columns)
        if db == "clickhouse":
            q = (
                f"SELECT {cols}, count() AS dup_count "
                f"FROM {table} "
                f"GROUP BY {cols} "
                f"HAVING count() > 1 "
                f"ORDER BY dup_count DESC "
                f"LIMIT {limit}"
            )
        else:
            q = (
                f"SELECT {cols}, COUNT(*) AS dup_count "
                f"FROM {table} "
                f"GROUP BY {cols} "
                f"HAVING COUNT(*) > 1 "
                f"ORDER BY dup_count DESC "
                f"FETCH FIRST {limit} ROWS ONLY"
            )
        return self.db.query(q, db=db, max_rows=limit)

    def _tool_detect_outliers(
        self,
        table: str,
        column: str,
        db: str = "clickhouse",
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        # Compute Q1, Q3 first
        if db == "clickhouse":
            q_quantiles = (
                f"SELECT "
                f"  quantile(0.25)({column}) AS q1, "
                f"  quantile(0.75)({column}) AS q3 "
                f"FROM {table}"
            )
        else:
            q_quantiles = (
                f"SELECT "
                f"  PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {column}) AS q1, "
                f"  PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {column}) AS q3 "
                f"FROM {table}"
            )
        qt = self.db.query(q_quantiles, db=db, max_rows=1)
        if not qt:
            return []
        q1  = float(qt[0].get("q1") or 0)
        q3  = float(qt[0].get("q3") or 0)
        iqr = q3 - q1
        low  = q1 - 1.5 * iqr
        high = q3 + 1.5 * iqr

        if db == "clickhouse":
            q_outliers = (
                f"SELECT *, {column} AS _flagged_value "
                f"FROM {table} "
                f"WHERE {column} < {low} OR {column} > {high} "
                f"LIMIT {limit}"
            )
        else:
            q_outliers = (
                f"SELECT * FROM {table} "
                f"WHERE {column} < {low} OR {column} > {high} "
                f"FETCH FIRST {limit} ROWS ONLY"
            )
        outliers = self.db.query(q_outliers, db=db, max_rows=limit)
        return {
            "q1": q1, "q3": q3, "iqr": iqr,
            "lower_bound": low, "upper_bound": high,
            "outlier_count": len(outliers),
            "outlier_samples": outliers[:10],
        }

    # ------------------------------------------------------------------ #
    #  Memory tools                                                        #
    # ------------------------------------------------------------------ #

    def _tool_store_finding(
        self,
        key: str,
        value: Any,
        category: str = "finding",
        confidence: float = 1.0,
    ) -> str:
        self.memory.store_fact(key, value, source="agent-tool",
                               category=category, confidence=confidence)
        return f"Finding '{key}' stored in semantic memory."

    def _tool_recall_facts(self, category: Optional[str] = None) -> List[Dict]:
        if category:
            facts = self.memory.get_facts_by_category(category)
        else:
            facts = self.memory.all_facts()
        return [
            {"key": f.key, "value": f.value, "category": f.category,
             "confidence": f.confidence, "source": f.source}
            for f in facts
        ]

    # ------------------------------------------------------------------ #
    #  Agent dispatch                                                      #
    # ------------------------------------------------------------------ #

    def _tool_dispatch_agent(
        self,
        agent_type: str,
        task: str,
        context: Optional[str] = None,
    ) -> Any:
        if self._dispatch is None:
            # Soft failure: return a recoverable error dict instead of raising.
            # The agent can read this and pivot to final_answer or a different tool.
            return {
                "error": (
                    f"dispatch_agent('{agent_type}') non disponible dans ce contexte : "
                    "aucun callback de dispatch enregistré."
                ),
                "agent_type": agent_type,
                "hint": (
                    "Complète la tâche directement avec les outils disponibles, "
                    "ou appelle final_answer si la tâche dépasse tes capacités."
                ),
            }
        return self._dispatch(agent_type=agent_type, task=task, context=context)

    def _tool_dispatch_agents_parallel(
        self,
        agents: List[Dict],
        aggregation_hint: str = "",
    ) -> Dict[str, Any]:
        """
        Dispatch multiple agents in parallel using ThreadPoolExecutor.
        Each spec: {agent_type, task, context?}
        Returns aggregated results dict.
        """
        if self._dispatch is None:
            return {
                "error": "dispatch_agents_parallel non disponible : aucun callback enregistré.",
                "hint": "Appelle final_answer ou utilise les outils disponibles directement.",
                "results": {}, "errors": {}, "combined": "",
                "agents_dispatched": 0, "successful": 0, "failed": len(agents),
            }
        if not agents:
            return {"error": "No agents specified", "combined": "", "results": {}}

        max_workers = min(len(agents), 8)
        results: Dict[str, Any] = {}
        errors:  Dict[str, str] = {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_key = {}
            for i, spec in enumerate(agents):
                if not isinstance(spec, dict):
                    errors[f"agent_{i}"] = (
                        f"Invalid spec at index {i}: expected dict, got "
                        f"{type(spec).__name__}. Spec: {str(spec)[:100]}"
                    )
                    continue
                atype   = spec.get("agent_type", "analyst")
                task    = spec.get("task", "")
                ctx     = spec.get("context")
                key     = f"{atype}_{i}"
                future  = executor.submit(
                    self._dispatch,
                    agent_type=atype,
                    task=task,
                    context=ctx,
                )
                future_to_key[future] = key

            for future in concurrent.futures.as_completed(future_to_key, timeout=600):
                key = future_to_key[future]
                try:
                    results[key] = future.result()
                except Exception as exc:
                    errors[key] = str(exc)

        parts = [f"=== {k} ===\n{v}" for k, v in results.items()]
        parts += [f"=== {k} [ERREUR] ===\n{v}" for k, v in errors.items()]
        combined = "\n\n".join(parts)
        if aggregation_hint:
            combined = f"[Hint d'agrégation: {aggregation_hint}]\n\n{combined}"

        return {
            "results":           results,
            "errors":            errors,
            "combined":          combined,
            "agents_dispatched": len(agents),
            "successful":        len(results),
            "failed":            len(errors),
        }

    def _tool_dispatch_agents_sequential(
        self,
        agents: List[Dict],
        pass_context: bool = True,
    ) -> Dict[str, Any]:
        """
        Dispatch agents one after another; each agent's result is passed as
        context to the next (when pass_context=True).
        Each spec: {agent_type, task, context?}
        """
        if self._dispatch is None:
            return {
                "error": "dispatch_agents_sequential non disponible : aucun callback enregistré.",
                "hint": "Appelle final_answer ou utilise les outils disponibles directement.",
                "chain_results": [], "final_result": None, "steps_executed": 0, "chain_summary": "",
            }
        if not agents:
            return {"error": "No agents specified", "chain_results": []}

        chain_results: List[Dict] = []
        prev_result: Optional[str] = None

        for i, spec in enumerate(agents):
            if not isinstance(spec, dict):
                chain_results.append({
                    "step":       i + 1,
                    "agent_type": f"unknown_{i}",
                    "task":       "",
                    "error":      (
                        f"Invalid spec at index {i}: expected dict, got "
                        f"{type(spec).__name__}. Spec: {str(spec)[:100]}"
                    ),
                    "success":    False,
                })
                prev_result = f"[Spec invalide à l'étape {i+1}]"
                continue

            atype   = spec.get("agent_type", "analyst")
            task    = spec.get("task", "")
            context = spec.get("context", "")

            if pass_context and prev_result is not None:
                # Serialize the previous result with a generous limit
                if isinstance(prev_result, (dict, list)):
                    prev_str = json.dumps(prev_result, ensure_ascii=False, default=str)
                else:
                    prev_str = str(prev_result)
                prev_str = prev_str[:8000]

                # Embed data DIRECTLY in the task so the agent can't miss it
                task = (
                    f"{task}\n\n"
                    f"[OUTPUT DE L'ÉTAPE PRÉCÉDENTE (étape {i}) — utilise ces données directement]:\n"
                    f"{prev_str}"
                )
                context = (
                    f"{context}\n\n=== OUTPUT DE L'ÉTAPE PRÉCÉDENTE (étape {i}) ==="
                    f"\n{prev_str}"
                ).strip()

            try:
                result = self._dispatch(
                    agent_type=atype,
                    task=task,
                    context=context if context else None,
                )
                chain_results.append({
                    "step":       i + 1,
                    "agent_type": atype,
                    "task":       task,
                    "result":     result,
                    "success":    True,
                })
                prev_result = result
            except Exception as exc:
                chain_results.append({
                    "step":       i + 1,
                    "agent_type": atype,
                    "task":       task,
                    "error":      str(exc),
                    "success":    False,
                })
                prev_result = f"[Erreur de {atype}: {exc}]"

        last = chain_results[-1] if chain_results else {}
        summary_lines = []
        for s in chain_results:
            if s.get("success"):
                snippet = str(s.get("result", ""))[:200]
                summary_lines.append(f"Étape {s['step']} ({s['agent_type']}): {snippet}")
            else:
                summary_lines.append(
                    f"Étape {s['step']} ({s['agent_type']}): ERREUR — {s.get('error', '')}"
                )

        return {
            "chain_results":   chain_results,
            "final_result":    last.get("result") if last.get("success") else None,
            "steps_executed":  len(chain_results),
            "chain_summary":   "\n".join(summary_lines),
        }

    # ------------------------------------------------------------------ #
    #  System tools                                                        #
    # ------------------------------------------------------------------ #

    def _tool_think(self, reasoning: str = "") -> str:
        return f"[REASONING] {reasoning}"

    def _tool_final_answer(self, answer: str, summary: str = "") -> Dict[str, str]:
        return {"answer": answer, "summary": summary or answer[:200]}
