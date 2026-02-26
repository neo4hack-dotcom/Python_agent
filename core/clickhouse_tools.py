"""
ClickHouse-Specific Tool Registry & Executor
=============================================

Extends the base ToolExecutor with ClickHouse-native capabilities:

  explain_query      — Preflight EXPLAIN to validate execution plan
  run_funnel         — windowFunnel() conversion funnel analysis
  run_time_series    — Time series with WITH FILL gap completion
  run_topk           — topK() Space-Saving approximate top-K
  run_hll_count      — uniqHLL12() approximate distinct count
  create_ch_table    — DDL CREATE TABLE (MergeTree family) with guardrails
  alter_ch_table     — ALTER TABLE (add/drop/modify columns); DROP TABLE blocked
  write_agent_table  — Sandboxed INSERT restricted to agent_ prefixed tables
  execute_template   — Parameterized query templates (P1→P4 built-in)
  list_templates     — Enumerate available templates
  dag_plan           — LLM-driven DAG decomposition for complex questions
  nl_to_sql          — Natural-language → optimized ClickHouse SQL
"""
import json
import re
from typing import Any, Dict, List, Optional, Callable, TYPE_CHECKING

from core.tools import ToolExecutor, TOOL_DEFINITIONS

if TYPE_CHECKING:
    from core.db_manager import DBManager
    from core.memory import MemoryManager
    from core.llm_client import LLMClient


# --------------------------------------------------------------------------- #
#  Built-in query templates                                                    #
# --------------------------------------------------------------------------- #

BUILTIN_TEMPLATES: Dict[str, Dict] = {
    "P1": {
        "name": "daily_active_users",
        "description": "Daily Active Users (DAU) with gap-fill over a date range",
        "params": {
            "table":      "Events table name",
            "user_col":   "User identifier column",
            "date_col":   "Date or DateTime column",
            "start_date": "Start date YYYY-MM-DD",
            "end_date":   "End date YYYY-MM-DD (exclusive)",
        },
        "sql": (
            "SELECT\n"
            "    toDate({date_col}) AS day,\n"
            "    uniqHLL12({user_col}) AS dau\n"
            "FROM {table}\n"
            "WHERE toDate({date_col}) >= '{start_date}'\n"
            "  AND toDate({date_col}) <  '{end_date}'\n"
            "GROUP BY day\n"
            "ORDER BY day ASC\n"
            "WITH FILL\n"
            "    FROM toDate('{start_date}')\n"
            "    TO   toDate('{end_date}')\n"
            "    STEP 1"
        ),
    },
    "P2": {
        "name": "funnel_conversion",
        "description": "Multi-step funnel conversion using windowFunnel()",
        "params": {
            "table":     "Events table name",
            "user_col":  "User identifier column",
            "time_col":  "Timestamp (DateTime) column",
            "event_col": "Event name column",
            "step1":     "First funnel event name",
            "step2":     "Second funnel event name",
            "step3":     "Third funnel event name (use same as step2 to ignore)",
            "window_h":  "Conversion window in hours (default 24)",
        },
        "sql": (
            "SELECT\n"
            "    level,\n"
            "    count()                                                 AS users,\n"
            "    round(count() * 100.0 / sum(count()) OVER (), 2)       AS pct_total,\n"
            "    round(count() * 100.0 / first_value(count()) OVER (), 2) AS pct_top\n"
            "FROM (\n"
            "    SELECT\n"
            "        {user_col},\n"
            "        windowFunnel(toUInt32({window_h}) * 3600)(\n"
            "            {time_col},\n"
            "            {event_col} = '{step1}',\n"
            "            {event_col} = '{step2}',\n"
            "            {event_col} = '{step3}'\n"
            "        ) AS level\n"
            "    FROM {table}\n"
            "    WHERE {event_col} IN ('{step1}', '{step2}', '{step3}')\n"
            "    GROUP BY {user_col}\n"
            ")\n"
            "WHERE level > 0\n"
            "GROUP BY level\n"
            "ORDER BY level ASC"
        ),
    },
    "P3": {
        "name": "retention_cohort",
        "description": "Weekly cohort Day-N retention analysis",
        "params": {
            "table":       "Events table name",
            "user_col":    "User identifier column",
            "time_col":    "DateTime column",
            "cohort_days": "Max retention day offset (default 7)",
        },
        "sql": (
            "SELECT\n"
            "    cohort_week,\n"
            "    day_offset,\n"
            "    uniqHLL12(user_id) AS retained_users\n"
            "FROM (\n"
            "    SELECT\n"
            "        e.{user_col}                                              AS user_id,\n"
            "        toMonday(first_seen.first_day)                            AS cohort_week,\n"
            "        dateDiff('day', first_seen.first_day, toDate(e.{time_col})) AS day_offset\n"
            "    FROM {table} AS e\n"
            "    INNER JOIN (\n"
            "        SELECT {user_col}, min(toDate({time_col})) AS first_day\n"
            "        FROM   {table}\n"
            "        GROUP BY {user_col}\n"
            "    ) AS first_seen USING ({user_col})\n"
            "    WHERE dateDiff('day', first_seen.first_day, toDate(e.{time_col}))\n"
            "          BETWEEN 0 AND {cohort_days}\n"
            ")\n"
            "GROUP BY cohort_week, day_offset\n"
            "ORDER BY cohort_week, day_offset"
        ),
    },
    "P4": {
        "name": "top_events",
        "description": "Top-K most frequent events with share percentage",
        "params": {
            "table":     "Events table name",
            "event_col": "Event name column",
            "k":         "Number of top events to return (default 20)",
        },
        "sql": (
            "SELECT\n"
            "    {event_col}                                              AS event_name,\n"
            "    count()                                                  AS event_count,\n"
            "    round(count() * 100.0 / sum(count()) OVER (), 2)        AS pct_total\n"
            "FROM {table}\n"
            "GROUP BY {event_col}\n"
            "ORDER BY event_count DESC\n"
            "LIMIT {k}"
        ),
    },
}


# --------------------------------------------------------------------------- #
#  Extended tool definitions                                                   #
# --------------------------------------------------------------------------- #

CH_TOOL_DEFINITIONS: List[Dict] = TOOL_DEFINITIONS + [
    {
        "name": "explain_query",
        "description": (
            "Run EXPLAIN PIPELINE on a ClickHouse query BEFORE executing it. "
            "Use as a mandatory preflight check for any complex or heavy query. "
            "Validates the execution plan and detects full-table scans."
        ),
        "params": {
            "query": "SQL query to explain (SELECT only; no FORMAT clause)",
        },
        "required": ["query"],
    },
    {
        "name": "run_funnel",
        "description": (
            "Run conversion funnel analysis using ClickHouse windowFunnel(). "
            "Computes how many users complete each step in an ordered event sequence "
            "within a time window. Returns step-by-step drop-off."
        ),
        "params": {
            "table":       "Table with event data",
            "user_col":    "User identifier column (e.g. user_id)",
            "time_col":    "DateTime timestamp column",
            "event_col":   "Column holding the event name/type",
            "steps":       "Ordered list of event names (2-8 steps)",
            "window_secs": "Conversion window in seconds (default 86400)",
            "filters":     "Optional WHERE conditions (without the WHERE keyword)",
        },
        "required": ["table", "user_col", "time_col", "event_col", "steps"],
    },
    {
        "name": "run_time_series",
        "description": (
            "Query a metric over time with automatic gap-filling using WITH FILL. "
            "Guarantees every time slot appears in the result even with zero data."
        ),
        "params": {
            "table":      "Table name",
            "time_col":   "Date or DateTime column",
            "metric_col": "Numeric column to aggregate (optional, uses count() if omitted)",
            "agg_func":   "'sum' | 'count' | 'avg' | 'max' | 'min' (default 'count')",
            "interval":   "'day' | 'hour' | 'week' | 'month' (default 'day')",
            "start_date": "Start boundary YYYY-MM-DD (optional)",
            "end_date":   "End boundary YYYY-MM-DD (optional)",
            "filters":    "Optional WHERE conditions (without WHERE keyword)",
        },
        "required": ["table", "time_col"],
    },
    {
        "name": "run_topk",
        "description": (
            "Return the K most frequent values in a column using ClickHouse topK(). "
            "Uses the Space-Saving algorithm — O(K) memory, blazing fast on billions of rows."
        ),
        "params": {
            "table":   "Table name",
            "column":  "Column to rank",
            "k":       "Number of top values (default 10)",
            "filters": "Optional WHERE conditions (without WHERE keyword)",
        },
        "required": ["table", "column"],
    },
    {
        "name": "run_hll_count",
        "description": (
            "Compute approximate distinct count using uniqHLL12(). "
            "~2% error rate vs COUNT(DISTINCT), but 100x faster on large datasets. "
            "Ideal for cardinality estimation on billion-row tables."
        ),
        "params": {
            "table":        "Table name",
            "column":       "Column to count distinct values for",
            "group_by_col": "Optional column to group by (returns per-group HLL counts)",
            "filters":      "Optional WHERE conditions (without WHERE keyword)",
        },
        "required": ["table", "column"],
    },
    {
        "name": "create_ch_table",
        "description": (
            "Create a ClickHouse table with the correct MergeTree engine. "
            "Enforces the agent_ prefix rule when sandboxing is enabled. "
            "Blocked operations: DROP TABLE, TRUNCATE (use alter_ch_table for column changes)."
        ),
        "params": {
            "table_name":    "Full table name (must start with agent_ when sandboxed)",
            "columns":       "List of {name, type, comment} column definitions",
            "engine":        "'MergeTree' | 'ReplacingMergeTree' | 'SummingMergeTree' | 'AggregatingMergeTree' | 'CollapsingMergeTree' (default 'MergeTree')",
            "order_by":      "List of column names for ORDER BY (primary key index)",
            "partition_by":  "Optional partition expression e.g. 'toYYYYMM(created_at)'",
            "ttl_expr":      "Optional TTL expression e.g. 'created_at + INTERVAL 90 DAY'",
            "if_not_exists": "Emit IF NOT EXISTS clause (default true)",
        },
        "required": ["table_name", "columns", "order_by"],
    },
    {
        "name": "alter_ch_table",
        "description": (
            "Alter a ClickHouse table schema: add, drop, modify, or rename columns, "
            "or add a secondary index. DROP TABLE and TRUNCATE are permanently blocked."
        ),
        "params": {
            "table_name":  "Table name to alter",
            "operation":   "'add_column' | 'drop_column' | 'modify_column' | 'rename_column' | 'add_index'",
            "column_name": "Target column name",
            "column_type": "Column type (required for add_column / modify_column)",
            "new_name":    "New column name (required for rename_column)",
            "after_col":   "Insert new column after this one (optional)",
            "index_expr":  "Index expression (required for add_index)",
            "index_type":  "Index type: 'minmax' | 'set(N)' | 'bloom_filter' (for add_index)",
            "index_granularity": "Index granularity (default 1, for add_index)",
        },
        "required": ["table_name", "operation", "column_name"],
    },
    {
        "name": "write_agent_table",
        "description": (
            "INSERT rows into a ClickHouse table. "
            "The table name MUST start with the agent_ prefix (sandboxing enforced). "
            "Supports batch inserts via JSONEachRow."
        ),
        "params": {
            "table": "Target table name — MUST start with 'agent_'",
            "rows":  "List of row dicts to insert",
        },
        "required": ["table", "rows"],
    },
    {
        "name": "execute_template",
        "description": (
            "Execute a pre-defined parameterized query template. "
            "Built-in: P1=daily_active_users, P2=funnel_conversion, "
            "P3=retention_cohort, P4=top_events. "
            "Custom templates from config are also available."
        ),
        "params": {
            "template_id": "Template ID: 'P1', 'P2', 'P3', 'P4', or custom name",
            "params":      "Dict of substitution values for template placeholders",
            "max_rows":    "Max rows to return (default 1000)",
        },
        "required": ["template_id"],
    },
    {
        "name": "list_templates",
        "description": "List all available query templates with their parameters and descriptions.",
        "params": {},
        "required": [],
    },
    {
        "name": "dag_plan",
        "description": (
            "Decompose a complex business question into an ordered DAG of analysis tasks. "
            "Returns a list of steps, each with a tool to call and its dependencies. "
            "Use this at the start of a complex multi-step analysis."
        ),
        "params": {
            "question": "The business question or analysis objective to decompose",
            "context":  "Available schema context to guide the decomposition (optional)",
        },
        "required": ["question"],
    },
    {
        "name": "nl_to_sql",
        "description": (
            "Translate a natural-language question into optimized ClickHouse SQL "
            "using a semantic layer (business terms, column aliases, calculation rules). "
            "Returns the SQL string ready to run via execute_sql."
        ),
        "params": {
            "question":       "Plain-language question about the data",
            "table_hints":    "Optional list of table names to restrict the translation scope",
            "semantic_layer": "Optional {terms, aliases, rules} dict to override config semantic layer",
        },
        "required": ["question"],
    },
]

CH_TOOL_NAMES = {t["name"] for t in CH_TOOL_DEFINITIONS}


# --------------------------------------------------------------------------- #
#  ClickHouseToolExecutor                                                      #
# --------------------------------------------------------------------------- #

class ClickHouseToolExecutor(ToolExecutor):
    """
    Extends the base ToolExecutor with ClickHouse-native tools.

    Additional init parameters
    --------------------------
    llm_client      : LLMClient — used by dag_plan and nl_to_sql
    templates       : custom template dict merged with BUILTIN_TEMPLATES
    semantic_layer  : {terms, aliases, rules} for nl_to_sql
    table_prefix    : enforced prefix for write_agent_table (default 'agent_')
    allow_ddl       : whether create_ch_table / alter_ch_table are enabled
    ddl_protected   : set of blocked DDL keywords (default DROP, TRUNCATE)
    """

    def __init__(
        self,
        db_manager:     "DBManager",
        memory:         "MemoryManager",
        llm_client:     Optional["LLMClient"] = None,
        allow_write:    bool = False,
        allow_ddl:      bool = False,
        max_rows:       int = 1000,
        dispatch_callback: Optional[Callable] = None,
        templates:      Optional[Dict[str, Dict]] = None,
        semantic_layer: Optional[Dict] = None,
        table_prefix:   str = "agent_",
        ddl_protected:  Optional[List[str]] = None,
    ):
        super().__init__(
            db_manager=db_manager,
            memory=memory,
            allow_write=allow_write,
            max_rows=max_rows,
            dispatch_callback=dispatch_callback,
        )
        self.llm            = llm_client
        self.allow_ddl      = allow_ddl
        self.table_prefix   = table_prefix
        self.ddl_protected  = set(ddl_protected or ["DROP", "TRUNCATE"])
        self.semantic_layer = semantic_layer or {}

        # Merge built-in templates with custom templates from config
        self._templates: Dict[str, Dict] = {**BUILTIN_TEMPLATES}
        if templates:
            self._templates.update(templates)

    # Override execute() to also resolve CH-specific tools
    def execute(self, tool_name: str, params: Dict[str, Any]) -> Any:
        if tool_name not in CH_TOOL_NAMES:
            raise ValueError(f"Unknown tool: {tool_name}")
        method = getattr(self, f"_tool_{tool_name}", None)
        if method is None:
            raise ValueError(f"Tool '{tool_name}' is defined but not implemented")
        return method(**params)

    # ------------------------------------------------------------------ #
    #  explain_query                                                       #
    # ------------------------------------------------------------------ #

    def _tool_explain_query(self, query: str) -> Dict[str, Any]:
        """Run EXPLAIN PIPELINE to validate execution plan before heavy queries."""
        clean = query.rstrip(";").strip()
        # Use EXPLAIN PIPELINE for a full pipeline description
        explain_sql = f"EXPLAIN PIPELINE {clean}"
        try:
            rows = self.db.query(explain_sql, db="clickhouse",
                                 max_rows=200, allow_write=False, use_cache=False)
            plan_lines = [r.get("explain", r.get("ExplainPipeline", str(r))) for r in rows]
            plan_text  = "\n".join(plan_lines)
        except Exception as e:
            # Some CH versions use different column names; fall back to EXPLAIN
            try:
                explain_sql = f"EXPLAIN {clean}"
                rows = self.db.query(explain_sql, db="clickhouse",
                                     max_rows=200, allow_write=False, use_cache=False)
                plan_lines = [str(r) for r in rows]
                plan_text  = "\n".join(plan_lines)
            except Exception as e2:
                return {"status": "error", "error": str(e2), "original_error": str(e)}

        # Heuristic: warn if full scan is detected
        warnings = []
        if "ReadFromMergeTree" in plan_text and "WHERE" not in clean.upper():
            warnings.append("No WHERE clause — potential full table scan.")
        if plan_text.count("ReadFromMergeTree") > 3:
            warnings.append("Multiple ReadFromMergeTree stages detected — consider a JOIN optimisation.")

        return {
            "status":   "ok",
            "plan":     plan_text,
            "warnings": warnings,
        }

    # ------------------------------------------------------------------ #
    #  run_funnel                                                          #
    # ------------------------------------------------------------------ #

    def _tool_run_funnel(
        self,
        table:       str,
        user_col:    str,
        time_col:    str,
        event_col:   str,
        steps:       List[str],
        window_secs: int = 86400,
        filters:     Optional[str] = None,
    ) -> Dict[str, Any]:
        if len(steps) < 2:
            raise ValueError("Funnel requires at least 2 steps.")
        if len(steps) > 8:
            raise ValueError("ClickHouse windowFunnel supports at most 8 steps.")

        # Build step conditions
        step_conditions = ",\n            ".join(
            f"{event_col} = '{s}'" for s in steps
        )
        steps_in = ", ".join(f"'{s}'" for s in steps)

        where_clause = f"WHERE {event_col} IN ({steps_in})"
        if filters:
            where_clause += f" AND ({filters})"

        sql = f"""
SELECT
    level,
    count()                                                   AS users,
    round(count() * 100.0 / sum(count()) OVER (), 2)         AS pct_of_total,
    round(count() * 100.0 / first_value(count()) OVER (ORDER BY level ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 2) AS pct_of_top
FROM (
    SELECT
        {user_col},
        windowFunnel({window_secs})(
            {time_col},
            {step_conditions}
        ) AS level
    FROM {table}
    {where_clause}
    GROUP BY {user_col}
)
WHERE level > 0
GROUP BY level
ORDER BY level ASC
""".strip()

        rows = self.db.query(sql, db="clickhouse", max_rows=len(steps) + 1,
                             allow_write=False, use_cache=False)
        return {
            "funnel_steps": steps,
            "window_secs":  window_secs,
            "results":      rows,
            "sql":          sql,
        }

    # ------------------------------------------------------------------ #
    #  run_time_series                                                     #
    # ------------------------------------------------------------------ #

    def _tool_run_time_series(
        self,
        table:      str,
        time_col:   str,
        metric_col: Optional[str] = None,
        agg_func:   str = "count",
        interval:   str = "day",
        start_date: Optional[str] = None,
        end_date:   Optional[str] = None,
        filters:    Optional[str] = None,
    ) -> List[Dict]:
        _allowed_agg = {"sum", "count", "avg", "max", "min"}
        _allowed_int = {"day", "hour", "week", "month"}
        if agg_func not in _allowed_agg:
            raise ValueError(f"agg_func must be one of {_allowed_agg}")
        if interval not in _allowed_int:
            raise ValueError(f"interval must be one of {_allowed_int}")

        # Build truncation function
        trunc_fn = {
            "day":   f"toDate({time_col})",
            "hour":  f"toStartOfHour({time_col})",
            "week":  f"toMonday({time_col})",
            "month": f"toStartOfMonth({time_col})",
        }[interval]

        # Build metric expression
        if agg_func == "count":
            metric_expr = "count()"
        else:
            if not metric_col:
                raise ValueError(f"metric_col required for agg_func='{agg_func}'")
            metric_expr = f"{agg_func}({metric_col})"

        # WHERE clause
        conditions = []
        if start_date:
            conditions.append(f"toDate({time_col}) >= '{start_date}'")
        if end_date:
            conditions.append(f"toDate({time_col}) <  '{end_date}'")
        if filters:
            conditions.append(f"({filters})")
        where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        # WITH FILL step
        fill_step = {
            "day":   "toIntervalDay(1)",
            "hour":  "toIntervalHour(1)",
            "week":  "toIntervalWeek(1)",
            "month": "toIntervalMonth(1)",
        }[interval]

        fill_clause = ""
        if start_date and end_date:
            fill_clause = (
                f"WITH FILL\n"
                f"    FROM toDate('{start_date}')\n"
                f"    TO   toDate('{end_date}')\n"
                f"    STEP {fill_step}"
            )

        sql = f"""
SELECT
    {trunc_fn} AS time_bucket,
    {metric_expr} AS metric
FROM {table}
{where_clause}
GROUP BY time_bucket
ORDER BY time_bucket ASC
{fill_clause}
""".strip()

        return self.db.query(sql, db="clickhouse", max_rows=self.max_rows,
                             allow_write=False, use_cache=False)

    # ------------------------------------------------------------------ #
    #  run_topk                                                            #
    # ------------------------------------------------------------------ #

    def _tool_run_topk(
        self,
        table:   str,
        column:  str,
        k:       int = 10,
        filters: Optional[str] = None,
    ) -> Dict[str, Any]:
        where_clause = f"WHERE {filters}" if filters else ""
        # Use topKArray combinator: returns the top-K values
        sql = f"""
SELECT arrayJoin(topK({k})({column})) AS top_value,
       countIf({column} = top_value)  AS approx_count
FROM {table}
{where_clause}
""".strip()
        # Simpler and more reliable: standard GROUP BY with LIMIT
        sql_fallback = f"""
SELECT
    {column}          AS value,
    count()           AS cnt,
    round(count() * 100.0 / sum(count()) OVER (), 2) AS pct
FROM {table}
{where_clause}
GROUP BY {column}
ORDER BY cnt DESC
LIMIT {k}
""".strip()
        try:
            rows = self.db.query(sql, db="clickhouse", max_rows=k,
                                 allow_write=False, use_cache=False)
        except Exception:
            rows = self.db.query(sql_fallback, db="clickhouse", max_rows=k,
                                 allow_write=False, use_cache=False)
        return {"column": column, "k": k, "results": rows}

    # ------------------------------------------------------------------ #
    #  run_hll_count                                                       #
    # ------------------------------------------------------------------ #

    def _tool_run_hll_count(
        self,
        table:        str,
        column:       str,
        group_by_col: Optional[str] = None,
        filters:      Optional[str] = None,
    ) -> Any:
        where_clause = f"WHERE {filters}" if filters else ""
        if group_by_col:
            sql = f"""
SELECT
    {group_by_col}             AS group_key,
    uniqHLL12({column})        AS approx_distinct
FROM {table}
{where_clause}
GROUP BY {group_by_col}
ORDER BY approx_distinct DESC
LIMIT 200
""".strip()
            return self.db.query(sql, db="clickhouse", max_rows=200,
                                 allow_write=False, use_cache=False)
        else:
            sql = f"""
SELECT uniqHLL12({column}) AS approx_distinct
FROM {table}
{where_clause}
""".strip()
            rows = self.db.query(sql, db="clickhouse", max_rows=1,
                                 allow_write=False, use_cache=False)
            return rows[0] if rows else {"approx_distinct": 0}

    # ------------------------------------------------------------------ #
    #  create_ch_table                                                     #
    # ------------------------------------------------------------------ #

    def _tool_create_ch_table(
        self,
        table_name:    str,
        columns:       List[Dict],
        order_by:      List[str],
        engine:        str = "MergeTree",
        partition_by:  Optional[str] = None,
        ttl_expr:      Optional[str] = None,
        if_not_exists: bool = True,
    ) -> str:
        if not self.allow_ddl:
            raise PermissionError(
                "DDL operations are disabled. Set clickhouse_agents.allow_ddl=true in config."
            )
        _allowed_engines = {
            "MergeTree", "ReplacingMergeTree", "SummingMergeTree",
            "AggregatingMergeTree", "CollapsingMergeTree", "VersionedCollapsingMergeTree",
        }
        if engine not in _allowed_engines:
            raise ValueError(f"Engine must be one of: {_allowed_engines}")

        # Enforce prefix when sandboxed
        if self.table_prefix and not table_name.startswith(self.table_prefix):
            raise PermissionError(
                f"Table name '{table_name}' must start with prefix '{self.table_prefix}'. "
                "This enforces the agent sandboxing policy."
            )

        # Build column definitions
        col_defs = []
        for col in columns:
            name    = col["name"]
            ch_type = col["type"]
            comment = col.get("comment", "")
            defn    = f"    `{name}` {ch_type}"
            if comment:
                defn += f" COMMENT '{comment}'"
            col_defs.append(defn)

        ine = "IF NOT EXISTS " if if_not_exists else ""
        order_str = ", ".join(f"`{c}`" for c in order_by)

        parts = [
            f"CREATE TABLE {ine}`{table_name}`",
            "(",
            ",\n".join(col_defs),
            ")",
            f"ENGINE = {engine}()",
        ]
        if partition_by:
            parts.append(f"PARTITION BY {partition_by}")
        parts.append(f"ORDER BY ({order_str})")
        if ttl_expr:
            parts.append(f"TTL {ttl_expr}")

        ddl = "\n".join(parts)
        return self.db.execute_write(ddl, db="clickhouse")

    # ------------------------------------------------------------------ #
    #  alter_ch_table                                                      #
    # ------------------------------------------------------------------ #

    def _tool_alter_ch_table(
        self,
        table_name:        str,
        operation:         str,
        column_name:       str,
        column_type:       Optional[str] = None,
        new_name:          Optional[str] = None,
        after_col:         Optional[str] = None,
        index_expr:        Optional[str] = None,
        index_type:        Optional[str] = None,
        index_granularity: int = 1,
    ) -> str:
        if not self.allow_ddl:
            raise PermissionError(
                "DDL operations are disabled. Set clickhouse_agents.allow_ddl=true in config."
            )
        _ops = {"add_column", "drop_column", "modify_column", "rename_column", "add_index"}
        if operation not in _ops:
            raise ValueError(f"operation must be one of {_ops}")

        if operation == "add_column":
            if not column_type:
                raise ValueError("column_type required for add_column")
            after = f" AFTER `{after_col}`" if after_col else ""
            ddl = f"ALTER TABLE `{table_name}` ADD COLUMN `{column_name}` {column_type}{after}"

        elif operation == "drop_column":
            ddl = f"ALTER TABLE `{table_name}` DROP COLUMN `{column_name}`"

        elif operation == "modify_column":
            if not column_type:
                raise ValueError("column_type required for modify_column")
            ddl = f"ALTER TABLE `{table_name}` MODIFY COLUMN `{column_name}` {column_type}"

        elif operation == "rename_column":
            if not new_name:
                raise ValueError("new_name required for rename_column")
            ddl = f"ALTER TABLE `{table_name}` RENAME COLUMN `{column_name}` TO `{new_name}`"

        elif operation == "add_index":
            if not index_expr or not index_type:
                raise ValueError("index_expr and index_type required for add_index")
            idx_name = f"idx_{column_name}"
            ddl = (
                f"ALTER TABLE `{table_name}` "
                f"ADD INDEX {idx_name} ({index_expr}) "
                f"TYPE {index_type} GRANULARITY {index_granularity}"
            )

        return self.db.execute_write(ddl, db="clickhouse")

    # ------------------------------------------------------------------ #
    #  write_agent_table                                                   #
    # ------------------------------------------------------------------ #

    def _tool_write_agent_table(self, table: str, rows: List[Dict]) -> Dict[str, Any]:
        if not self.allow_write:
            raise PermissionError(
                "Write queries are disabled. Set security.allow_write_queries=true in config."
            )
        if not table.startswith(self.table_prefix):
            raise PermissionError(
                f"write_agent_table is sandboxed: table '{table}' must start with "
                f"'{self.table_prefix}'. Choose a different table name or use execute_write_sql."
            )
        if not rows:
            return {"inserted": 0, "status": "no-op"}

        # Build JSONEachRow payload
        lines   = [json.dumps(row, ensure_ascii=False, default=str) for row in rows]
        payload = "\n".join(lines)
        sql     = f"INSERT INTO `{table}` FORMAT JSONEachRow\n{payload}"
        self.db.execute_write(sql, db="clickhouse")
        return {"table": table, "inserted": len(rows), "status": "ok"}

    # ------------------------------------------------------------------ #
    #  execute_template                                                    #
    # ------------------------------------------------------------------ #

    def _tool_execute_template(
        self,
        template_id: str,
        params:      Optional[Dict[str, Any]] = None,
        max_rows:    int = 1000,
    ) -> Dict[str, Any]:
        tmpl = self._templates.get(template_id)
        if tmpl is None:
            available = list(self._templates.keys())
            raise ValueError(
                f"Template '{template_id}' not found. Available: {available}"
            )

        params = params or {}
        sql_raw = tmpl["sql"]

        # Substitute placeholders {param_name}
        missing = []
        for key in re.findall(r"\{(\w+)\}", sql_raw):
            if key not in params:
                # Use defaults from template definition if available
                if "defaults" in tmpl and key in tmpl["defaults"]:
                    params[key] = tmpl["defaults"][key]
                else:
                    missing.append(key)
        if missing:
            raise ValueError(
                f"Template '{template_id}' missing parameters: {missing}. "
                f"Required params: {list(tmpl.get('params', {}).keys())}"
            )

        sql = sql_raw.format(**params)
        rows = self.db.query(sql, db="clickhouse", max_rows=max_rows,
                             allow_write=False, use_cache=False)
        return {
            "template_id":   template_id,
            "template_name": tmpl.get("name", template_id),
            "params_used":   params,
            "row_count":     len(rows),
            "results":       rows,
            "sql":           sql,
        }

    # ------------------------------------------------------------------ #
    #  list_templates                                                      #
    # ------------------------------------------------------------------ #

    def _tool_list_templates(self) -> List[Dict[str, Any]]:
        return [
            {
                "id":          tid,
                "name":        t.get("name", tid),
                "description": t.get("description", ""),
                "params":      t.get("params", {}),
            }
            for tid, t in self._templates.items()
        ]

    # ------------------------------------------------------------------ #
    #  dag_plan                                                            #
    # ------------------------------------------------------------------ #

    def _tool_dag_plan(
        self,
        question: str,
        context:  Optional[str] = None,
    ) -> Dict[str, Any]:
        if self.llm is None:
            # Return a default 3-step DAG without LLM
            return {
                "question": question,
                "dag": [
                    {"step": 1, "task": "Explore schema",           "tool": "get_schema",    "depends_on": []},
                    {"step": 2, "task": "Sample relevant tables",   "tool": "get_sample",    "depends_on": [1]},
                    {"step": 3, "task": "Compute targeted metrics", "tool": "execute_sql",   "depends_on": [2]},
                    {"step": 4, "task": "Synthesize findings",      "tool": "final_answer",  "depends_on": [3]},
                ],
                "note": "Default DAG (no LLM available for planning).",
            }

        ctx_snippet = f"\n\nAvailable context:\n{context[:2000]}" if context else ""
        prompt = (
            "You are a ClickHouse data analyst planning a multi-step analysis.\n"
            "Decompose the business question below into an ordered DAG of analysis tasks.\n\n"
            f"Question: {question}{ctx_snippet}\n\n"
            "Available tools (use exact names):\n"
            "  get_schema, get_sample, execute_sql, explain_query,\n"
            "  run_funnel, run_time_series, run_topk, run_hll_count,\n"
            "  compute_stats, detect_nulls, store_finding, think, final_answer\n\n"
            "Reply ONLY with valid JSON:\n"
            "{\n"
            '  "dag": [\n'
            '    {"step": 1, "task": "description", "tool": "tool_name",\n'
            '     "params_hint": {"key": "value"}, "depends_on": []},\n'
            "    ...\n"
            "  ],\n"
            '  "reasoning": "brief explanation"\n'
            "}\n"
            "Rules:\n"
            "- Minimum 3 steps, maximum 8 steps\n"
            "- Last step must always use final_answer\n"
            "- depends_on lists step numbers that must complete first\n"
        )
        try:
            raw      = self.llm.complete([{"role": "user", "content": prompt}])
            decision = self.llm._extract_json(raw)
            dag      = decision.get("dag", []) if decision else []
            reasoning = (decision or {}).get("reasoning", "")
        except Exception as e:
            dag       = []
            reasoning = f"LLM planning failed: {e}"

        if not dag:
            dag = [
                {"step": 1, "task": "Explore schema",         "tool": "get_schema",   "depends_on": []},
                {"step": 2, "task": "Sample data",            "tool": "get_sample",   "depends_on": [1]},
                {"step": 3, "task": "Targeted SQL analysis",  "tool": "execute_sql",  "depends_on": [2]},
                {"step": 4, "task": "Synthesize findings",    "tool": "final_answer", "depends_on": [3]},
            ]

        return {"question": question, "dag": dag, "reasoning": reasoning}

    # ------------------------------------------------------------------ #
    #  nl_to_sql                                                           #
    # ------------------------------------------------------------------ #

    def _tool_nl_to_sql(
        self,
        question:       str,
        table_hints:    Optional[List[str]] = None,
        semantic_layer: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        if self.llm is None:
            raise RuntimeError("nl_to_sql requires an LLM client.")

        # Merge provided semantic layer with instance-level one
        sl = {**self.semantic_layer, **(semantic_layer or {})}

        # Build schema context
        try:
            if table_hints:
                schema_ctx = {}
                full = self.db.get_schema(db="clickhouse")
                for t in table_hints:
                    if t in full:
                        schema_ctx[t] = full[t]
            else:
                schema_ctx = self.db.get_schema(db="clickhouse")
            schema_str = json.dumps(schema_ctx, ensure_ascii=False, indent=2)[:6000]
        except Exception as e:
            schema_str = f"(schema unavailable: {e})"

        sl_str = json.dumps(sl, ensure_ascii=False, indent=2) if sl else "(none)"

        prompt = (
            "You are a senior ClickHouse SQL expert. Translate the business question into "
            "optimized ClickHouse SQL.\n\n"
            "## Database Schema\n"
            f"```json\n{schema_str}\n```\n\n"
            "## Semantic Layer (business terms, aliases, rules)\n"
            f"```json\n{sl_str}\n```\n\n"
            "## ClickHouse Best Practices to Apply\n"
            "- Use uniqHLL12() instead of COUNT(DISTINCT) for large cardinalities\n"
            "- Use topK() for approximate top-N queries\n"
            "- Use windowFunnel() for funnel/sequence analysis\n"
            "- Use WITH FILL for time-series gap filling\n"
            "- Use ASOF JOIN for temporal data alignment\n"
            "- Use -If / -Array / -State combinators where applicable\n"
            "- Always put the SMALLER table on the RIGHT side of JOINs\n"
            "- Use quantileTDigest() for median/percentile calculations\n"
            "- Add LIMIT to prevent accidental full scans\n"
            "- Use dictGet() if a dictionary is referenced in the semantic layer\n\n"
            f"## Question\n{question}\n\n"
            "Reply ONLY with valid JSON:\n"
            "{\n"
            '  "sql": "SELECT ...",\n'
            '  "explanation": "brief explanation of the query logic",\n'
            '  "tables_used": ["table1"],\n'
            '  "ch_features_used": ["uniqHLL12", "topK"]\n'
            "}"
        )

        try:
            raw      = self.llm.complete([{"role": "user", "content": prompt}])
            decision = self.llm._extract_json(raw)
        except Exception as e:
            return {"error": str(e), "sql": None}

        if decision is None:
            return {"error": "LLM returned non-JSON output", "raw": raw, "sql": None}

        return {
            "question":        question,
            "sql":             decision.get("sql"),
            "explanation":     decision.get("explanation", ""),
            "tables_used":     decision.get("tables_used", []),
            "ch_features":     decision.get("ch_features_used", []),
        }
