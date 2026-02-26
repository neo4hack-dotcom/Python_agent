"""
ClickHouse Agent Suite
======================

Six specialized agents for ClickHouse analytics and administration:

  sql_analyst            — Senior ClickHouse SQL expert; generates optimized queries
  clickhouse_generic     — All-terrain analyst with DAG-based task decomposition
  clickhouse_table_manager — DDL administrator with guardrails (CRU, no D)
  clickhouse_writer      — Sandboxed DML writer (agent_ prefix enforced)
  clickhouse_specific    — Parameterized template executor (P1→P4)
  text_to_sql_translator — Natural-language → ClickHouse SQL via semantic layer
"""
from agents.clickhouse.sql_analyst      import SQLAnalystAgent
from agents.clickhouse.generic          import ClickHouseGenericAgent
from agents.clickhouse.table_manager    import ClickHouseTableManagerAgent
from agents.clickhouse.writer           import ClickHouseWriterAgent
from agents.clickhouse.specific         import ClickHouseSpecificAgent
from agents.clickhouse.text_to_sql      import TextToSQLAgent

__all__ = [
    "SQLAnalystAgent",
    "ClickHouseGenericAgent",
    "ClickHouseTableManagerAgent",
    "ClickHouseWriterAgent",
    "ClickHouseSpecificAgent",
    "TextToSQLAgent",
]

# Canonical name → class mapping used by main.py / graph.py
AGENT_REGISTRY = {
    "sql_analyst":              SQLAnalystAgent,
    "clickhouse_generic":       ClickHouseGenericAgent,
    "clickhouse_table_manager": ClickHouseTableManagerAgent,
    "clickhouse_writer":        ClickHouseWriterAgent,
    "clickhouse_specific":      ClickHouseSpecificAgent,
    "text_to_sql_translator":   TextToSQLAgent,
}
