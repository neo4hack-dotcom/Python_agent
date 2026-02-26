"""
Generic Analyst Agent — clickhouse_generic
==========================================

All-terrain ClickHouse analytical agent with DAG-based task decomposition.

Capabilities
------------
- Decomposes complex business questions into a DAG of analysis steps
- Deep schema exploration via system.columns / system.tables
- Auto-corrects queries on ClickHouse error feedback
- Injects business rules and KPI definitions from the semantic layer
- Escalates subtasks to specialized agents via dispatch_agent
- Persists intermediate findings for multi-step reasoning
"""
from agents.clickhouse.base import ClickHouseBaseAgent
from utils.prompts import CH_GENERIC_MISSION


class ClickHouseGenericAgent(ClickHouseBaseAgent):
    name           = "clickhouse_generic"
    specialization = (
        "All-terrain ClickHouse analyst: decomposes complex business questions "
        "into DAG-driven analysis pipelines with schema-aware query generation."
    )
    mission = CH_GENERIC_MISSION
