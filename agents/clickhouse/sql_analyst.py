"""
SQL Analyst Agent — sql_analyst
================================

Senior ClickHouse SQL expert.

Capabilities
------------
- Generates fully optimized ClickHouse SQL (dialect-native)
- Mandatory EXPLAIN preflight on complex queries
- Exploits CH-native functions:
    * uniqHLL12 / uniqCombined for HLL cardinality
    * quantileTDigest / quantileTiming for percentiles
    * topK for Space-Saving approximate top-K
    * windowFunnel for funnel/sequence analysis
    * WITH FILL for time-series gap completion
    * ASOF JOIN for temporal alignment
    * -If / -Array / -State / -Merge combinators
    * dictGet() for external dictionary lookups
- Enforces JOIN optimisation (smallest table on the right)
- Auto-corrects failed queries from ClickHouse error messages
"""
from agents.clickhouse.base import ClickHouseBaseAgent
from utils.prompts import CH_SQL_ANALYST_MISSION


class SQLAnalystAgent(ClickHouseBaseAgent):
    name           = "sql_analyst"
    specialization = (
        "Senior ClickHouse SQL Expert: generates performance-optimized ClickHouse SQL "
        "using native functions, combinators, and engine-specific optimisations."
    )
    mission = CH_SQL_ANALYST_MISSION
