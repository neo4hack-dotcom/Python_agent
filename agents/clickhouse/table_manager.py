"""
Table Manager Agent — clickhouse_table_manager
==============================================

Autonomous ClickHouse DDL administrator.

Capabilities
------------
- Creates tables with proper MergeTree engine selection
  (MergeTree, ReplacingMergeTree, SummingMergeTree, AggregatingMergeTree, …)
- Recommends ORDER BY / PARTITION BY strategies based on query patterns
- Adds secondary indexes (minmax, set, bloom_filter) as needed
- Alters table schemas (add/drop/modify/rename columns)
- Sets TTL expressions for data lifecycle management
- Auto-discovers existing schema via system.columns / system.tables
- BLOCKED: DROP TABLE, TRUNCATE — these require explicit human approval

Guardrails
----------
- allow_ddl must be true in config to execute DDL
- table_prefix enforcement is active by default
- No destructive DDL without explicit confirmation in the task
"""
from agents.clickhouse.base import ClickHouseBaseAgent
from utils.prompts import CH_TABLE_MANAGER_MISSION


class ClickHouseTableManagerAgent(ClickHouseBaseAgent):
    name           = "clickhouse_table_manager"
    specialization = (
        "ClickHouse DDL administrator: designs, creates, and evolves table schemas "
        "with MergeTree engine selection, partition strategies, and lifecycle management."
    )
    mission = CH_TABLE_MANAGER_MISSION

    def __init__(self, *args, **kwargs):
        # Table manager always needs DDL access
        kwargs.setdefault("allow_ddl", True)
        super().__init__(*args, **kwargs)
