"""
Data Writer Agent — clickhouse_writer
======================================

Secure ClickHouse DML scripter with sandboxed INSERT operations.

Security model
--------------
- All writes are restricted to tables prefixed with `agent_`
  (configurable via clickhouse_agents.table_prefix)
- Uses write_agent_table which enforces the prefix at the tool level,
  making bypass impossible without config change
- Validates data types against existing column definitions before insert
- Supports batch inserts for efficiency
- Never executes UPDATE / DELETE / DROP / TRUNCATE

Capabilities
------------
- Prepares data: type coercion, null handling, deduplication hints
- Inserts rows into agent_* tables using JSONEachRow format
- Describes target table schema before insertion for validation
- Reports insertion results (rows written, errors)
"""
from agents.clickhouse.base import ClickHouseBaseAgent
from utils.prompts import CH_WRITER_MISSION


class ClickHouseWriterAgent(ClickHouseBaseAgent):
    name           = "clickhouse_writer"
    specialization = (
        "Secure ClickHouse DML writer: validates and inserts data exclusively "
        "into sandboxed agent_* tables using batch JSONEachRow format."
    )
    mission = CH_WRITER_MISSION

    def __init__(self, *args, **kwargs):
        # Writer always needs write access
        kwargs.setdefault("allow_write", True)
        super().__init__(*args, **kwargs)
