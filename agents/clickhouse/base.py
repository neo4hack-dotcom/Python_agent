"""
ClickHouse Base Agent
=====================

All ClickHouse agents inherit from this class.
It wires the ClickHouseToolExecutor instead of the generic ToolExecutor,
and injects the LLM client for tools that need it (dag_plan, nl_to_sql).
"""
from typing import Any, Dict, List, Optional

from core.llm_client        import LLMClient
from core.db_manager        import DBManager
from core.memory            import MemoryManager
from core.engine            import AgentEngine
from core.clickhouse_tools  import ClickHouseToolExecutor, CH_TOOL_DEFINITIONS
from utils.logger           import AgentLogger


class ClickHouseBaseAgent:
    """
    Base class for all ClickHouse-specialized agents.

    Parameters
    ----------
    llm          : LLMClient
    db           : DBManager
    logger       : AgentLogger
    max_steps    : ReAct step budget
    allow_write  : Allow DML (INSERT) operations
    allow_ddl    : Allow DDL (CREATE TABLE, ALTER TABLE) operations
    max_rows     : Max rows returned by SELECT queries
    table_prefix : Prefix enforced by write_agent_table (default 'agent_')
    templates    : Custom query templates merged with built-ins
    semantic_layer: Business terms / column aliases for nl_to_sql
    dispatch_cb  : Callable for dispatch_agent tool
    """

    name:           str = "ClickHouseBaseAgent"
    specialization: str = "ClickHouse data operations"
    mission:        str = ""

    def __init__(
        self,
        llm:            LLMClient,
        db:             DBManager,
        logger:         AgentLogger,
        max_steps:      int = 20,
        allow_write:    bool = False,
        allow_ddl:      bool = False,
        max_rows:       int = 1000,
        table_prefix:   str = "agent_",
        templates:      Optional[Dict] = None,
        semantic_layer: Optional[Dict] = None,
        dispatch_cb             = None,
    ):
        self.llm    = llm
        self.db     = db
        self.logger = logger

        self.memory = MemoryManager(
            working_window=6,
            compress_threshold=10,
            agent_name=self.name,
        )

        self.tool_executor = ClickHouseToolExecutor(
            db_manager=db,
            memory=self.memory,
            llm_client=llm,
            allow_write=allow_write,
            allow_ddl=allow_ddl,
            max_rows=max_rows,
            dispatch_callback=dispatch_cb,
            templates=templates,
            semantic_layer=semantic_layer or {},
            table_prefix=table_prefix,
        )

        self.engine = AgentEngine(
            llm=llm,
            memory=self.memory,
            tool_executor=self.tool_executor,
            logger=logger,
            agent_name=self.name,
            specialization=self.specialization,
            mission=self.mission,
            max_steps=max_steps,
            reflection_interval=5,
        )

        # Patch engine to expose the CH-extended tool list
        self.engine._ch_tool_defs = CH_TOOL_DEFINITIONS

    def run(self, task: str, context: str = "") -> Dict[str, Any]:
        return self.engine.run(task, initial_context=context)

    @classmethod
    def from_config(
        cls,
        llm:    LLMClient,
        db:     DBManager,
        logger: AgentLogger,
        config: dict,
    ) -> "ClickHouseBaseAgent":
        """Construct the agent from a full config dict."""
        ch_cfg      = config.get("clickhouse_agents", {})
        agent_cfg   = config.get("agents", {})
        sec_cfg     = config.get("security", {})

        return cls(
            llm=llm,
            db=db,
            logger=logger,
            max_steps=int(agent_cfg.get("max_steps", 20)),
            allow_write=bool(sec_cfg.get("allow_write_queries", False)),
            allow_ddl=bool(ch_cfg.get("allow_ddl", False)),
            max_rows=int(sec_cfg.get("max_rows_returned", 1000)),
            table_prefix=ch_cfg.get("table_prefix", "agent_"),
            templates=ch_cfg.get("templates", {}),
            semantic_layer=ch_cfg.get("semantic_layer", {}),
        )
