"""
Base Agent — classe de base pour tous les agents spécialisés.
Chaque agent instancie son propre Engine + MemoryManager.
"""
from typing import Any, Callable, Dict, Optional

from core.llm_client  import LLMClient
from core.db_manager  import DBManager
from core.memory      import MemoryManager
from core.tools       import ToolExecutor
from core.engine      import AgentEngine
from utils.logger     import AgentLogger


class BaseAgent:
    """
    Wraps AgentEngine with agent-specific configuration.
    Subclasses override: name, specialization, mission.
    """

    name:           str = "BaseAgent"
    specialization: str = "general data tasks"
    mission:        str = ""

    def __init__(
        self,
        llm:           LLMClient,
        db:            DBManager,
        logger:        AgentLogger,
        max_steps:     int = 20,
        allow_write:   bool = False,
        max_rows:      int = 1000,
        dispatch_cb    = None,     # callable for dispatch_agent tool
        step_callback: Optional[Callable] = None,
    ):
        self.llm    = llm
        self.db     = db
        self.logger = logger

        self.memory = MemoryManager(
            working_window=6,
            compress_threshold=10,
            agent_name=self.name,
        )

        self.tool_executor = ToolExecutor(
            db_manager=db,
            memory=self.memory,
            allow_write=allow_write,
            max_rows=max_rows,
            dispatch_callback=dispatch_cb,
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
            step_callback=step_callback,
        )

    def run(self, task: str, context: str = "") -> Dict[str, Any]:
        """Run the agent on a task and return the result dict."""
        return self.engine.run(task, initial_context=context)


class CustomAgent(BaseAgent):
    """
    Dynamically configured agent based on user-defined parameters.
    Allows creating agents with custom name, specialization and mission
    without subclassing.
    """

    def __init__(
        self,
        llm:            LLMClient,
        db:             DBManager,
        logger:         AgentLogger,
        name:           str = "CustomAgent",
        specialization: str = "custom data analysis",
        mission:        str = "",
        max_steps:      int = 20,
        allow_write:    bool = False,
        max_rows:       int = 1000,
        dispatch_cb     = None,
        step_callback:  Optional[Callable] = None,
    ):
        # Set instance attributes first — they shadow the class-level defaults
        self.name           = name
        self.specialization = specialization
        self.mission        = mission
        super().__init__(
            llm=llm, db=db, logger=logger,
            max_steps=max_steps, allow_write=allow_write,
            max_rows=max_rows, dispatch_cb=dispatch_cb,
            step_callback=step_callback,
        )
