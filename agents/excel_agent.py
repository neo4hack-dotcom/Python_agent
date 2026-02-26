"""
Excel Agent — Création, lecture et modification de fichiers Excel (.xlsx).

Cet agent est autonome : il peut créer des classeurs, ajouter des feuilles,
écrire des données, insérer des formules et appliquer des formats professionnels.

Dépendance : openpyxl (pip install openpyxl)
"""
from typing import Any, Callable, Dict, Optional

from core.llm_client   import LLMClient
from core.db_manager   import DBManager
from core.memory       import MemoryManager
from core.excel_tools  import ExcelToolExecutor, EXCEL_TOOL_DEFINITIONS
from core.engine       import AgentEngine
from utils.logger      import AgentLogger
from utils.prompts     import EXCEL_AGENT_MISSION


class ExcelAgent:
    """
    Agent spécialisé dans la manipulation de fichiers Excel (.xlsx).

    Capacités :
    - Créer, ouvrir, lire et modifier des classeurs Excel
    - Écrire des données (cellules individuelles ou lignes en masse)
    - Insérer des formules Excel (SUM, AVERAGE, IF, VLOOKUP…)
    - Formater des plages (gras, couleurs, alignement, largeur automatique)
    - Gérer les feuilles (ajout, suppression, listage)
    - Enregistrer les classeurs sur disque

    Usage :
        agent = ExcelAgent(llm, db, logger)
        result = agent.run("Crée un fichier Excel avec les ventes Q1 et des totaux")
    """

    name           = "ExcelAgent"
    specialization = "creating, reading and editing Excel workbooks (.xlsx) with openpyxl"
    mission        = EXCEL_AGENT_MISSION

    def __init__(
        self,
        llm:           LLMClient,
        db:            DBManager,
        logger:        AgentLogger,
        max_steps:     int = 25,
        dispatch_cb             = None,
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

        self.tool_executor = ExcelToolExecutor(
            memory=self.memory,
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

        # Inject the Excel-specific tool list so the engine uses it
        self.engine._ch_tool_defs = EXCEL_TOOL_DEFINITIONS

    def run(self, task: str, context: str = "") -> Dict[str, Any]:
        """Run the Excel agent on a task and return the result dict."""
        return self.engine.run(task, initial_context=context)
