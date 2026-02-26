"""
Text File Agent — Création, lecture et modification de fichiers texte.

Cet agent est autonome : il peut créer, lire, modifier, rechercher et
remplacer du contenu dans des fichiers texte de tout type (.txt, .csv, .log, .json…).

Aucune dépendance externe requise (stdlib Python uniquement).
"""
from typing import Any, Callable, Dict, Optional

from core.llm_client  import LLMClient
from core.db_manager  import DBManager
from core.memory      import MemoryManager
from core.text_tools  import TextToolExecutor, TEXT_TOOL_DEFINITIONS
from core.engine      import AgentEngine
from utils.logger     import AgentLogger
from utils.prompts    import TEXT_AGENT_MISSION


class TextFileAgent:
    """
    Agent spécialisé dans la manipulation de fichiers texte.

    Capacités :
    - Créer, lire, écraser et compléter des fichiers texte
    - Rechercher des mots ou expressions (regex) dans un fichier
    - Remplacer des occurrences dans un fichier
    - Lister les fichiers texte d'un répertoire (avec filtre extension)
    - Obtenir des statistiques de fichiers (lignes, mots, caractères)

    Formats supportés : .txt, .csv, .log, .json, .xml, .yaml, .ini, .md…
    Encodages : UTF-8 par défaut, configurable.

    Usage :
        agent = TextFileAgent(llm, db, logger)
        result = agent.run("Crée un fichier notes.txt avec un résumé de la réunion")
    """

    name           = "TextFileAgent"
    specialization = (
        "creating, reading and editing text files "
        "(.txt, .csv, .log, .json, .xml, etc.) with stdlib only"
    )
    mission        = TEXT_AGENT_MISSION

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

        self.tool_executor = TextToolExecutor(
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

        # Inject the text-file-specific tool list
        self.engine._ch_tool_defs = TEXT_TOOL_DEFINITIONS

    def run(self, task: str, context: str = "") -> Dict[str, Any]:
        """Run the text file agent on a task and return the result dict."""
        return self.engine.run(task, initial_context=context)
