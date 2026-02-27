"""
Web Agent — Recherche internet, navigation et extraction d'informations.

Cet agent est autonome : il peut rechercher sur internet, naviguer sur des pages,
extraire du contenu structuré (tableaux, listes), remplir des formulaires et
télécharger des fichiers.

Dépendances :
  - urllib, html.parser (stdlib, toujours disponible)
  - playwright (optionnel, pour formulaires JS et screenshots) :
      pip install playwright && playwright install chromium

Usage :
    agent = WebAgent(llm, db, logger)
    result = agent.run("Recherche les dernières nouveautés sur l'intelligence artificielle")
"""
from typing import Any, Callable, Dict, Optional

from core.llm_client  import LLMClient
from core.db_manager  import DBManager
from core.memory      import MemoryManager
from core.web_tools   import WebToolExecutor, WEB_TOOL_DEFINITIONS
from core.engine      import AgentEngine
from utils.logger     import AgentLogger
from utils.prompts    import WEB_AGENT_MISSION


class WebAgent:
    """
    Agent spécialisé dans la navigation internet et l'extraction d'informations web.

    Capacités :
    - Recherche sur DuckDuckGo (sans clé API)
    - Navigation et lecture de pages web
    - Extraction de données structurées (tableaux, listes, métadonnées)
    - Remplissage et soumission de formulaires HTML (nécessite playwright)
    - Clic sur des éléments de page (nécessite playwright)
    - Capture d'écran de pages (nécessite playwright)
    - Téléchargement de fichiers

    Usage :
        agent = WebAgent(llm, db, logger)
        result = agent.run(
            "Recherche les dernières actualités sur l'IA et résume les 3 premiers résultats"
        )
    """

    name           = "WebAgent"
    specialization = (
        "internet navigation, web search on search engines, "
        "page content extraction, form filling and web data retrieval"
    )
    mission        = WEB_AGENT_MISSION

    def __init__(
        self,
        llm:           LLMClient,
        db:            DBManager,
        logger:        AgentLogger,
        max_steps:     int = 20,
        dispatch_cb             = None,
        step_callback: Optional[Callable] = None,
        timeout:       int = 20,
        results_dir:   str = "./results",
    ):
        self.llm    = llm
        self.db     = db
        self.logger = logger

        self.memory = MemoryManager(
            working_window=6,
            compress_threshold=10,
            agent_name=self.name,
        )

        self.tool_executor = WebToolExecutor(
            memory=self.memory,
            dispatch_callback=dispatch_cb,
            timeout=timeout,
            results_dir=results_dir,
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

        # Injection de la liste d'outils spécifique à l'agent web
        self.engine._ch_tool_defs = WEB_TOOL_DEFINITIONS

    def run(self, task: str, context: str = "") -> Dict[str, Any]:
        """Exécute l'agent web sur une tâche et retourne le résultat."""
        try:
            return self.engine.run(task, initial_context=context)
        finally:
            # Libère les ressources playwright si utilisées
            try:
                self.tool_executor.close()
            except Exception:
                pass

    @classmethod
    def from_config(
        cls,
        llm:    LLMClient,
        db:     DBManager,
        logger: AgentLogger,
        config: dict,
        step_callback: Optional[Callable] = None,
    ) -> "WebAgent":
        """Construit l'agent depuis un dict de configuration."""
        agent_cfg = config.get("agents", {})
        web_cfg   = config.get("web_agent", {})
        return cls(
            llm=llm,
            db=db,
            logger=logger,
            max_steps=int(web_cfg.get("max_steps", agent_cfg.get("max_steps", 20))),
            timeout=int(web_cfg.get("timeout", 20)),
            results_dir=agent_cfg.get("result_dir", "./results"),
            step_callback=step_callback,
        )
