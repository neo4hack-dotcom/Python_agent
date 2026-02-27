"""
RAG JSON Agent — Recherche par similarité dans une base de connaissances JSON.

Cet agent utilise un index TF-IDF (pur Python, sans dépendance externe) pour
effectuer des recherches sémantiques dans un fichier JSON configuré.

Configuration dans config.json :
  "rag_json": {
    "json_path": "/chemin/vers/ma_base.json",
    "list_key": null,          // clé de liste si le JSON est un dict (optionnel)
    "max_steps": 15
  }

Usage CLI :
  python main.py --agent rag_json "Recherche des articles sur le machine learning"

Usage UI :
  Sélectionner l'agent "RAG JSON" dans l'interface Gradio
"""

from typing import Any, Callable, Dict, Optional

from core.llm_client import LLMClient
from core.db_manager import DBManager
from core.memory import MemoryManager
from core.rag_tools import RAGToolExecutor, RAG_TOOL_DEFINITIONS
from core.engine import AgentEngine
from utils.logger import AgentLogger
from utils.prompts import RAG_JSON_MISSION


class RAGJsonAgent:
    """
    Agent de recherche par similarité dans une base JSON (TF-IDF).

    Outils disponibles :
      rag_search      — Recherche sémantique TF-IDF
      rag_get_by_key  — Correspondance exacte champ/valeur
      rag_list_fields — Structure de la base
      rag_count       — Nombre total d'enregistrements
      rag_sample      — Exemples d'enregistrements
      rag_filter      — Filtrage structuré (=, !=, >, <, contains)
      store_finding   — Mémoire sémantique
      think / final_answer

    Paramètres de configuration (config["rag_json"]) :
      json_path : Chemin absolu vers le fichier JSON (requis)
      list_key  : Clé de liste si le JSON est un dict (optionnel)
      max_steps : Nombre max d'étapes ReAct (défaut: 15)
    """

    name = "RAGJsonAgent"
    specialization = (
        "semantic search and information retrieval from a JSON knowledge base "
        "using TF-IDF similarity scoring"
    )
    mission = RAG_JSON_MISSION

    def __init__(
        self,
        llm: LLMClient,
        db: DBManager,
        logger: AgentLogger,
        json_path: str,
        list_key: Optional[str] = None,
        max_steps: int = 15,
        dispatch_cb=None,
        step_callback: Optional[Callable] = None,
    ):
        """
        Args:
            llm          : Client LLM
            db           : Gestionnaire de bases de données (utilisé pour la compatibilité)
            logger       : Logger
            json_path    : Chemin vers le fichier JSON à indexer
            list_key     : Clé de la liste dans le JSON si c'est un dict (ex: "items")
            max_steps    : Nombre max d'étapes ReAct
            dispatch_cb  : Callback pour le dispatch d'agents (non utilisé ici)
            step_callback: Callback pour le streaming UI
        """
        self.llm = llm
        self.db = db
        self.logger = logger
        self.json_path = json_path

        self.memory = MemoryManager(
            working_window=6,
            compress_threshold=10,
            agent_name=self.name,
        )

        self.tool_executor = RAGToolExecutor(
            json_path=json_path,
            memory=self.memory,
            list_key=list_key,
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

        # Override tool definitions for the engine
        self.engine._ch_tool_defs = RAG_TOOL_DEFINITIONS

    def run(self, task: str, context: str = "") -> Dict[str, Any]:
        """Exécute l'agent RAG sur une tâche de recherche."""
        return self.engine.run(task, initial_context=context)

    @classmethod
    def from_config(
        cls,
        llm: LLMClient,
        db: DBManager,
        logger: AgentLogger,
        config: Dict,
        step_callback: Optional[Callable] = None,
    ) -> "RAGJsonAgent":
        """Construit l'agent depuis la configuration."""
        rag_cfg = config.get("rag_json", {})
        json_path = rag_cfg.get("json_path", "")

        if not json_path:
            raise ValueError(
                "La configuration 'rag_json.json_path' est requise. "
                "Ajoutez-la dans config.json sous la clé 'rag_json'."
            )

        agents_cfg = config.get("agents", {})
        max_steps = int(rag_cfg.get("max_steps", agents_cfg.get("max_steps", 15)))

        return cls(
            llm=llm,
            db=db,
            logger=logger,
            json_path=json_path,
            list_key=rag_cfg.get("list_key"),
            max_steps=max_steps,
            step_callback=step_callback,
        )
