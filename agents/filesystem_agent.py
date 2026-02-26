"""
FileSystem Agent — Navigation, recherche et ingestion de fichiers Windows/Linux.

Cet agent est polyvalent et autonome. Il peut :
  — Explorer des arborescences de répertoires (Windows et Linux)
  — Rechercher des fichiers par nom (glob) ou par contenu (mot-clé / regex)
  — Lire, copier, déplacer et organiser des fichiers
  — Ouvrir des fichiers avec l'application OS par défaut
  — Ingérer des fichiers (CSV, JSON, TXT) dans ClickHouse automatiquement
  — Scanner plusieurs répertoires en lot avec filtrage par mot-clé

Exemple de pipeline complet :
  "Ouvre tous les fichiers des 3 sous-répertoires, cherche ceux qui contiennent
   le mot 'facture', puis intègre-les dans la table ClickHouse 'agent_factures'"
  → L'agent utilise ingest_directory_to_clickhouse avec keyword_filter='facture'

Aucune dépendance externe requise (stdlib Python uniquement).
"""
from typing import Any, Callable, Dict, Optional

from core.llm_client        import LLMClient
from core.db_manager        import DBManager
from core.memory            import MemoryManager
from core.filesystem_tools  import FileSystemToolExecutor, FS_TOOL_DEFINITIONS
from core.engine            import AgentEngine
from utils.logger           import AgentLogger
from utils.prompts          import FILESYSTEM_AGENT_MISSION


class FileSystemAgent:
    """
    Agent polyvalent pour la gestion de fichiers et répertoires Windows/Linux.

    Capacités :
    - Navigation : list_directory, list_all_recursive, get_file_info
    - Recherche nom : find_files (glob sur arborescence)
    - Recherche contenu : search_content_in_files (cross-directory)
    - Opérations : copy_path, move_path, delete_path, create_directory
    - Lecture : read_file_content (txt, csv, json, log…)
    - Ouverture OS : open_file_with_app
    - Ingestion CH : ingest_file_to_clickhouse, ingest_directory_to_clickhouse

    Paramètres :
        allow_delete : Autoriser les opérations de suppression (défaut: False)

    Usage :
        agent = FileSystemAgent(llm, db, logger)
        result = agent.run(
            "Scanne C:/exports/, cherche les CSV contenant 'client', "
            "insère-les dans la table ClickHouse 'agent_clients'"
        )
    """

    name           = "FileSystemAgent"
    specialization = (
        "autonomous file system navigation, cross-directory content search, "
        "and ClickHouse data ingestion from CSV/JSON/TXT files"
    )
    mission        = FILESYSTEM_AGENT_MISSION

    def __init__(
        self,
        llm:           LLMClient,
        db:            DBManager,
        logger:        AgentLogger,
        max_steps:     int = 30,
        allow_delete:  bool = False,
        dispatch_cb             = None,
        step_callback: Optional[Callable] = None,
    ):
        self.llm    = llm
        self.db     = db
        self.logger = logger

        self.memory = MemoryManager(
            working_window=8,
            compress_threshold=12,
            agent_name=self.name,
        )

        self.tool_executor = FileSystemToolExecutor(
            db_manager=db,
            memory=self.memory,
            dispatch_callback=dispatch_cb,
            allow_delete=allow_delete,
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

        # Inject the filesystem-specific tool list
        self.engine._ch_tool_defs = FS_TOOL_DEFINITIONS

    def run(self, task: str, context: str = "") -> Dict[str, Any]:
        """Run the filesystem agent on a task and return the result dict."""
        return self.engine.run(task, initial_context=context)

    @classmethod
    def from_config(
        cls,
        llm:    LLMClient,
        db:     DBManager,
        logger: AgentLogger,
        config: dict,
        step_callback: Optional[Callable] = None,
    ) -> "FileSystemAgent":
        """Construct the agent from a config dict."""
        agent_cfg = config.get("agents", {})
        sec_cfg   = config.get("security", {})
        return cls(
            llm=llm,
            db=db,
            logger=logger,
            max_steps=int(agent_cfg.get("max_steps", 30)),
            allow_delete=bool(sec_cfg.get("allow_delete", False)),
            step_callback=step_callback,
        )
