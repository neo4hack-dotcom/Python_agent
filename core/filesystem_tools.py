"""
FileSystem Tool Registry — Navigation, recherche et ingestion de fichiers Windows/Linux.

Utilise uniquement la stdlib Python (os, pathlib, shutil, re, csv, json, subprocess).
Compatible Windows et Linux.

Fonctionnalités :
  — Navigation dans des arborescences de répertoires
  — Recherche de fichiers par nom (glob) ou par contenu (regex/mot-clé)
  — Lecture de fichiers texte, CSV, JSON
  — Copie, déplacement, suppression de fichiers/répertoires
  — Ouverture de fichiers avec l'application OS par défaut
  — Ingestion automatique vers ClickHouse (CSV, JSON, TXT)
  — Ingestion de masse depuis plusieurs répertoires avec filtre par mot-clé

Outils disponibles :
  list_directory               — Lister un répertoire
  list_all_recursive           — Lister récursivement un arbre
  create_directory             — Créer un répertoire
  delete_path                  — Supprimer un fichier ou répertoire
  copy_path                    — Copier un fichier ou répertoire
  move_path                    — Déplacer/renommer
  get_file_info                — Métadonnées d'un fichier
  read_file_content            — Lire le contenu d'un fichier
  find_files                   — Chercher des fichiers par nom (glob)
  search_content_in_files      — Chercher un mot/motif dans plusieurs fichiers/répertoires
  open_file_with_app           — Ouvrir avec l'application par défaut
  ingest_file_to_clickhouse    — Ingérer un fichier (CSV/JSON/TXT) dans ClickHouse
  ingest_directory_to_clickhouse — Ingérer un répertoire entier (avec filtre optionnel)
"""
import csv
import datetime
import json
import os
import platform
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from core.db_manager import DBManager
    from core.memory import MemoryManager


# Extensions de fichiers texte considérées lisibles
_TEXT_EXTENSIONS = {
    ".txt", ".csv", ".log", ".json", ".xml", ".yaml", ".yml",
    ".ini", ".cfg", ".conf", ".md", ".rst", ".html", ".htm",
    ".sql", ".py", ".js", ".ts", ".java", ".cs", ".go", ".rb",
    ".tsv", ".ndjson", ".jsonl",
}


# --------------------------------------------------------------------------- #
#  Tool definitions                                                            #
# --------------------------------------------------------------------------- #

FS_TOOL_DEFINITIONS: List[Dict] = [
    {
        "name": "list_directory",
        "description": (
            "List the contents of a directory (files and subdirectories) with metadata "
            "(name, type, size, full path)."
        ),
        "params": {
            "path": "Directory path to list (e.g. 'C:/data' or '/home/user/data')",
            "show_hidden": "Include hidden files and folders (default: false)",
        },
        "required": ["path"],
    },
    {
        "name": "list_all_recursive",
        "description": "Recursively list all files in a directory tree, with optional extension filter and depth limit.",
        "params": {
            "path": "Root directory path",
            "extension_filter": "Only include files with this extension (e.g. '.csv', '.txt') — optional",
            "max_depth": "Maximum recursion depth (default: 5)",
        },
        "required": ["path"],
    },
    {
        "name": "create_directory",
        "description": "Create a new directory and all necessary parent directories.",
        "params": {
            "path": "Directory path to create",
        },
        "required": ["path"],
    },
    {
        "name": "delete_path",
        "description": (
            "Delete a file or directory. "
            "For directories: set recursive=true to delete non-empty directories."
        ),
        "params": {
            "path": "Path to delete",
            "recursive": "Delete directory and all contents recursively (default: false — only empty dirs)",
        },
        "required": ["path"],
    },
    {
        "name": "copy_path",
        "description": "Copy a file or directory to a new location.",
        "params": {
            "source": "Source path",
            "destination": "Destination path",
            "overwrite": "Overwrite destination if it already exists (default: false)",
        },
        "required": ["source", "destination"],
    },
    {
        "name": "move_path",
        "description": "Move or rename a file or directory.",
        "params": {
            "source": "Source path",
            "destination": "Destination path",
        },
        "required": ["source", "destination"],
    },
    {
        "name": "get_file_info",
        "description": "Get metadata for a file or directory: size, creation date, modification date, type, extension.",
        "params": {
            "path": "File or directory path",
        },
        "required": ["path"],
    },
    {
        "name": "read_file_content",
        "description": (
            "Read the content of any text-based file (txt, csv, json, log, xml, etc.). "
            "Returns the content as a string, truncated at max_chars."
        ),
        "params": {
            "path": "File path",
            "encoding": "File encoding (default: 'utf-8')",
            "max_chars": "Maximum characters to return (default: 50000)",
        },
        "required": ["path"],
    },
    {
        "name": "find_files",
        "description": "Find files by name pattern (glob) across a directory tree.",
        "params": {
            "root": "Root directory to start search from",
            "pattern": "Filename glob pattern (e.g. '*.txt', 'report_*.csv', '**/*.json')",
            "recursive": "Search subdirectories (default: true)",
        },
        "required": ["root", "pattern"],
    },
    {
        "name": "search_content_in_files",
        "description": (
            "Search for a word or pattern across ALL files in one or more directories. "
            "Returns each file that contains the pattern with the matching lines. "
            "Use this to find which files contain a specific piece of information."
        ),
        "params": {
            "directories": "List of directory paths to search in",
            "pattern": "Word or pattern to search for (plain string or regex)",
            "regex": "Interpret pattern as a regular expression (default: false)",
            "case_sensitive": "Case-sensitive search (default: false)",
            "file_extensions": "Only search files with these extensions (e.g. ['.txt', '.csv']) — default: all text files",
            "recursive": "Search subdirectories (default: true)",
            "max_results": "Maximum number of matching files to return (default: 50)",
        },
        "required": ["directories", "pattern"],
    },
    {
        "name": "open_file_with_app",
        "description": "Open a file with the default OS application (Explorer/Finder/xdg-open).",
        "params": {
            "path": "File path to open",
        },
        "required": ["path"],
    },
    {
        "name": "ingest_file_to_clickhouse",
        "description": (
            "Read a file (CSV, JSON, or TXT) and insert its content into a ClickHouse table. "
            "The table is created automatically if it does not exist. "
            "CSV: uses first row as column headers. "
            "JSON: expects a JSON array of objects. "
            "TXT: each line becomes a row with columns 'line_number' and 'content'."
        ),
        "params": {
            "file_path": "Path to the source file",
            "table_name": "ClickHouse target table name",
            "file_format": "File format: 'csv', 'json', 'txt' (auto-detected from extension if omitted)",
            "delimiter": "CSV delimiter character (default: ',')",
            "encoding": "File encoding (default: 'utf-8')",
            "create_table_if_missing": "Automatically create the table if it does not exist (default: true)",
            "batch_size": "Number of rows per INSERT batch (default: 5000)",
        },
        "required": ["file_path", "table_name"],
    },
    {
        "name": "ingest_directory_to_clickhouse",
        "description": (
            "Scan one or more directories and ingest all matching files into a ClickHouse table. "
            "Optionally filter files by keyword: only files containing the keyword will be ingested. "
            "Example: open all CSV files from 3 subdirectories, keep only those mentioning 'invoice', "
            "then load them into ClickHouse."
        ),
        "params": {
            "directories": "List of directory paths to scan",
            "table_name": "ClickHouse target table name",
            "file_extensions": "File extensions to process (e.g. ['.csv', '.txt']) — default: ['.csv', '.json', '.txt']",
            "keyword_filter": "Optional keyword: only ingest files whose content contains this word/phrase",
            "recursive": "Search subdirectories (default: true)",
            "file_format": "Override file format: 'csv', 'json', 'txt' (auto-detected if omitted)",
            "encoding": "File encoding (default: 'utf-8')",
            "batch_size": "Rows per INSERT batch (default: 5000)",
        },
        "required": ["directories", "table_name"],
    },
    {
        "name": "store_finding",
        "description": "Store an important finding in semantic memory for later retrieval.",
        "params": {
            "key": "Short identifier for this finding",
            "value": "The finding content (text or structured data)",
            "category": "Category: 'file', 'search', 'ingest', 'finding'",
        },
        "required": ["key", "value"],
    },
    {
        "name": "think",
        "description": "Pure reasoning step — no external action. Use to plan before acting.",
        "params": {"reasoning": "Your detailed reasoning"},
        "required": ["reasoning"],
    },
    {
        "name": "final_answer",
        "description": "Emit the final answer when the task is complete.",
        "params": {
            "answer": "The complete answer or report",
            "summary": "One-sentence executive summary",
        },
        "required": ["answer"],
    },
]

FS_TOOL_NAMES = {t["name"] for t in FS_TOOL_DEFINITIONS}


# --------------------------------------------------------------------------- #
#  Tool executor                                                               #
# --------------------------------------------------------------------------- #

class FileSystemToolExecutor:
    """
    Executes filesystem tools for Windows/Linux navigation, content search,
    and ClickHouse data ingestion. Uses Python stdlib only.
    """

    def __init__(
        self,
        db_manager: "DBManager",
        memory: "MemoryManager",
        dispatch_callback: Optional[Callable] = None,
        allow_delete: bool = True,
    ):
        self.db = db_manager
        self.memory = memory
        self._dispatch = dispatch_callback
        self.allow_delete = allow_delete

    def execute(self, tool_name: str, params: Dict[str, Any]) -> Any:
        if tool_name not in FS_TOOL_NAMES:
            raise ValueError(f"Unknown filesystem tool: {tool_name}")
        method = getattr(self, f"_tool_{tool_name}", None)
        if method is None:
            raise ValueError(f"Tool '{tool_name}' is defined but not implemented")
        return method(**params)

    # ------------------------------------------------------------------ #
    #  Directory navigation                                                #
    # ------------------------------------------------------------------ #

    def _tool_list_directory(self, path: str, show_hidden: bool = False) -> List[Dict]:
        base = Path(path).resolve()
        if not base.exists():
            raise FileNotFoundError(f"Directory not found: {path}")
        if not base.is_dir():
            raise NotADirectoryError(f"Path is not a directory: {path}")

        entries = []
        for item in sorted(base.iterdir()):
            if not show_hidden and item.name.startswith("."):
                continue
            stat = item.stat()
            entries.append({
                "name": item.name,
                "type": "directory" if item.is_dir() else "file",
                "size_bytes": stat.st_size if item.is_file() else None,
                "modified": datetime.datetime.fromtimestamp(stat.st_mtime).isoformat(),
                "path": str(item),
            })
        return entries

    def _tool_list_all_recursive(
        self,
        path: str,
        extension_filter: Optional[str] = None,
        max_depth: int = 5,
    ) -> List[Dict]:
        base = Path(path).resolve()
        results = []
        ext = None
        if extension_filter:
            ext = extension_filter if extension_filter.startswith(".") else f".{extension_filter}"

        def _walk(p: Path, depth: int) -> None:
            if depth > max_depth:
                return
            try:
                for item in sorted(p.iterdir()):
                    if item.is_dir():
                        _walk(item, depth + 1)
                    elif item.is_file():
                        if ext and item.suffix.lower() != ext.lower():
                            continue
                        stat = item.stat()
                        results.append({
                            "path": str(item),
                            "name": item.name,
                            "relative_path": str(item.relative_to(base)),
                            "size_bytes": stat.st_size,
                            "extension": item.suffix,
                        })
            except PermissionError:
                pass

        _walk(base, 0)
        return results

    def _tool_create_directory(self, path: str) -> str:
        abs_path = Path(path).resolve()
        abs_path.mkdir(parents=True, exist_ok=True)
        return f"Directory created: {abs_path}"

    def _tool_delete_path(self, path: str, recursive: bool = False) -> str:
        if not self.allow_delete:
            raise PermissionError("Delete operations are disabled for this agent.")
        abs_path = Path(path).resolve()
        if not abs_path.exists():
            return f"Path not found: {path}"
        if abs_path.is_file():
            abs_path.unlink()
            return f"File deleted: {abs_path}"
        if abs_path.is_dir():
            if recursive:
                shutil.rmtree(str(abs_path))
                return f"Directory deleted recursively: {abs_path}"
            else:
                abs_path.rmdir()  # raises OSError if not empty
                return f"Empty directory deleted: {abs_path}"
        return f"Unknown path type: {abs_path}"

    def _tool_copy_path(
        self,
        source: str,
        destination: str,
        overwrite: bool = False,
    ) -> str:
        src = Path(source).resolve()
        dst = Path(destination).resolve()
        if dst.exists() and not overwrite:
            raise FileExistsError(
                f"Destination already exists: {dst}. Use overwrite=true to replace."
            )
        if src.is_file():
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(src), str(dst))
        elif src.is_dir():
            if dst.exists():
                shutil.rmtree(str(dst))
            shutil.copytree(str(src), str(dst))
        else:
            raise FileNotFoundError(f"Source not found: {source}")
        return f"Copied: {src} → {dst}"

    def _tool_move_path(self, source: str, destination: str) -> str:
        src = Path(source).resolve()
        dst = Path(destination).resolve()
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src), str(dst))
        return f"Moved: {src} → {dst}"

    def _tool_get_file_info(self, path: str) -> Dict:
        abs_path = Path(path).resolve()
        if not abs_path.exists():
            raise FileNotFoundError(f"Path not found: {path}")
        stat = abs_path.stat()
        return {
            "path": str(abs_path),
            "name": abs_path.name,
            "type": "directory" if abs_path.is_dir() else "file",
            "extension": abs_path.suffix,
            "size_bytes": stat.st_size,
            "created": datetime.datetime.fromtimestamp(stat.st_ctime).isoformat(),
            "modified": datetime.datetime.fromtimestamp(stat.st_mtime).isoformat(),
            "parent_directory": str(abs_path.parent),
        }

    def _tool_read_file_content(
        self,
        path: str,
        encoding: str = "utf-8",
        max_chars: int = 50000,
    ) -> str:
        abs_path = str(Path(path).resolve())
        with open(abs_path, "r", encoding=encoding, errors="replace") as f:
            content = f.read(max_chars)
        if len(content) == max_chars:
            content += f"\n\n[... content truncated at {max_chars} characters ...]"
        return content

    # ------------------------------------------------------------------ #
    #  File search                                                         #
    # ------------------------------------------------------------------ #

    def _tool_find_files(
        self,
        root: str,
        pattern: str,
        recursive: bool = True,
    ) -> List[str]:
        base = Path(root).resolve()
        if recursive:
            found = list(base.rglob(pattern))
        else:
            found = list(base.glob(pattern))
        return [str(f) for f in sorted(found) if f.is_file()]

    def _tool_search_content_in_files(
        self,
        directories: List[str],
        pattern: str,
        regex: bool = False,
        case_sensitive: bool = False,
        file_extensions: Optional[List[str]] = None,
        recursive: bool = True,
        max_results: int = 50,
    ) -> List[Dict]:
        flags = 0 if case_sensitive else re.IGNORECASE

        # Normalize allowed extensions
        if file_extensions:
            allowed_exts = {
                e if e.startswith(".") else f".{e}"
                for e in file_extensions
            }
        else:
            allowed_exts = _TEXT_EXTENSIONS

        results = []
        for dir_path in directories:
            if len(results) >= max_results:
                break
            base = Path(dir_path).resolve()
            if not base.exists():
                continue
            all_files = (
                [f for f in base.rglob("*") if f.is_file()]
                if recursive
                else [f for f in base.glob("*") if f.is_file()]
            )
            for file_path in all_files:
                if len(results) >= max_results:
                    break
                if file_path.suffix.lower() not in allowed_exts:
                    continue
                try:
                    with open(str(file_path), "r", encoding="utf-8", errors="replace") as f:
                        lines = f.readlines()
                    matching_lines = []
                    for i, line in enumerate(lines):
                        if regex:
                            hit = bool(re.search(pattern, line, flags=flags))
                        else:
                            hit = (
                                (pattern in line)
                                if case_sensitive
                                else (pattern.lower() in line.lower())
                            )
                        if hit:
                            matching_lines.append({
                                "line": i + 1,
                                "text": line.rstrip(),
                            })
                    if matching_lines:
                        results.append({
                            "file": str(file_path),
                            "match_count": len(matching_lines),
                            "matches": matching_lines[:10],   # max 10 matches shown per file
                        })
                except (PermissionError, UnicodeDecodeError):
                    continue

        return results

    def _tool_open_file_with_app(self, path: str) -> str:
        abs_path = str(Path(path).resolve())
        system = platform.system()
        if system == "Windows":
            os.startfile(abs_path)
        elif system == "Darwin":
            subprocess.Popen(["open", abs_path])
        else:
            subprocess.Popen(["xdg-open", abs_path])
        return f"Opened with default application: {abs_path}"

    # ------------------------------------------------------------------ #
    #  ClickHouse ingestion                                                #
    # ------------------------------------------------------------------ #

    def _detect_format(self, path: str) -> str:
        ext = Path(path).suffix.lower()
        if ext == ".csv" or ext == ".tsv":
            return "csv"
        if ext in (".json", ".ndjson", ".jsonl"):
            return "json"
        return "txt"

    def _read_csv_rows(
        self, path: str, delimiter: str = ",", encoding: str = "utf-8"
    ) -> List[Dict]:
        rows = []
        with open(path, "r", encoding=encoding, newline="") as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            for row in reader:
                rows.append(dict(row))
        return rows

    def _read_json_rows(self, path: str, encoding: str = "utf-8") -> List[Dict]:
        with open(path, "r", encoding=encoding) as f:
            content = f.read().strip()

        # Support NDJSON / JSONL (one JSON object per line)
        if content.startswith("{"):
            rows = []
            for line in content.splitlines():
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
            return rows

        data = json.loads(content)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
        raise ValueError("JSON file must contain an array or object at top level.")

    def _read_txt_rows(self, path: str, encoding: str = "utf-8") -> List[Dict]:
        rows = []
        with open(path, "r", encoding=encoding) as f:
            for i, line in enumerate(f, start=1):
                rows.append({"line_number": i, "content": line.rstrip()})
        return rows

    def _ensure_ch_table(self, table_name: str, sample_row: Dict) -> None:
        """Create ClickHouse table if it does not exist, inferring column types from the sample row."""
        existing = self.db.query(
            f"SELECT name FROM system.tables "
            f"WHERE database = currentDatabase() AND name = '{table_name}'",
            db="clickhouse",
            max_rows=1,
        )
        if existing:
            return  # table already exists

        col_defs = []
        for col, val in sample_row.items():
            safe_col = re.sub(r"[^a-zA-Z0-9_]", "_", str(col))
            if safe_col and safe_col[0].isdigit():
                safe_col = f"col_{safe_col}"
            if isinstance(val, bool):
                ch_type = "UInt8"
            elif isinstance(val, int):
                ch_type = "Int64"
            elif isinstance(val, float):
                ch_type = "Float64"
            else:
                ch_type = "String"
            col_defs.append(f"    `{safe_col}` Nullable({ch_type})")

        col_defs.append("    `_source_file` String DEFAULT ''")
        col_defs.append("    `_ingested_at` DateTime DEFAULT now()")

        ddl = (
            f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
            + ",\n".join(col_defs)
            + "\n) ENGINE = MergeTree() ORDER BY tuple()"
        )
        self.db.execute_write(ddl, db="clickhouse")

    def _insert_rows_to_ch(
        self,
        table_name: str,
        rows: List[Dict],
        source_file: str,
        batch_size: int = 5000,
    ) -> int:
        if not rows:
            return 0

        # Sanitize column names from first row
        raw_keys = list(rows[0].keys())
        sanitized = []
        for k in raw_keys:
            sc = re.sub(r"[^a-zA-Z0-9_]", "_", str(k))
            if sc and sc[0].isdigit():
                sc = f"col_{sc}"
            sanitized.append(sc)

        all_cols = sanitized + ["_source_file"]
        col_list = ", ".join(f"`{c}`" for c in all_cols)

        total_inserted = 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            values_parts = []
            for row in batch:
                vals = list(row.values()) + [source_file]
                escaped = []
                for v in vals:
                    if v is None:
                        escaped.append("NULL")
                    elif isinstance(v, bool):
                        escaped.append("1" if v else "0")
                    elif isinstance(v, (int, float)):
                        escaped.append(str(v))
                    else:
                        s = str(v).replace("\\", "\\\\").replace("'", "\\'")
                        escaped.append(f"'{s}'")
                values_parts.append(f"({', '.join(escaped)})")

            insert_sql = (
                f"INSERT INTO {table_name} ({col_list}) VALUES "
                + ", ".join(values_parts)
            )
            self.db.execute_write(insert_sql, db="clickhouse")
            total_inserted += len(batch)

        return total_inserted

    def _tool_ingest_file_to_clickhouse(
        self,
        file_path: str,
        table_name: str,
        file_format: Optional[str] = None,
        delimiter: str = ",",
        encoding: str = "utf-8",
        create_table_if_missing: bool = True,
        batch_size: int = 5000,
    ) -> Dict:
        abs_path = str(Path(file_path).resolve())
        fmt = file_format or self._detect_format(abs_path)

        if fmt == "csv":
            rows = self._read_csv_rows(abs_path, delimiter=delimiter, encoding=encoding)
        elif fmt == "json":
            rows = self._read_json_rows(abs_path, encoding=encoding)
        else:
            rows = self._read_txt_rows(abs_path, encoding=encoding)

        if not rows:
            return {"status": "empty", "file": abs_path, "rows_ingested": 0}

        if create_table_if_missing:
            self._ensure_ch_table(table_name, rows[0])

        n_inserted = self._insert_rows_to_ch(table_name, rows, abs_path, batch_size)
        return {
            "status": "success",
            "file": abs_path,
            "format": fmt,
            "rows_read": len(rows),
            "rows_ingested": n_inserted,
            "table": table_name,
        }

    def _tool_ingest_directory_to_clickhouse(
        self,
        directories: List[str],
        table_name: str,
        file_extensions: Optional[List[str]] = None,
        keyword_filter: Optional[str] = None,
        recursive: bool = True,
        file_format: Optional[str] = None,
        encoding: str = "utf-8",
        batch_size: int = 5000,
    ) -> Dict:
        # Default extensions
        allowed_exts = set(file_extensions) if file_extensions else {".csv", ".json", ".txt"}
        allowed_exts = {e if e.startswith(".") else f".{e}" for e in allowed_exts}

        # Collect candidate files
        all_files: List[Path] = []
        for dir_path in directories:
            base = Path(dir_path).resolve()
            candidates = (
                [f for f in base.rglob("*") if f.is_file()]
                if recursive
                else [f for f in base.glob("*") if f.is_file()]
            )
            for f in candidates:
                if f.suffix.lower() in allowed_exts:
                    all_files.append(f)

        # Apply optional keyword filter
        if keyword_filter:
            filtered = []
            for f in all_files:
                try:
                    with open(str(f), "r", encoding=encoding, errors="replace") as fh:
                        if keyword_filter.lower() in fh.read().lower():
                            filtered.append(f)
                except Exception:
                    pass
            all_files = filtered

        # Ingest each file
        results = []
        total_rows = 0
        for f in all_files:
            try:
                res = self._tool_ingest_file_to_clickhouse(
                    file_path=str(f),
                    table_name=table_name,
                    file_format=file_format,
                    encoding=encoding,
                    batch_size=batch_size,
                )
                results.append(res)
                total_rows += res.get("rows_ingested", 0)
            except Exception as e:
                results.append({
                    "file": str(f),
                    "status": "error",
                    "error": str(e),
                })

        return {
            "status": "done",
            "directories_scanned": directories,
            "files_found": len(all_files),
            "files_processed": len([r for r in results if r.get("status") != "error"]),
            "files_with_errors": len([r for r in results if r.get("status") == "error"]),
            "total_rows_ingested": total_rows,
            "table": table_name,
            "keyword_filter": keyword_filter,
            "details": results,
        }

    # ------------------------------------------------------------------ #
    #  Memory & system tools                                               #
    # ------------------------------------------------------------------ #

    def _tool_store_finding(
        self,
        key: str,
        value: Any,
        category: str = "finding",
    ) -> str:
        self.memory.store_fact(key, value, source="filesystem-agent",
                               category=category, confidence=1.0)
        return f"Finding '{key}' stored in semantic memory."

    def _tool_think(self, reasoning: str = "") -> str:
        return f"[REASONING] {reasoning}"

    def _tool_final_answer(self, answer: str, summary: str = "") -> Dict[str, str]:
        return {"answer": answer, "summary": summary or answer[:200]}
