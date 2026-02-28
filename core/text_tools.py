"""
Text File Tool Registry — Outils pour créer, lire et modifier des fichiers texte.

Utilise uniquement la stdlib Python (os, re, pathlib).
Compatible Windows et Linux.

Outils disponibles :
  create_text_file  — Créer un fichier texte
  read_text_file    — Lire le contenu d'un fichier texte
  write_text_file   — Écraser le contenu d'un fichier texte
  append_to_file    — Ajouter du texte à la fin d'un fichier
  search_in_file    — Rechercher un motif dans un fichier (avec numéros de lignes)
  replace_in_file   — Remplacer des occurrences dans un fichier
  count_lines       — Compter le nombre de lignes d'un fichier
  delete_text_file  — Supprimer un fichier
  list_text_files   — Lister les fichiers texte d'un répertoire
  get_file_stats    — Statistiques d'un fichier (lignes, mots, caractères, taille)
"""
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from core.memory import MemoryManager


# --------------------------------------------------------------------------- #
#  Tool definitions                                                            #
# --------------------------------------------------------------------------- #

TEXT_TOOL_DEFINITIONS: List[Dict] = [
    {
        "name": "create_text_file",
        "description": "Create a new text file with optional initial content. Parent directories are created automatically.",
        "params": {
            "path": "Path to the file to create (e.g. 'C:/data/notes.txt' or '/home/user/notes.txt')",
            "content": "Initial content to write (optional, default: empty file)",
            "encoding": "File encoding (default: 'utf-8')",
        },
        "required": ["path"],
    },
    {
        "name": "read_text_file",
        "description": "Read the content of a text file. Optionally limit to a line range.",
        "params": {
            "path": "Path to the file",
            "encoding": "File encoding (default: 'utf-8')",
            "start_line": "First line to read — 1-based (optional, reads from beginning if omitted)",
            "end_line": "Last line to read — 1-based (optional, reads to end if omitted)",
        },
        "required": ["path"],
    },
    {
        "name": "write_text_file",
        "description": "Overwrite a text file with new content. Creates parent directories if needed.",
        "params": {
            "path": "Path to the file",
            "content": "New content to write (replaces existing content)",
            "encoding": "File encoding (default: 'utf-8')",
        },
        "required": ["path", "content"],
    },
    {
        "name": "append_to_file",
        "description": "Append text to the end of a file. Creates the file if it does not exist.",
        "params": {
            "path": "Path to the file",
            "content": "Text to append",
            "newline": "Add a newline separator before the new content (default: true)",
            "encoding": "File encoding (default: 'utf-8')",
        },
        "required": ["path", "content"],
    },
    {
        "name": "search_in_file",
        "description": (
            "Search for a word or pattern in a file. "
            "Returns matching lines with their line numbers and optional context lines."
        ),
        "params": {
            "path": "Path to the file",
            "pattern": "Search pattern (plain string or regular expression)",
            "regex": "Interpret pattern as a regular expression (default: false)",
            "case_sensitive": "Case-sensitive search (default: false)",
            "context_lines": "Number of surrounding lines to include around each match (default: 0)",
            "encoding": "File encoding (default: 'utf-8')",
        },
        "required": ["path", "pattern"],
    },
    {
        "name": "replace_in_file",
        "description": "Replace all occurrences of a search string (or regex) in a file with a replacement.",
        "params": {
            "path": "Path to the file",
            "search": "Text or pattern to search for",
            "replacement": "Replacement text",
            "regex": "Interpret search as a regular expression (default: false)",
            "case_sensitive": "Case-sensitive search (default: true)",
            "encoding": "File encoding (default: 'utf-8')",
        },
        "required": ["path", "search", "replacement"],
    },
    {
        "name": "count_lines",
        "description": "Count the number of lines in a text file.",
        "params": {
            "path": "Path to the file",
            "encoding": "File encoding (default: 'utf-8')",
        },
        "required": ["path"],
    },
    {
        "name": "delete_text_file",
        "description": "Permanently delete a text file from disk.",
        "params": {
            "path": "Path to the file to delete",
        },
        "required": ["path"],
    },
    {
        "name": "list_text_files",
        "description": "List files in a directory, optionally filtered by extension and recursively.",
        "params": {
            "directory": "Directory path to list files from",
            "extension": "File extension filter (e.g. '.txt', '.csv', '.log') — optional",
            "recursive": "Search subdirectories recursively (default: false)",
        },
        "required": ["directory"],
    },
    {
        "name": "get_file_stats",
        "description": "Get statistics about a text file: line count, word count, character count, size in bytes.",
        "params": {
            "path": "Path to the file",
            "encoding": "File encoding (default: 'utf-8')",
        },
        "required": ["path"],
    },
    {
        "name": "store_finding",
        "description": "Store an important finding in semantic memory for later retrieval.",
        "params": {
            "key": "Short identifier for this finding",
            "value": "The finding content",
            "category": "Category: 'file', 'search', 'finding'",
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

TEXT_TOOL_NAMES = {t["name"] for t in TEXT_TOOL_DEFINITIONS}


# --------------------------------------------------------------------------- #
#  Tool executor                                                               #
# --------------------------------------------------------------------------- #

class TextToolExecutor:
    """
    Executes text file tools using Python stdlib only (os, re, pathlib).
    No external dependencies required.
    """

    def __init__(
        self,
        memory: "MemoryManager",
        dispatch_callback=None,
    ):
        self.memory = memory
        self._dispatch = dispatch_callback

    def execute(self, tool_name: str, params: Dict[str, Any]) -> Any:
        if tool_name not in TEXT_TOOL_NAMES:
            raise ValueError(f"Unknown text tool: {tool_name}")
        method = getattr(self, f"_tool_{tool_name}", None)
        if method is None:
            raise ValueError(f"Tool '{tool_name}' is defined but not implemented")
        return method(**params)

    # ------------------------------------------------------------------ #
    #  Text file tools                                                     #
    # ------------------------------------------------------------------ #

    def _tool_create_text_file(
        self,
        path: str,
        content: str = "",
        encoding: str = "utf-8",
    ) -> str:
        abs_path = str(Path(path).resolve())
        Path(abs_path).parent.mkdir(parents=True, exist_ok=True)
        with open(abs_path, "w", encoding=encoding) as f:
            f.write(content)
        return f"File created: {abs_path} ({len(content)} characters)"

    def _tool_read_text_file(
        self,
        path: str,
        encoding: str = "utf-8",
        start_line: Optional[int] = None,
        end_line: Optional[int] = None,
    ) -> str:
        abs_path = str(Path(path).resolve())
        with open(abs_path, "r", encoding=encoding) as f:
            if start_line is not None or end_line is not None:
                lines = f.readlines()
                s = (start_line - 1) if start_line else 0
                e = end_line if end_line else len(lines)
                return "".join(lines[s:e])
            return f.read()

    def _tool_write_text_file(
        self,
        path: str,
        content: str,
        encoding: str = "utf-8",
    ) -> str:
        abs_path = str(Path(path).resolve())
        Path(abs_path).parent.mkdir(parents=True, exist_ok=True)
        with open(abs_path, "w", encoding=encoding) as f:
            f.write(content)
        return f"File written: {abs_path} ({len(content)} characters)"

    def _tool_append_to_file(
        self,
        path: str,
        content: str,
        newline: bool = True,
        encoding: str = "utf-8",
    ) -> str:
        abs_path = str(Path(path).resolve())
        Path(abs_path).parent.mkdir(parents=True, exist_ok=True)
        with open(abs_path, "a", encoding=encoding) as f:
            p = Path(abs_path)
            if newline and p.exists() and p.stat().st_size > 0:
                f.write("\n")
            f.write(content)
        return f"Appended {len(content)} characters to {abs_path}"

    def _tool_search_in_file(
        self,
        path: str,
        pattern: str,
        regex: bool = False,
        case_sensitive: bool = False,
        context_lines: int = 0,
        encoding: str = "utf-8",
    ) -> List[Dict]:
        abs_path = str(Path(path).resolve())
        with open(abs_path, "r", encoding=encoding) as f:
            lines = f.readlines()

        flags = 0 if case_sensitive else re.IGNORECASE
        matches = []
        for i, line in enumerate(lines):
            if regex:
                found = bool(re.search(pattern, line, flags=flags))
            else:
                found = (pattern in line) if case_sensitive else (pattern.lower() in line.lower())

            if found:
                start = max(0, i - context_lines)
                end = min(len(lines), i + context_lines + 1)
                ctx = [
                    {"line": j + 1, "text": lines[j].rstrip(), "is_match": j == i}
                    for j in range(start, end)
                ]
                matches.append({
                    "line_number": i + 1,
                    "line": line.rstrip(),
                    "context": ctx,
                })
        return matches

    def _tool_replace_in_file(
        self,
        path: str,
        search: str,
        replacement: str,
        regex: bool = False,
        case_sensitive: bool = True,
        encoding: str = "utf-8",
    ) -> str:
        abs_path = str(Path(path).resolve())
        with open(abs_path, "r", encoding=encoding) as f:
            content = f.read()

        flags = 0 if case_sensitive else re.IGNORECASE
        if regex:
            new_content, count = re.subn(search, replacement, content, flags=flags)
        else:
            if case_sensitive:
                count = content.count(search)
                new_content = content.replace(search, replacement)
            else:
                compiled = re.compile(re.escape(search), flags)
                new_content, count = compiled.subn(replacement, content)

        with open(abs_path, "w", encoding=encoding) as f:
            f.write(new_content)
        return f"Replaced {count} occurrence(s) of '{search}' with '{replacement}' in {abs_path}"

    def _tool_count_lines(self, path: str, encoding: str = "utf-8") -> Dict:
        abs_path = str(Path(path).resolve())
        with open(abs_path, "r", encoding=encoding) as f:
            count = sum(1 for _ in f)
        return {"path": abs_path, "line_count": count}

    def _tool_delete_text_file(self, path: str) -> str:
        abs_path = str(Path(path).resolve())
        Path(abs_path).unlink()
        return f"File deleted: {abs_path}"

    def _tool_list_text_files(
        self,
        directory: str,
        extension: Optional[str] = None,
        recursive: bool = False,
    ) -> List[str]:
        base = Path(directory).resolve()
        if recursive:
            files = [f for f in base.rglob("*") if f.is_file()]
        else:
            files = [f for f in base.glob("*") if f.is_file()]

        if extension:
            ext = extension if extension.startswith(".") else f".{extension}"
            files = [f for f in files if f.suffix.lower() == ext.lower()]
        return [str(f) for f in sorted(files)]

    def _tool_get_file_stats(self, path: str, encoding: str = "utf-8") -> Dict:
        abs_path = str(Path(path).resolve())
        stat = Path(abs_path).stat()
        with open(abs_path, "r", encoding=encoding) as f:
            content = f.read()
        return {
            "path": abs_path,
            "size_bytes": stat.st_size,
            "line_count": len(content.splitlines()),
            "word_count": len(content.split()),
            "char_count": len(content),
            "encoding": encoding,
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
        self.memory.store_fact(key, value, source="text-agent",
                               category=category, confidence=1.0)
        return f"Finding '{key}' stored in semantic memory."

    def _tool_think(self, reasoning: str = "") -> str:
        return f"[REASONING] {reasoning}"

    def _tool_final_answer(self, answer: str, summary: str = "") -> Dict[str, str]:
        return {"answer": answer, "summary": summary or answer[:200]}
