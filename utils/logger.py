"""
Logger coloré pour le terminal Windows/Linux.
Utilise colorama (facultatif) ou des codes ANSI directs.
"""
import sys
import json
import time
import logging
import os
from typing import Any, Dict, Optional

# Try colorama (Windows color support)
try:
    import colorama  # type: ignore
    colorama.init(autoreset=True)
    COLORAMA = True
except ImportError:
    COLORAMA = False

# ANSI color codes (work on modern Windows 10+ terminal, Linux, macOS)
class C:
    RESET  = "\033[0m"
    BOLD   = "\033[1m"
    DIM    = "\033[2m"
    # Foreground
    RED    = "\033[91m"
    GREEN  = "\033[92m"
    YELLOW = "\033[93m"
    BLUE   = "\033[94m"
    MAGENTA= "\033[95m"
    CYAN   = "\033[96m"
    WHITE  = "\033[97m"
    GREY   = "\033[90m"


def _strip_ansi(text: str) -> str:
    import re
    return re.sub(r"\033\[[0-9;]*m", "", text)


class AgentLogger:
    """
    Logs agent activity to console (colored) and optionally to a log file.
    """

    def __init__(
        self,
        name: str = "AgentManager",
        log_file: Optional[str] = None,
        level: str = "INFO",
        colors: bool = True,
    ):
        self.name   = name
        self.colors = colors and (sys.stdout.isatty() or COLORAMA)
        self._indent = 0

        # File handler
        self._file_handler: Optional[logging.FileHandler] = None
        if log_file:
            os.makedirs(os.path.dirname(os.path.abspath(log_file)), exist_ok=True)
            fh = logging.FileHandler(log_file, encoding="utf-8")
            fh.setLevel(logging.DEBUG)
            fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
            self._file_handler = fh

    # ------------------------------------------------------------------ #
    #  Public interface                                                    #
    # ------------------------------------------------------------------ #

    def agent_start(self, name: str, task: str):
        self._print_separator("=", color=C.CYAN)
        self._print(f"  AGENT START: {name}", color=C.CYAN, bold=True)
        self._print(f"  TASK: {task[:120]}", color=C.WHITE)
        self._print_separator("=", color=C.CYAN)
        self._log(f"AGENT START [{name}]: {task}")

    def agent_done(self, name: str, steps: int, duration: float):
        self._print_separator("-", color=C.GREEN)
        self._print(
            f"  AGENT DONE: {name} | {steps} steps | {duration:.1f}s",
            color=C.GREEN, bold=True
        )
        self._print_separator("-", color=C.GREEN)
        self._log(f"AGENT DONE [{name}]: {steps} steps in {duration:.1f}s")

    def step(self, num: int, thought: str, action: str, params: Dict, confidence: float):
        thought_short = thought[:150] + ("…" if len(thought) > 150 else "")
        params_str    = json.dumps(params, ensure_ascii=False, default=str)[:120]

        self._print(f"\n[Step {num}]", color=C.YELLOW, bold=True)
        self._print(f"  THOUGHT    : {thought_short}", color=C.WHITE)
        self._print(f"  ACTION     : {action}", color=C.MAGENTA, bold=True)
        self._print(f"  PARAMS     : {params_str}", color=C.GREY)
        self._print(f"  CONFIDENCE : {confidence:.0%}", color=C.CYAN)
        self._log(f"Step {num} | {action} | conf={confidence:.2f} | {thought_short}")

    def step_result(self, result: Any, error: Optional[str]):
        if error:
            self._print(f"  RESULT [ERR]: {str(error)[:200]}", color=C.RED)
            self._log(f"  ERROR: {error}")
        else:
            result_str = self._format_result(result)
            self._print(f"  RESULT [OK] : {result_str[:200]}", color=C.GREEN)
            self._log(f"  OK: {result_str[:200]}")

    def manager_dispatch(self, agent_type: str, task: str):
        self._print(f"\n  >> DISPATCH {agent_type.upper()} AGENT", color=C.BLUE, bold=True)
        self._print(f"     Task: {task[:120]}", color=C.GREY)
        self._log(f"DISPATCH [{agent_type}]: {task}")

    def manager_result(self, agent_type: str, summary: str):
        self._print(f"  << {agent_type.upper()} returned: {summary[:120]}", color=C.BLUE)
        self._log(f"RESULT [{agent_type}]: {summary}")

    def info(self, msg: str):
        self._print(f"  [INFO] {msg}", color=C.CYAN)
        self._log(f"INFO: {msg}")

    def warn(self, msg: str):
        self._print(f"  [WARN] {msg}", color=C.YELLOW)
        self._log(f"WARN: {msg}")

    def error(self, msg: str):
        self._print(f"  [ERROR] {msg}", color=C.RED, bold=True)
        self._log(f"ERROR: {msg}")

    def section(self, title: str):
        self._print_separator("-", color=C.BLUE)
        self._print(f"  {title}", color=C.BLUE, bold=True)

    def final_answer(self, answer: str, summary: str = ""):
        self._print_separator("*", color=C.GREEN)
        self._print("  FINAL ANSWER", color=C.GREEN, bold=True)
        if summary:
            self._print(f"  SUMMARY: {summary}", color=C.WHITE, bold=True)
        self._print_separator("-", color=C.GREEN)
        self._print(answer, color=C.WHITE)
        self._print_separator("*", color=C.GREEN)
        self._log(f"FINAL_ANSWER: {answer[:500]}")

    # ------------------------------------------------------------------ #
    #  Internal                                                            #
    # ------------------------------------------------------------------ #

    def _print(self, text: str, color: str = "", bold: bool = False):
        if self.colors and (color or bold):
            prefix = (C.BOLD if bold else "") + color
            line   = f"{prefix}{text}{C.RESET}"
        else:
            line = text
        print(line, flush=True)

    def _print_separator(self, char: str = "-", width: int = 70, color: str = ""):
        self._print(char * width, color=color)

    def _log(self, msg: str):
        if self._file_handler:
            record = logging.LogRecord(
                name=self.name, level=logging.INFO,
                pathname="", lineno=0,
                msg=_strip_ansi(msg), args=(), exc_info=None,
            )
            self._file_handler.emit(record)

    @staticmethod
    def _format_result(result: Any) -> str:
        if result is None:
            return "None"
        if isinstance(result, list):
            if not result:
                return "[] (empty)"
            return f"[{len(result)} rows] {json.dumps(result[0], ensure_ascii=False, default=str)[:100]}…"
        if isinstance(result, dict):
            return json.dumps(result, ensure_ascii=False, default=str)[:200]
        return str(result)[:200]
