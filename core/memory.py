"""
Hierarchical Memory System — supporte des chaînes de 15+ étapes sans perte de contexte.

Architecture :
  1. Working Memory   : les N dernières étapes (accès rapide)
  2. Episodic Memory  : résumés compressés des étapes passées
  3. Semantic Memory  : faits clés extraits (schéma, patterns, anomalies)
  4. Goal Stack       : objectifs imbriqués courants
"""
import json
import time
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field, asdict
from copy import deepcopy


@dataclass
class Step:
    """Une étape d'exécution de l'agent."""
    index:      int
    thought:    str
    action:     str
    params:     Dict[str, Any]
    result:     Any
    error:      Optional[str] = None
    timestamp:  float = field(default_factory=time.time)
    duration_s: float = 0.0
    importance: float = 0.5   # 0=insignifiant, 1=critique

    def to_text(self) -> str:
        result_str = self._format_result(self.result)
        err_str = f"\n  ERROR: {self.error}" if self.error else ""
        return (
            f"[Step {self.index}] THOUGHT: {self.thought}\n"
            f"  ACTION: {self.action}({json.dumps(self.params, ensure_ascii=False)[:200]})\n"
            f"  RESULT: {result_str[:500]}{err_str}"
        )

    def to_compact(self) -> str:
        """Short single-line representation for episodic summaries."""
        result_str = self._format_result(self.result)
        status = "ERR" if self.error else "OK"
        return (
            f"Step{self.index}[{status}] {self.action} → {result_str[:120]}"
        )

    @staticmethod
    def _format_result(result: Any) -> str:
        if result is None:
            return "None"
        if isinstance(result, list):
            if len(result) == 0:
                return "[] (empty)"
            sample = json.dumps(result[:3], ensure_ascii=False, default=str)
            total = f" ({len(result)} rows total)" if len(result) > 3 else ""
            return sample + total
        if isinstance(result, dict):
            return json.dumps(result, ensure_ascii=False, default=str)[:300]
        return str(result)[:300]


@dataclass
class Fact:
    """Un fait clé extrait et gardé en mémoire sémantique."""
    key:        str               # identifiant court
    value:      Any               # valeur
    source:     str               # "step-3", "user", "discovery"…
    category:   str               # "schema", "pattern", "anomaly", "metric", "finding"
    confidence: float = 1.0
    timestamp:  float = field(default_factory=time.time)

    def __str__(self):
        return f"[{self.category.upper()}] {self.key}: {str(self.value)[:200]}"


class MemoryManager:
    """
    Gère la mémoire de l'agent pour des chaînes longues.

    Params
    ------
    working_window : int
        Nombre d'étapes récentes à garder en mémoire de travail intactes.
    compress_threshold : int
        Quand le total dépasse ce seuil, compresser les étapes hors fenêtre.
    """

    def __init__(
        self,
        working_window: int = 6,
        compress_threshold: int = 10,
        agent_name: str = "Agent",
    ):
        self.working_window     = working_window
        self.compress_threshold = compress_threshold
        self.agent_name         = agent_name

        # The three memory tiers
        self._steps:    List[Step]    = []   # all steps (working + episodic source)
        self._episodes: List[str]     = []   # compressed episode summaries
        self._facts:    Dict[str, Fact] = {}  # key → Fact
        self._goal_stack: List[str]   = []

        self._step_counter = 0

    # ------------------------------------------------------------------ #
    #  Step recording                                                      #
    # ------------------------------------------------------------------ #

    def add_step(
        self,
        thought:    str,
        action:     str,
        params:     Dict[str, Any],
        result:     Any,
        error:      Optional[str] = None,
        duration_s: float = 0.0,
    ) -> Step:
        self._step_counter += 1
        importance = self._estimate_importance(action, result, error)
        step = Step(
            index=self._step_counter,
            thought=thought,
            action=action,
            params=params,
            result=result,
            error=error,
            duration_s=duration_s,
            importance=importance,
        )
        self._steps.append(step)

        # Auto-compress when we exceed threshold
        if len(self._steps) > self.compress_threshold:
            self._compress_old_steps()

        return step

    # ------------------------------------------------------------------ #
    #  Fact (semantic memory)                                              #
    # ------------------------------------------------------------------ #

    def store_fact(
        self,
        key: str,
        value: Any,
        source: str = "agent",
        category: str = "finding",
        confidence: float = 1.0,
    ):
        self._facts[key] = Fact(key=key, value=value, source=source,
                                category=category, confidence=confidence)

    def get_fact(self, key: str) -> Optional[Any]:
        f = self._facts.get(key)
        return f.value if f else None

    def get_facts_by_category(self, category: str) -> List[Fact]:
        return [f for f in self._facts.values() if f.category == category]

    def all_facts(self) -> List[Fact]:
        return list(self._facts.values())

    # ------------------------------------------------------------------ #
    #  Goal stack                                                          #
    # ------------------------------------------------------------------ #

    def push_goal(self, goal: str):
        self._goal_stack.append(goal)

    def pop_goal(self) -> Optional[str]:
        return self._goal_stack.pop() if self._goal_stack else None

    def current_goal(self) -> Optional[str]:
        return self._goal_stack[-1] if self._goal_stack else None

    def goal_stack(self) -> List[str]:
        return list(self._goal_stack)

    # ------------------------------------------------------------------ #
    #  Context building (used by engine to construct LLM messages)         #
    # ------------------------------------------------------------------ #

    def build_context(self, max_chars: int = 8000) -> str:
        """
        Build a textual context block for the LLM.
        Includes: episodic summaries + working memory steps + key facts.
        Budget-aware: truncates if necessary.
        """
        parts = []

        # Goals
        if self._goal_stack:
            parts.append("=== CURRENT GOALS ===")
            for i, g in enumerate(reversed(self._goal_stack)):
                prefix = "→ (active)" if i == 0 else "  (parent)"
                parts.append(f"{prefix} {g}")

        # Key facts
        if self._facts:
            parts.append("\n=== KEY FINDINGS & FACTS ===")
            # Prioritize high-confidence facts
            sorted_facts = sorted(self._facts.values(),
                                  key=lambda f: (-f.confidence, f.timestamp))
            for fact in sorted_facts[:30]:  # cap at 30 facts
                parts.append(str(fact))

        # Episodic summaries
        if self._episodes:
            parts.append("\n=== PAST EPISODE SUMMARIES ===")
            for ep in self._episodes:
                parts.append(ep)

        # Working memory (recent steps)
        working = self._working_steps()
        if working:
            parts.append("\n=== RECENT STEPS (Working Memory) ===")
            for step in working:
                parts.append(step.to_text())

        context = "\n".join(parts)

        # Trim if over budget
        if len(context) > max_chars:
            context = context[:max_chars - 100] + "\n...[context truncated]"

        return context

    def build_messages_for_llm(
        self,
        system_prompt: str,
        user_task: str,
        current_thought_prompt: str,
    ) -> List[Dict[str, str]]:
        """
        Construct the full messages list for the LLM.
        """
        context = self.build_context()
        messages = [{"role": "system", "content": system_prompt}]

        # Inject task + context as a user message
        task_msg = f"TASK: {user_task}"
        if context:
            task_msg += f"\n\n{context}"
        messages.append({"role": "user", "content": task_msg})

        # The actual reasoning prompt
        messages.append({"role": "user", "content": current_thought_prompt})

        return messages

    # ------------------------------------------------------------------ #
    #  Summary / export                                                    #
    # ------------------------------------------------------------------ #

    def get_all_steps(self) -> List[Step]:
        return list(self._steps)

    def total_steps(self) -> int:
        return self._step_counter

    def summary(self) -> Dict[str, Any]:
        """Human-readable summary of this memory state."""
        return {
            "total_steps":      self._step_counter,
            "working_steps":    len(self._working_steps()),
            "episode_summaries": len(self._episodes),
            "facts":            len(self._facts),
            "goals":            len(self._goal_stack),
            "active_goal":      self.current_goal(),
        }

    def export_findings(self) -> Dict[str, Any]:
        """Export all findings organized by category."""
        findings: Dict[str, Any] = {}
        for fact in self._facts.values():
            findings.setdefault(fact.category, []).append({
                "key":        fact.key,
                "value":      fact.value,
                "confidence": fact.confidence,
                "source":     fact.source,
            })
        return findings

    # ------------------------------------------------------------------ #
    #  Internal                                                            #
    # ------------------------------------------------------------------ #

    def _working_steps(self) -> List[Step]:
        """Return the most recent `working_window` steps."""
        return self._steps[-self.working_window:] if self._steps else []

    def _compress_old_steps(self):
        """
        Compress steps outside the working window into an episodic summary.
        Keep high-importance steps in full even if old.
        """
        cutoff = len(self._steps) - self.working_window
        if cutoff <= 0:
            return

        to_compress = self._steps[:cutoff]
        # Extract critical facts before compressing
        for step in to_compress:
            self._auto_extract_facts(step)

        # Build episode summary
        lines = [f"[EPISODE: steps {to_compress[0].index}–{to_compress[-1].index}]"]
        for step in to_compress:
            lines.append(step.to_compact())
        episode_text = "\n".join(lines)
        self._episodes.append(episode_text)

        # Remove compressed steps from list
        self._steps = self._steps[cutoff:]

    def _auto_extract_facts(self, step: Step):
        """Heuristic: extract important facts from a step automatically."""
        # Store schema discoveries
        if step.action in ("list_tables", "describe_table", "get_schema"):
            if not step.error and step.result:
                key = f"schema_{step.action}_{step.index}"
                self.store_fact(key, step.result, source=f"step-{step.index}",
                                category="schema")

        # Store query results that look like aggregations
        if step.action in ("execute_sql", "run_query") and not step.error:
            if isinstance(step.result, list) and 1 <= len(step.result) <= 10:
                key = f"query_result_{step.index}"
                self.store_fact(key, step.result, source=f"step-{step.index}",
                                category="metric")

        # Store anomaly findings
        if "anomal" in step.thought.lower() or "quality" in step.thought.lower():
            if step.result and not step.error:
                key = f"quality_finding_{step.index}"
                self.store_fact(key, {"thought": step.thought[:200], "result": step.result},
                                source=f"step-{step.index}", category="anomaly",
                                confidence=0.8)

    @staticmethod
    def _estimate_importance(action: str, result: Any, error: Optional[str]) -> float:
        """Heuristic importance score for a step."""
        if error:
            return 0.3  # errors are low-importance after compression
        high_importance = {"final_answer", "store_finding", "dispatch_agent",
                           "get_schema", "describe_table"}
        if action in high_importance:
            return 0.9
        if isinstance(result, list) and len(result) > 0:
            return 0.7
        return 0.5

    def reset(self):
        self._steps.clear()
        self._episodes.clear()
        self._facts.clear()
        self._goal_stack.clear()
        self._step_counter = 0
