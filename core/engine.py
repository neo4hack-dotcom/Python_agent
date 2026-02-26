"""
ReAct Execution Engine — Boucle Reason → Act → Observe → Reflect

Innovations :
  - Structured JSON output (fonctionne avec tout LLM)
  - Auto-réflexion tous les N steps
  - Compression mémoire automatique
  - Détection de boucles (répétitions d'actions)
  - Budget de steps adaptatif
  - Injection de contexte DB au démarrage
"""
import json
import time
from typing import Any, Dict, List, Optional, Callable

from core.llm_client  import LLMClient, LLMError
from core.memory      import MemoryManager
from core.tools       import ToolExecutor, TOOL_DEFINITIONS
from utils.logger     import AgentLogger


# --------------------------------------------------------------------------- #
#  Prompt fragments                                                            #
# --------------------------------------------------------------------------- #

REACT_SYSTEM_TEMPLATE = """You are {agent_name}, an autonomous AI agent specialized in {specialization}.

## Your mission
{mission}

## Available tools
{tools_json}

## Response format — ALWAYS reply with a single JSON object:
```json
{{
  "thought": "Your step-by-step reasoning about what to do next",
  "action": "tool_name",
  "params": {{"param1": "value1"}},
  "confidence": 0.85
}}
```

## Rules
1. Reply ONLY with a valid JSON object — no prose, no markdown fences outside the JSON.
2. Use `think` tool for pure reasoning without side effects.
3. Use `store_finding` to persist important discoveries before they are compressed.
4. Use `final_answer` ONLY when the task is fully complete.
5. If a tool fails, analyze the error and try a different approach.
6. Be systematic: explore schema first, then sample data, then analyze.
7. After {reflection_interval} steps, explicitly reflect on your progress toward the goal.
8. You have a budget of {max_steps} steps — use them wisely.

## Current step budget: {steps_used}/{max_steps}
"""

REFLECTION_PROMPT = """
You have completed {n} steps. Pause and reflect:
1. What have you discovered so far?
2. Are you making progress toward the goal?
3. What are the most important remaining tasks?
4. Are there any dead ends you should avoid?

Reply with a JSON object:
{{
  "thought": "Detailed reflection on progress and next priorities",
  "action": "think",
  "params": {{"reasoning": "Your full reflection here"}},
  "confidence": 0.9
}}
"""

STEP_PROMPT = """
Step {step_num} — Decide your next action.

{context}

What is your next action to progress toward the goal?
Reply ONLY with a valid JSON object following the format defined in your system prompt.
"""


# --------------------------------------------------------------------------- #
#  Engine                                                                      #
# --------------------------------------------------------------------------- #

class AgentEngine:
    """
    Core ReAct engine. Runs one agent through its task.

    Parameters
    ----------
    llm             : LLMClient instance
    memory          : MemoryManager instance
    tool_executor   : ToolExecutor instance
    logger          : AgentLogger instance
    max_steps       : Maximum steps before forced stop
    reflection_interval : Steps between auto-reflections
    loop_detection_window : Steps to look back for repeated actions
    step_callback   : Optional callable(dict) called after each step for UI streaming
    """

    def __init__(
        self,
        llm:                   LLMClient,
        memory:                MemoryManager,
        tool_executor:         ToolExecutor,
        logger:                AgentLogger,
        agent_name:            str = "Agent",
        specialization:        str = "data analysis",
        mission:               str = "",
        max_steps:             int = 20,
        reflection_interval:   int = 5,
        loop_detection_window: int = 4,
        step_callback:         Optional[Callable] = None,
    ):
        self.llm    = llm
        self.memory = memory
        self.tools  = tool_executor
        self.logger = logger

        self.agent_name          = agent_name
        self.specialization      = specialization
        self.mission             = mission
        self.max_steps           = max_steps
        self.reflection_interval = reflection_interval
        self.loop_window         = loop_detection_window
        self.step_callback       = step_callback

        self._recent_actions: List[str] = []   # for loop detection

    # ------------------------------------------------------------------ #
    #  Main run loop                                                       #
    # ------------------------------------------------------------------ #

    def run(self, task: str, initial_context: str = "") -> Dict[str, Any]:
        """
        Execute the ReAct loop for the given task.
        Returns a result dict with keys: answer, summary, steps, findings.
        """
        self.memory.push_goal(task)
        if initial_context:
            self.memory.store_fact("initial_context", initial_context,
                                   source="user", category="finding")

        self.logger.agent_start(self.agent_name, task)
        start_time = time.time()

        system_prompt = self._build_system_prompt()
        result = None

        for step_num in range(1, self.max_steps + 1):
            # Auto-reflect at regular intervals
            is_reflection_step = (step_num > 1 and step_num % self.reflection_interval == 0)
            prompt = self._build_step_prompt(step_num, is_reflection_step)

            messages = [
                {"role": "system",  "content": system_prompt},
                {"role": "user",    "content": f"TASK: {task}"},
                {"role": "assistant", "content": "Understood. I will work on this task systematically."},
                {"role": "user",    "content": prompt},
            ]

            # --- LLM call ---
            t0 = time.time()
            try:
                raw = self.llm.complete(messages)
                decision = self.llm._extract_json(raw)
                if decision is None:
                    self.logger.warn(f"LLM output not parseable as JSON, retrying…")
                    messages.append({"role": "assistant", "content": raw})
                    messages.append({"role": "user",
                                     "content": "Reply ONLY with a valid JSON object as specified."})
                    raw2 = self.llm.complete(messages)
                    decision = self.llm._extract_json(raw2)
                    if decision is None:
                        decision = {"thought": raw2, "action": "think",
                                    "params": {"reasoning": raw2}, "confidence": 0.3}
            except LLMError as e:
                self.logger.error(f"LLM error at step {step_num}: {e}")
                break

            llm_duration = time.time() - t0

            thought    = decision.get("thought", "")
            action     = decision.get("action", "think")
            params     = decision.get("params", {})
            confidence = float(decision.get("confidence", 0.5))

            self.logger.step(step_num, thought, action, params, confidence)

            # --- Loop detection ---
            if self._detect_loop(action, params):
                self.logger.warn(
                    f"Loop detected at step {step_num} (action '{action}' repeated). "
                    "Injecting loop-break hint."
                )
                self.memory.store_fact(
                    f"loop_warning_{step_num}",
                    f"Action '{action}' was repeated — try a different approach",
                    source="engine", category="finding",
                )
                # Force a reflection step
                action = "think"
                params = {"reasoning": (
                    f"I notice I've been repeating action '{action}'. "
                    "I should try a completely different approach."
                )}

            # --- Execute tool ---
            t0 = time.time()
            tool_result = None
            tool_error  = None
            try:
                tool_result = self.tools.execute(action, params)
            except Exception as e:
                tool_error  = str(e)
                tool_result = None
                self.logger.error(f"Tool '{action}' failed: {e}")

            tool_duration = time.time() - t0

            # --- Record in memory ---
            step = self.memory.add_step(
                thought=thought,
                action=action,
                params=params,
                result=tool_result,
                error=tool_error,
                duration_s=llm_duration + tool_duration,
            )

            self.logger.step_result(tool_result, tool_error)

            # --- Notify UI callback ---
            if self.step_callback:
                try:
                    self.step_callback({
                        "type": "step",
                        "agent": self.agent_name,
                        "step": step_num,
                        "max_steps": self.max_steps,
                        "thought": thought,
                        "action": action,
                        "params": params,
                        "result": tool_result,
                        "error": tool_error,
                        "confidence": confidence,
                        "duration": round(llm_duration + tool_duration, 2),
                    })
                except Exception:
                    pass  # never let UI callback break the agent

            # --- Check termination ---
            if action == "final_answer":
                result = tool_result if isinstance(tool_result, dict) else {
                    "answer": str(tool_result),
                    "summary": str(tool_result)[:200],
                }
                self.logger.agent_done(self.agent_name, step_num, time.time() - start_time)
                break

        # If loop exhausted without final_answer
        if result is None:
            result = self._build_emergency_answer(task)
            self.logger.warn(
                f"Max steps ({self.max_steps}) reached without final_answer. "
                "Generating emergency summary."
            )

        result["steps_used"] = self.memory.total_steps()
        result["findings"]   = self.memory.export_findings()
        result["duration_s"] = round(time.time() - start_time, 2)

        return result

    # ------------------------------------------------------------------ #
    #  Prompt builders                                                     #
    # ------------------------------------------------------------------ #

    def _build_system_prompt(self) -> str:
        # Use CH-extended tool list if the base agent injected it, else fall back
        tool_defs  = getattr(self, "_ch_tool_defs", None) or TOOL_DEFINITIONS
        tools_json = json.dumps(
            [{"name": t["name"], "description": t["description"], "params": t.get("params", {})}
             for t in tool_defs],
            indent=2, ensure_ascii=False
        )
        return REACT_SYSTEM_TEMPLATE.format(
            agent_name=self.agent_name,
            specialization=self.specialization,
            mission=self.mission or f"Complete the assigned task using the available tools.",
            tools_json=tools_json,
            reflection_interval=self.reflection_interval,
            max_steps=self.max_steps,
            steps_used=self.memory.total_steps(),
        )

    def _build_step_prompt(self, step_num: int, is_reflection: bool) -> str:
        context = self.memory.build_context(max_chars=6000)
        if is_reflection:
            base = REFLECTION_PROMPT.format(n=step_num - 1)
        else:
            base = STEP_PROMPT.format(
                step_num=step_num,
                context=context,
            )
        return base

    # ------------------------------------------------------------------ #
    #  Loop detection                                                      #
    # ------------------------------------------------------------------ #

    def _detect_loop(self, action: str, params: Dict) -> bool:
        action_key = f"{action}:{json.dumps(params, sort_keys=True, default=str)[:100]}"
        self._recent_actions.append(action_key)
        if len(self._recent_actions) > self.loop_window:
            self._recent_actions.pop(0)
        # Loop if the same action appears 3+ times in the window
        count = self._recent_actions.count(action_key)
        return count >= 3

    # ------------------------------------------------------------------ #
    #  Emergency fallback                                                  #
    # ------------------------------------------------------------------ #

    def _build_emergency_answer(self, task: str) -> Dict[str, Any]:
        findings = self.memory.export_findings()
        steps    = self.memory.get_all_steps()
        lines    = [f"Task: {task}", "", "Findings accumulated:"]
        for cat, items in findings.items():
            lines.append(f"\n[{cat.upper()}]")
            for item in items:
                lines.append(f"  {item['key']}: {str(item['value'])[:200]}")
        if not findings:
            lines.append("  (no findings stored)")
        lines.append(f"\nTotal steps executed: {len(steps)}")
        return {
            "answer":  "\n".join(lines),
            "summary": f"Task incomplete after {self.max_steps} steps. Partial findings available.",
            "partial": True,
        }
