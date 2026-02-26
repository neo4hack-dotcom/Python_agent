"""
LangGraph Multi-Agent Graph
============================

Implémente un workflow multi-agents avec LangGraph StateGraph.

Flux d'exécution :
  START → [plan_node] → routing → [agent_node(s)] → [aggregate_node] → END

  - plan_node     : le LLM décide quels agents sont nécessaires pour la tâche
  - agent nodes   : wrappent les agents existants (Analyst, Quality, Pattern, Query)
  - aggregate_node: fusionne tous les résultats en un rapport final

Le routage conditionnel s'appuie sur la liste `plan` dans le GraphState.
"""
import json
import time
import os
from typing import TypedDict, List, Optional, Dict, Any
from datetime import datetime

from langgraph.graph import StateGraph, END

from core.llm_client import LLMClient
from core.db_manager import DBManager
from utils.logger import AgentLogger


# --------------------------------------------------------------------------- #
#  State                                                                       #
# --------------------------------------------------------------------------- #

class GraphState(TypedDict):
    """Shared state passed between all nodes in the graph."""
    task:           str                  # User's original task
    plan:           List[str]            # Ordered list of agent types to run
    agents_done:    List[str]            # Agents that have already completed
    sub_results:    Dict[str, Any]       # Results keyed by agent type
    shared_context: str                  # Cumulative context passed to each agent
    final_answer:   str                  # Final aggregated answer
    summary:        str                  # One-line executive summary
    error:          Optional[str]        # Error message if something went wrong


# All routable agent names (original + ClickHouse specialists)
_ALL_AGENT_NAMES = {
    "analyst", "quality", "pattern", "query",
    "sql_analyst", "clickhouse_generic", "clickhouse_table_manager",
    "clickhouse_writer", "clickhouse_specific", "text_to_sql_translator",
}


# --------------------------------------------------------------------------- #
#  Graph factory                                                               #
# --------------------------------------------------------------------------- #

def create_agent_graph(
    llm_client: LLMClient,
    db_manager: DBManager,
    config: dict,
    logger: AgentLogger,
):
    """
    Build and compile the LangGraph StateGraph for multi-agent analysis.

    Parameters
    ----------
    llm_client  : LLMClient instance (shared across all nodes)
    db_manager  : DBManager instance (shared across all nodes)
    config      : full config dict (agents, security, etc.)
    logger      : AgentLogger for colored console + file output

    Returns
    -------
    Compiled LangGraph graph (supports .invoke(), .stream())
    """
    agent_cfg   = config.get("agents", {})
    sec_cfg     = config.get("security", {})
    max_steps   = int(agent_cfg.get("max_steps", 20))
    allow_write = bool(sec_cfg.get("allow_write_queries", False))
    max_rows    = int(sec_cfg.get("max_rows_returned", 1000))

    # ------------------------------------------------------------------ #
    #  Node: plan                                                          #
    # ------------------------------------------------------------------ #

    def plan_node(state: GraphState) -> GraphState:
        """
        Ask the LLM which specialized agents to run for the given task.
        Populates state['plan'] with an ordered list of agent types.
        """
        task = state["task"]
        logger.section("LangGraph — Planning")
        logger.info(f"Task: {task[:120]}")

        prompt = (
            "You are an AI orchestrator for data analysis.\n"
            "Given the task below, decide which specialized agents to run.\n\n"
            "Available agents:\n"
            "  --- General purpose ---\n"
            "  - analyst                  : data statistics, trends, KPIs, distributions\n"
            "  - quality                  : data quality (nulls, duplicates, outliers)\n"
            "  - pattern                  : pattern discovery, correlations, anomaly detection\n"
            "  - query                    : SQL query building and optimization\n"
            "  --- ClickHouse specialists ---\n"
            "  - sql_analyst              : expert CH SQL generation with preflight EXPLAIN\n"
            "  - clickhouse_generic       : DAG-driven deep ClickHouse analysis\n"
            "  - clickhouse_table_manager : DDL admin (CREATE/ALTER tables)\n"
            "  - clickhouse_writer        : sandboxed DML (INSERT into agent_ tables)\n"
            "  - clickhouse_specific      : parameterized template reports (P1-P4)\n"
            "  - text_to_sql_translator   : natural-language to ClickHouse SQL\n\n"
            f"Task: {task}\n\n"
            "Reply ONLY with a valid JSON object:\n"
            "{\n"
            '  "plan": ["agent1", "agent2"],\n'
            '  "reasoning": "brief explanation"\n'
            "}\n\n"
            "Rules:\n"
            "- Include only agents relevant to the task\n"
            "- Order them logically (e.g. sql_analyst before clickhouse_generic)\n"
            "- Prefer ClickHouse specialists for ClickHouse-specific tasks\n"
            "- Minimum 1 agent, maximum 4 agents\n"
        )

        messages = [{"role": "user", "content": prompt}]
        try:
            raw      = llm_client.complete(messages)
            decision = llm_client._extract_json(raw)
            plan     = decision.get("plan", ["analyst"]) if decision else ["analyst"]
            valid    = _ALL_AGENT_NAMES
            plan     = [a for a in plan if a in valid]
            if not plan:
                plan = ["analyst"]
            reasoning = (decision or {}).get("reasoning", "")
            if reasoning:
                logger.info(f"Reasoning: {reasoning[:200]}")
        except Exception as exc:
            logger.warn(f"Planner LLM call failed ({exc}). Defaulting to [analyst].")
            plan = ["analyst"]

        logger.info(f"Plan: {plan}")
        return {
            **state,
            "plan":        plan,
            "agents_done": [],
            "sub_results": {},
        }

    # ------------------------------------------------------------------ #
    #  Routing function (conditional edge)                                 #
    # ------------------------------------------------------------------ #

    def route(state: GraphState) -> str:
        """
        Decide the next node after plan_node or any agent node.
        Returns the next agent name, or 'aggregate' when the plan is complete.
        """
        plan      = state.get("plan", [])
        done      = state.get("agents_done", [])
        remaining = [a for a in plan if a not in done]
        return remaining[0] if remaining else "aggregate"

    # ------------------------------------------------------------------ #
    #  Generic agent runner                                                #
    # ------------------------------------------------------------------ #

    def _merge_agent_result(
        agent_name: str,
        result: Dict[str, Any],
        state: GraphState,
        elapsed: float,
    ) -> GraphState:
        """Merge a completed agent result into the shared graph state."""
        logger.manager_result(agent_name, f"{result.get('summary', 'done')} ({elapsed}s)")
        new_sub_results = {**state.get("sub_results", {}), agent_name: result}
        new_done        = state.get("agents_done", []) + [agent_name]
        ctx_parts = []
        for name, res in new_sub_results.items():
            if res.get("summary"):
                ctx_parts.append(f"[{name.upper()} AGENT] {res['summary']}")
        return {
            **state,
            "sub_results":    new_sub_results,
            "agents_done":    new_done,
            "shared_context": "\n".join(ctx_parts),
        }

    def _run_agent(agent_cls, agent_name: str, state: GraphState) -> GraphState:
        """Instantiate a base agent and run it, then merge results into state."""
        task    = state["task"]
        context = state.get("shared_context", "")
        logger.manager_dispatch(agent_name, task)
        t0 = time.time()
        try:
            agent  = agent_cls(
                llm=llm_client, db=db_manager, logger=logger,
                max_steps=max(8, max_steps // 2),
                allow_write=allow_write,
                max_rows=max_rows,
            )
            result = agent.run(task, context=context)
        except Exception as exc:
            logger.error(f"Agent '{agent_name}' raised: {exc}")
            result = {
                "answer": f"Agent {agent_name} encountered an error: {exc}",
                "summary": f"{agent_name} failed: {exc}",
                "findings": {}, "steps_used": 0,
            }
        return _merge_agent_result(agent_name, result, state, round(time.time() - t0, 1))

    def _run_agent_instance(agent, agent_name: str, state: GraphState) -> GraphState:
        """Run a pre-built agent instance (used for CH specialists), merge results."""
        task    = state["task"]
        context = state.get("shared_context", "")
        logger.manager_dispatch(agent_name, task)
        t0 = time.time()
        try:
            result = agent.run(task, context=context)
        except Exception as exc:
            logger.error(f"Agent '{agent_name}' raised: {exc}")
            result = {
                "answer": f"Agent {agent_name} encountered an error: {exc}",
                "summary": f"{agent_name} failed: {exc}",
                "findings": {}, "steps_used": 0,
            }
        return _merge_agent_result(agent_name, result, state, round(time.time() - t0, 1))

    # ------------------------------------------------------------------ #
    #  Agent nodes                                                         #
    # ------------------------------------------------------------------ #

    def analyst_node(state: GraphState) -> GraphState:
        from agents.analyst_agent import AnalystAgent
        return _run_agent(AnalystAgent, "analyst", state)

    def quality_node(state: GraphState) -> GraphState:
        from agents.quality_agent import QualityAgent
        return _run_agent(QualityAgent, "quality", state)

    def pattern_node(state: GraphState) -> GraphState:
        from agents.pattern_agent import PatternAgent
        return _run_agent(PatternAgent, "pattern", state)

    def query_node(state: GraphState) -> GraphState:
        from agents.query_agent import QueryAgent
        return _run_agent(QueryAgent, "query", state)

    # ---- ClickHouse specialist nodes ----------------------------------------

    def sql_analyst_node(state: GraphState) -> GraphState:
        from agents.clickhouse import SQLAnalystAgent
        agent = SQLAnalystAgent.from_config(llm_client, db_manager, logger, config)
        return _run_agent_instance(agent, "sql_analyst", state)

    def clickhouse_generic_node(state: GraphState) -> GraphState:
        from agents.clickhouse import ClickHouseGenericAgent
        agent = ClickHouseGenericAgent.from_config(llm_client, db_manager, logger, config)
        return _run_agent_instance(agent, "clickhouse_generic", state)

    def clickhouse_table_manager_node(state: GraphState) -> GraphState:
        from agents.clickhouse import ClickHouseTableManagerAgent
        agent = ClickHouseTableManagerAgent.from_config(llm_client, db_manager, logger, config)
        return _run_agent_instance(agent, "clickhouse_table_manager", state)

    def clickhouse_writer_node(state: GraphState) -> GraphState:
        from agents.clickhouse import ClickHouseWriterAgent
        agent = ClickHouseWriterAgent.from_config(llm_client, db_manager, logger, config)
        return _run_agent_instance(agent, "clickhouse_writer", state)

    def clickhouse_specific_node(state: GraphState) -> GraphState:
        from agents.clickhouse import ClickHouseSpecificAgent
        agent = ClickHouseSpecificAgent.from_config(llm_client, db_manager, logger, config)
        return _run_agent_instance(agent, "clickhouse_specific", state)

    def text_to_sql_node(state: GraphState) -> GraphState:
        from agents.clickhouse import TextToSQLAgent
        agent = TextToSQLAgent.from_config(llm_client, db_manager, logger, config)
        return _run_agent_instance(agent, "text_to_sql_translator", state)

    # ------------------------------------------------------------------ #
    #  Aggregate node                                                      #
    # ------------------------------------------------------------------ #

    def aggregate_node(state: GraphState) -> GraphState:
        """
        Fuse all sub-agent results into a final answer and executive summary.
        """
        task        = state["task"]
        sub_results = state.get("sub_results", {})

        logger.section("LangGraph — Aggregating results")

        # Build structured report
        sections = [f"# Rapport d'analyse\n\nTâche : {task}\n"]
        for agent_name, result in sub_results.items():
            sections.append(f"\n## Agent {agent_name.title()}")
            summary = result.get("summary", "")
            answer  = result.get("answer", "")
            if summary:
                sections.append(f"**Résumé** : {summary}")
            if answer:
                sections.append(answer[:2000])

        final_answer = "\n".join(sections)

        # Use LLM for a clean executive summary
        summary_line = (
            f"Analyse complète. Agents exécutés : {', '.join(sub_results.keys())}."
        )
        if sub_results:
            try:
                agent_summaries = "\n".join(
                    f"- {name}: {res.get('summary', '(aucun résumé)')}"
                    for name, res in sub_results.items()
                )
                prompt = (
                    f"Résume en 2-3 phrases ces résultats d'agents pour la tâche : {task}\n\n"
                    f"Résumés des agents :\n{agent_summaries}\n\n"
                    "Réponds en texte simple uniquement."
                )
                msgs         = [{"role": "user", "content": prompt}]
                summary_line = llm_client.complete(msgs).strip()
            except Exception:
                pass  # Keep default summary_line

        logger.info(f"Summary: {summary_line[:200]}")
        return {
            **state,
            "final_answer": final_answer,
            "summary":      summary_line,
        }

    # ------------------------------------------------------------------ #
    #  Assemble the graph                                                  #
    # ------------------------------------------------------------------ #

    graph = StateGraph(GraphState)

    # Original agent nodes
    graph.add_node("plan",      plan_node)
    graph.add_node("analyst",   analyst_node)
    graph.add_node("quality",   quality_node)
    graph.add_node("pattern",   pattern_node)
    graph.add_node("query",     query_node)
    # ClickHouse specialist nodes
    graph.add_node("sql_analyst",              sql_analyst_node)
    graph.add_node("clickhouse_generic",       clickhouse_generic_node)
    graph.add_node("clickhouse_table_manager", clickhouse_table_manager_node)
    graph.add_node("clickhouse_writer",        clickhouse_writer_node)
    graph.add_node("clickhouse_specific",      clickhouse_specific_node)
    graph.add_node("text_to_sql_translator",   text_to_sql_node)
    # Aggregation
    graph.add_node("aggregate", aggregate_node)

    graph.set_entry_point("plan")

    _agent_targets = {
        # Original
        "analyst":   "analyst",
        "quality":   "quality",
        "pattern":   "pattern",
        "query":     "query",
        # ClickHouse specialists
        "sql_analyst":              "sql_analyst",
        "clickhouse_generic":       "clickhouse_generic",
        "clickhouse_table_manager": "clickhouse_table_manager",
        "clickhouse_writer":        "clickhouse_writer",
        "clickhouse_specific":      "clickhouse_specific",
        "text_to_sql_translator":   "text_to_sql_translator",
        # Terminal
        "aggregate": "aggregate",
    }

    # From plan_node: route to first agent or aggregate
    graph.add_conditional_edges("plan", route, _agent_targets)

    # From every agent node: route to next agent or aggregate
    for _node in [
        "analyst", "quality", "pattern", "query",
        "sql_analyst", "clickhouse_generic", "clickhouse_table_manager",
        "clickhouse_writer", "clickhouse_specific", "text_to_sql_translator",
    ]:
        graph.add_conditional_edges(_node, route, _agent_targets)

    graph.add_edge("aggregate", END)

    return graph.compile()


# --------------------------------------------------------------------------- #
#  Public convenience function                                                 #
# --------------------------------------------------------------------------- #

def run_graph(
    task:       str,
    llm_client: LLMClient,
    db_manager: DBManager,
    config:     dict,
    logger:     AgentLogger,
) -> Dict[str, Any]:
    """
    Run the full LangGraph pipeline for a task.

    Returns a result dict compatible with the existing ManagerAgent output:
    {
      "task", "answer", "summary", "sub_agents", "findings",
      "steps_used", "duration_s", "saved_to" (optional)
    }
    """
    compiled = create_agent_graph(llm_client, db_manager, config, logger)

    initial_state: GraphState = {
        "task":           task,
        "plan":           [],
        "agents_done":    [],
        "sub_results":    {},
        "shared_context": "",
        "final_answer":   "",
        "summary":        "",
        "error":          None,
    }

    t0          = time.time()
    final_state = compiled.invoke(initial_state)
    duration    = round(time.time() - t0, 2)

    # Build output compatible with manager_agent result format
    sub_agents: Dict[str, Any] = {}
    findings:   Dict[str, Any] = {}
    total_steps = 0

    for agent_name, agent_result in final_state.get("sub_results", {}).items():
        sub_agents[agent_name] = {
            "agent":    agent_name,
            "summary":  agent_result.get("summary", ""),
            "findings": agent_result.get("findings", {}),
            "steps":    agent_result.get("steps_used", 0),
        }
        total_steps += agent_result.get("steps_used", 0)
        for cat, items in agent_result.get("findings", {}).items():
            findings.setdefault(cat, []).extend(items)

    result = {
        "task":        task,
        "answer":      final_state.get("final_answer", ""),
        "summary":     final_state.get("summary", ""),
        "sub_agents":  sub_agents,
        "findings":    findings,
        "steps_used":  total_steps,
        "duration_s":  duration,
    }

    # Optionally save to disk
    result_dir = config.get("agents", {}).get("result_dir", "./results")
    try:
        os.makedirs(result_dir, exist_ok=True)
        ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
        path     = os.path.join(result_dir, f"result_graph_{ts}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"timestamp": ts, **result}, f,
                      indent=2, ensure_ascii=False, default=str)
        result["saved_to"] = path
        logger.info(f"Résultat LangGraph sauvegardé → {path}")
    except Exception as exc:
        logger.warn(f"Impossible de sauvegarder le résultat : {exc}")

    logger.final_answer(result["answer"], result["summary"])
    return result
