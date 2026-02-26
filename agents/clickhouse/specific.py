"""
Template Executor Agent — clickhouse_specific
=============================================

Executes parameterized ClickHouse query templates for recurring reports.

Built-in templates
------------------
  P1 — daily_active_users    : DAU with WITH FILL gap-completion
  P2 — funnel_conversion     : windowFunnel() multi-step conversion
  P3 — retention_cohort      : Weekly cohort Day-N retention
  P4 — top_events            : Top-K events by frequency with share %

Custom templates can be added in config.json under
  clickhouse_agents.templates.<TEMPLATE_ID>:
    name, description, params, sql

Workflow
--------
1. list_templates — inspect available templates and their parameters
2. execute_template — run the chosen template with substituted params
3. store_finding — persist key metrics from template results
4. final_answer — return formatted report
"""
from agents.clickhouse.base import ClickHouseBaseAgent
from utils.prompts import CH_SPECIFIC_MISSION


class ClickHouseSpecificAgent(ClickHouseBaseAgent):
    name           = "clickhouse_specific"
    specialization = (
        "ClickHouse template executor: runs parameterized recurring reports "
        "(DAU, funnels, cohort retention, top events) with zero SQL authoring."
    )
    mission = CH_SPECIFIC_MISSION
