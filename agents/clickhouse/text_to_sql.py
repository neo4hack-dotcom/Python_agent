"""
Text-to-SQL Translator Agent — text_to_sql_translator
======================================================

Converts natural-language business questions into optimized ClickHouse SQL
via a semantic layer that encodes business vocabulary and column mappings.

Semantic layer
--------------
The semantic layer is a structured knowledge base injected at runtime:

  terms    — business term definitions
             e.g. "active_user": "user with ≥1 event in last 30 days"

  aliases  — logical name → physical column mapping
             e.g. "revenue": "amount_usd", "user": "uid"

  rules    — calculation rules for KPIs
             e.g. "arpu": "sum(revenue) / uniqHLL12(user_id)"

  dicts    — ClickHouse dictionaries available via dictGet()
             e.g. "geo_dict": "country_id → country_name, region"

Workflow
--------
1. nl_to_sql — translate the question using the semantic layer + schema
2. explain_query — preflight validation of generated SQL
3. execute_sql — run the validated query
4. store_finding — persist the result
5. final_answer — return result + generated SQL for auditability

Auto-correction
---------------
If execute_sql raises a ClickHouse error, the agent feeds the error
message back to the LLM to rewrite the SQL (at most 2 retries).
"""
from agents.clickhouse.base import ClickHouseBaseAgent
from utils.prompts import CH_TEXT_TO_SQL_MISSION


class TextToSQLAgent(ClickHouseBaseAgent):
    name           = "text_to_sql_translator"
    specialization = (
        "ClickHouse Text-to-SQL translator: converts plain-language questions "
        "into optimized SQL using a semantic layer, then validates and executes them."
    )
    mission = CH_TEXT_TO_SQL_MISSION
