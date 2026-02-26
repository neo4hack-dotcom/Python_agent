"""Query Agent — construction et optimisation de requêtes SQL."""
from agents.base_agent import BaseAgent
from utils.prompts import QUERY_MISSION


class QueryAgent(BaseAgent):
    name           = "SQLQueryBuilder"
    specialization = "SQL query design and optimization for ClickHouse and Oracle"
    mission        = QUERY_MISSION
