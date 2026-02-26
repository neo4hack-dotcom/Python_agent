"""Analyst Agent — analyse de données approfondie."""
from agents.base_agent import BaseAgent
from utils.prompts import ANALYST_MISSION


class AnalystAgent(BaseAgent):
    name           = "DataAnalyst"
    specialization = "deep data analysis, statistics, and trend detection"
    mission        = ANALYST_MISSION
