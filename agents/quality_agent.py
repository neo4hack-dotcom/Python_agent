"""Quality Agent — audit de qualité des données."""
from agents.base_agent import BaseAgent
from utils.prompts import QUALITY_MISSION


class QualityAgent(BaseAgent):
    name           = "DataQuality"
    specialization = "data quality auditing — nulls, duplicates, outliers, consistency"
    mission        = QUALITY_MISSION
