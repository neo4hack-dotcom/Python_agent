"""Pattern Agent — découverte de patterns et corrélations."""
from agents.base_agent import BaseAgent
from utils.prompts import PATTERN_MISSION


class PatternAgent(BaseAgent):
    name           = "PatternDiscovery"
    specialization = "pattern discovery, correlations, anomaly detection, statistical mining"
    mission        = PATTERN_MISSION
