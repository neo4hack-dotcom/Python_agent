# AI Manager Agent — Analyse autonome ClickHouse / Oracle

Agent IA autonome en Python pur pour l'analyse de données, la détection de patterns et l'audit qualité.
Fonctionne sur n'importe quel PC Windows **sans installation complexe**.

## Prérequis

- Python 3.8+
- Un LLM local exposé en HTTP (Ollama, LM Studio, vLLM, text-generation-webui…)
- ClickHouse et/ou Oracle accessibles en réseau

## Installation

```bash
pip install colorama          # couleurs terminal (optionnel)
pip install python-oracledb   # uniquement si Oracle est utilisé
```

> ClickHouse ne nécessite **aucun driver** : l'app utilise directement l'API HTTP native (port 8123).

## Configuration

Éditez `config.json` :

```json
{
  "llm": {
    "api_type":  "ollama",
    "base_url":  "http://localhost:11434",
    "model":     "llama3.1:8b",
    "temperature": 0.1,
    "max_tokens": 4096
  },
  "databases": {
    "clickhouse": {
      "enabled": true,
      "host": "localhost",
      "port": 8123,
      "database": "default"
    },
    "oracle": {
      "enabled": false,
      "host": "localhost",
      "port": 1521,
      "service_name": "ORCL",
      "thick_mode": false
    }
  }
}
```

## Utilisation

```bash
# Tâche en ligne de commande
python main.py "Analyse la qualité des données dans ma base ClickHouse"

# Spécifier l'agent
python main.py --task "Audit complet" --agent quality
python main.py --task "Trouve les patterns dans events" --agent pattern

# Mode interactif
python main.py --interactive

# Vérifier les connexions
python main.py --check-connections

# Lister les outils disponibles
python main.py --list-tools

# Sauvegarder le résultat
python main.py "Mon analyse" --output result.json
```

## Architecture

```
main.py                     Point d'entree CLI
config.json                 Configuration unique

core/
  llm_client.py             Client HTTP LLM (Ollama + OpenAI-compat, stdlib pur)
  db_manager.py             ClickHouse HTTP + Oracle thin mode
  memory.py                 Memoire hierarchique (working / episodique / semantique)
  tools.py                  Registre + executeur d'outils
  engine.py                 Boucle ReAct (Reason -> Act -> Observe -> Reflect)

agents/
  manager_agent.py          Orchestrateur principal
  analyst_agent.py          Analyse statistique et tendances
  quality_agent.py          Audit qualite (nulls, doublons, outliers)
  pattern_agent.py          Decouverte de patterns et correlations
  query_agent.py            Construction et optimisation SQL
  base_agent.py             Classe de base commune

utils/
  logger.py                 Logging colore Windows/Linux
  prompts.py                Templates de prompts par agent

results/                    Resultats sauvegardes (JSON + logs)
```

## Fonctionnalites cles

| Feature | Detail |
|---------|--------|
| **15+ steps** | Memoire hierarchique : working / episodique / semantique |
| **Zero driver CH** | ClickHouse via HTTP natif (urllib stdlib) |
| **Oracle thin** | python-oracledb sans client Oracle requis |
| **LLM universel** | Ollama, LM Studio, vLLM, OpenAI-compatible |
| **ReAct loop** | Thought -> Action -> Observation -> Reflexion |
| **Auto-reflexion** | Pause reflexive tous les 5 steps |
| **Anti-boucle** | Detection et correction des actions repetees |
| **Cache requetes** | TTL 5min sur les resultats de requetes |
| **Securite** | Blocage write par defaut, limite de lignes |
| **Multi-agents** | Manager dispatche analyst/quality/pattern/query |

## Use cases

- **Analyse autonome** : `"Explore la table orders et donne les metriques cles"`
- **Data Quality** : `"Audit complet de la qualite des donnees de la base default"`
- **Pattern mining** : `"Trouve les anomalies et patterns inhabituels dans events"`
- **Rapport** : `"Genere un rapport de data quality pour la table users"`

## Outils disponibles pour les agents

- `execute_sql` — Requete SELECT sur ClickHouse ou Oracle
- `list_tables` — Liste des tables
- `describe_table` — Schema d'une table
- `get_sample` — Echantillon de donnees
- `compute_stats` — Statistiques (count, nulls, min, max, avg, stddev)
- `detect_nulls` — Taux de valeurs nulles par colonne
- `detect_duplicates` — Lignes dupliquees par cle
- `detect_outliers` — Valeurs aberrantes (methode IQR)
- `store_finding` — Persiste un fait en memoire semantique
- `recall_facts` — Recupere les faits stockes
- `dispatch_agent` — Lance un sous-agent specialise
- `think` — Raisonnement pur sans action externe
- `final_answer` — Reponse finale
