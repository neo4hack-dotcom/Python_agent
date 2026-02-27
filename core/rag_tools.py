"""
core/rag_tools.py
=================
Outils RAG (Retrieval-Augmented Generation) pour la recherche par similarité
dans des fichiers JSON.

Implémentation TF-IDF pure Python (stdlib uniquement) :
- Tokenisation et indexation de documents JSON
- Recherche par similarité cosinus
- Recherche exacte par clé/valeur
- Filtrage et pagination des résultats

Le fichier JSON peut être :
  - Une liste de dict : [{"id": 1, "text": "..."}, ...]
  - Un dict avec une clé de liste : {"items": [...], "records": [...]}
  - Un dict plat à indexer directement : {"key1": "val1", "key2": "val2"}
"""

import json
import math
import os
import re
from typing import Any, Callable, Dict, List, Optional, Tuple, TYPE_CHECKING

if TYPE_CHECKING:
    from core.memory import MemoryManager

# --------------------------------------------------------------------------- #
#  TF-IDF engine (stdlib pure)                                                 #
# --------------------------------------------------------------------------- #

def _tokenize(text: str) -> List[str]:
    """Tokenise du texte en mots minuscules (alphanumériques uniquement)."""
    return re.findall(r"[a-z0-9]+", text.lower())


def _flatten_record(record: Any, max_depth: int = 3, _depth: int = 0) -> str:
    """Aplatit récursivement un dict/list en chaîne de texte pour l'indexation."""
    if _depth > max_depth:
        return ""
    if isinstance(record, dict):
        parts = []
        for k, v in record.items():
            parts.append(str(k))
            parts.append(_flatten_record(v, max_depth, _depth + 1))
        return " ".join(parts)
    if isinstance(record, list):
        return " ".join(_flatten_record(v, max_depth, _depth + 1) for v in record)
    return str(record)


class TFIDFIndex:
    """Index TF-IDF minimaliste en Python pur."""

    def __init__(self):
        self._docs: List[Dict] = []          # documents originaux
        self._texts: List[str] = []          # textes aplatis
        self._tf: List[Dict[str, float]] = []  # tf par document
        self._idf: Dict[str, float] = {}     # idf global
        self._vocab: List[str] = []          # vocabulaire trié

    def build(self, documents: List[Dict]) -> int:
        """
        Construit l'index TF-IDF à partir d'une liste de documents.
        Retourne le nombre de documents indexés.
        """
        self._docs = documents
        self._texts = [_flatten_record(d) for d in documents]
        n = len(self._texts)

        if n == 0:
            return 0

        # Compute TF
        token_lists = [_tokenize(t) for t in self._texts]
        self._tf = []
        df: Dict[str, int] = {}

        for tokens in token_lists:
            total = len(tokens) or 1
            freq: Dict[str, int] = {}
            for tok in tokens:
                freq[tok] = freq.get(tok, 0) + 1
            tf = {tok: cnt / total for tok, cnt in freq.items()}
            self._tf.append(tf)
            for tok in freq:
                df[tok] = df.get(tok, 0) + 1

        # Compute IDF (log-smoothed)
        self._idf = {
            tok: math.log((n + 1) / (cnt + 1)) + 1.0
            for tok, cnt in df.items()
        }
        self._vocab = sorted(self._idf.keys())
        return n

    def _tfidf_vec(self, tf: Dict[str, float]) -> Dict[str, float]:
        return {tok: tf.get(tok, 0.0) * self._idf.get(tok, 0.0) for tok in self._vocab}

    def _cosine(self, a: Dict[str, float], b: Dict[str, float]) -> float:
        """Cosine similarity entre deux vecteurs sparse."""
        dot = sum(a.get(tok, 0.0) * b.get(tok, 0.0) for tok in a)
        norm_a = math.sqrt(sum(v * v for v in a.values()))
        norm_b = math.sqrt(sum(v * v for v in b.values()))
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return dot / (norm_a * norm_b)

    def search(
        self,
        query: str,
        top_k: int = 5,
        min_score: float = 0.0,
    ) -> List[Dict]:
        """
        Recherche par similarité cosinus TF-IDF.

        Returns:
            Liste de dicts {score, rank, document} triée par score décroissant.
        """
        if not self._docs:
            return []

        q_tokens = _tokenize(query)
        total = len(q_tokens) or 1
        q_freq: Dict[str, int] = {}
        for tok in q_tokens:
            q_freq[tok] = q_freq.get(tok, 0) + 1
        q_tf = {tok: cnt / total for tok, cnt in q_freq.items()}
        q_vec = self._tfidf_vec(q_tf)

        scores: List[Tuple[float, int]] = []
        for i, tf in enumerate(self._tf):
            doc_vec = self._tfidf_vec(tf)
            score = self._cosine(q_vec, doc_vec)
            if score >= min_score:
                scores.append((score, i))

        scores.sort(reverse=True)
        results = []
        for rank, (score, idx) in enumerate(scores[:top_k]):
            results.append({
                "rank": rank + 1,
                "score": round(score, 4),
                "document": self._docs[idx],
            })
        return results

    def __len__(self) -> int:
        return len(self._docs)


# --------------------------------------------------------------------------- #
#  JSON Loader                                                                 #
# --------------------------------------------------------------------------- #

def load_json_as_records(
    path: str,
    list_key: Optional[str] = None,
) -> List[Dict]:
    """
    Charge un fichier JSON et retourne une liste de records.

    Gère :
    - Une liste de dicts : [{"id": 1, ...}, ...]
    - Un dict avec une clé qui contient une liste : {"items": [...]}
    - Un dict plat : {"key": "val"} → [{"key": "val"}]

    Args:
        path     : Chemin vers le fichier JSON
        list_key : Clé à extraire si le JSON est un dict (auto-détecté si None)
    """
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    if isinstance(data, list):
        # Wrap non-dict items
        return [r if isinstance(r, dict) else {"value": r} for r in data]

    if isinstance(data, dict):
        if list_key:
            sub = data.get(list_key, [])
            if isinstance(sub, list):
                return [r if isinstance(r, dict) else {"value": r} for r in sub]

        # Auto-detect: find the first list value
        for k, v in data.items():
            if isinstance(v, list) and v:
                return [r if isinstance(r, dict) else {"value": r} for r in v]

        # Treat the whole dict as a single record
        return [data]

    # Scalar or unknown
    return [{"value": data}]


# --------------------------------------------------------------------------- #
#  Tool definitions                                                            #
# --------------------------------------------------------------------------- #

RAG_TOOL_DEFINITIONS: List[Dict] = [
    {
        "name": "rag_search",
        "description": (
            "Search for relevant records in the JSON knowledge base by semantic similarity. "
            "Uses TF-IDF cosine similarity. Returns the top-K most similar records."
        ),
        "params": {
            "query":     "Natural language query to search for",
            "top_k":     "Number of results to return (default: 5)",
            "min_score": "Minimum similarity score threshold 0.0-1.0 (default: 0.0)",
        },
        "required": ["query"],
    },
    {
        "name": "rag_get_by_key",
        "description": (
            "Retrieve records from the JSON knowledge base where a specific field "
            "matches a given value (exact match, case-insensitive)."
        ),
        "params": {
            "field": "Field/key name to filter on",
            "value": "Value to match (string, case-insensitive)",
            "limit": "Maximum number of records to return (default: 10)",
        },
        "required": ["field", "value"],
    },
    {
        "name": "rag_list_fields",
        "description": "List all distinct field names present in the JSON knowledge base.",
        "params": {},
        "required": [],
    },
    {
        "name": "rag_count",
        "description": "Return the total number of records in the JSON knowledge base.",
        "params": {},
        "required": [],
    },
    {
        "name": "rag_sample",
        "description": "Return a sample of records from the JSON knowledge base.",
        "params": {
            "n": "Number of records to sample (default: 3)",
        },
        "required": [],
    },
    {
        "name": "rag_filter",
        "description": (
            "Filter records from the JSON knowledge base using a simple expression. "
            "Expression format: 'field operator value' (operators: =, !=, >, <, contains). "
            "Example: 'category = electronics', 'price > 100', 'name contains python'."
        ),
        "params": {
            "expression": "Filter expression: 'field operator value'",
            "limit":      "Maximum number of records to return (default: 20)",
        },
        "required": ["expression"],
    },
    {
        "name": "store_finding",
        "description": "Store an important finding in semantic memory for later retrieval.",
        "params": {
            "key":        "Short identifier for this finding",
            "value":      "The finding content (text or structured data)",
            "category":   "Category: 'anomaly', 'pattern', 'metric', 'schema', 'finding'",
            "confidence": "Confidence score 0.0-1.0 (default 1.0)",
        },
        "required": ["key", "value"],
    },
    {
        "name": "recall_facts",
        "description": "Retrieve stored facts/findings from semantic memory.",
        "params": {
            "category": "Filter by category (optional)",
        },
        "required": [],
    },
    {
        "name": "think",
        "description": "Pure reasoning step — no external action. Use to plan or reason.",
        "params": {
            "reasoning": "Your detailed reasoning",
        },
        "required": ["reasoning"],
    },
    {
        "name": "final_answer",
        "description": "Emit the final answer/report when the task is complete.",
        "params": {
            "answer":  "The complete answer, analysis, or report",
            "summary": "One-sentence executive summary",
        },
        "required": ["answer"],
    },
]

RAG_TOOL_NAMES = {t["name"] for t in RAG_TOOL_DEFINITIONS}


# --------------------------------------------------------------------------- #
#  RAGToolExecutor                                                             #
# --------------------------------------------------------------------------- #

class RAGToolExecutor:
    """
    Exécuteur d'outils pour l'agent RAG JSON.
    Maintient un index TF-IDF en mémoire sur le fichier JSON configuré.
    """

    def __init__(
        self,
        json_path: str,
        memory: "MemoryManager",
        list_key: Optional[str] = None,
        dispatch_callback: Optional[Callable] = None,
    ):
        self.json_path = os.path.abspath(json_path)
        self.memory = memory
        self.list_key = list_key
        self.dispatch_callback = dispatch_callback
        self._index = TFIDFIndex()
        self._records: List[Dict] = []
        self._loaded = False

    def _ensure_loaded(self):
        """Charge et indexe le JSON au premier appel (lazy loading)."""
        if self._loaded:
            return
        if not os.path.exists(self.json_path):
            raise FileNotFoundError(
                f"Fichier JSON introuvable : {self.json_path}"
            )
        self._records = load_json_as_records(self.json_path, self.list_key)
        self._index.build(self._records)
        self._loaded = True

    def execute(self, action: str, params: Dict) -> Any:
        """Dispatch vers la bonne méthode d'outil."""
        try:
            if action == "rag_search":
                return self._tool_search(params)
            elif action == "rag_get_by_key":
                return self._tool_get_by_key(params)
            elif action == "rag_list_fields":
                return self._tool_list_fields()
            elif action == "rag_count":
                return self._tool_count()
            elif action == "rag_sample":
                return self._tool_sample(params)
            elif action == "rag_filter":
                return self._tool_filter(params)
            elif action == "store_finding":
                return self._store_finding(params)
            elif action == "recall_facts":
                return self._recall_facts(params)
            elif action == "think":
                return {"thought": params.get("reasoning", "")}
            elif action == "final_answer":
                return {"answer": params.get("answer", ""), "summary": params.get("summary", "")}
            else:
                return {"error": f"Outil inconnu: {action}"}
        except Exception as exc:
            return {"error": str(exc)}

    # ------------------------------------------------------------------ #
    #  RAG tools                                                           #
    # ------------------------------------------------------------------ #

    def _tool_search(self, params: Dict) -> Dict:
        self._ensure_loaded()
        query = params.get("query", "")
        top_k = int(params.get("top_k", 5))
        min_score = float(params.get("min_score", 0.0))

        if not query:
            return {"error": "Le paramètre 'query' est requis."}

        results = self._index.search(query, top_k=top_k, min_score=min_score)
        return {
            "query": query,
            "total_indexed": len(self._index),
            "results_count": len(results),
            "results": results,
        }

    def _tool_get_by_key(self, params: Dict) -> Dict:
        self._ensure_loaded()
        field = params.get("field", "")
        value = str(params.get("value", "")).lower()
        limit = int(params.get("limit", 10))

        if not field:
            return {"error": "Le paramètre 'field' est requis."}

        matches = []
        for rec in self._records:
            rec_val = str(rec.get(field, "")).lower()
            if rec_val == value:
                matches.append(rec)
                if len(matches) >= limit:
                    break

        return {
            "field": field,
            "value": params.get("value"),
            "matched": len(matches),
            "results": matches,
        }

    def _tool_list_fields(self) -> Dict:
        self._ensure_loaded()
        fields: set = set()
        for rec in self._records:
            if isinstance(rec, dict):
                fields.update(rec.keys())
        return {"fields": sorted(fields), "total_records": len(self._records)}

    def _tool_count(self) -> Dict:
        self._ensure_loaded()
        return {
            "total_records": len(self._records),
            "json_path": self.json_path,
        }

    def _tool_sample(self, params: Dict) -> Dict:
        self._ensure_loaded()
        n = int(params.get("n", 3))
        return {
            "sample_size": min(n, len(self._records)),
            "records": self._records[:n],
        }

    def _tool_filter(self, params: Dict) -> Dict:
        self._ensure_loaded()
        expr = params.get("expression", "").strip()
        limit = int(params.get("limit", 20))

        if not expr:
            return {"error": "Le paramètre 'expression' est requis."}

        # Parse expression: "field operator value"
        # Operators: =, !=, >, <, >=, <=, contains
        pattern = re.match(
            r"^(\w+)\s*(>=|<=|!=|>|<|=|contains)\s*(.+)$", expr, re.IGNORECASE
        )
        if not pattern:
            return {
                "error": (
                    f"Expression invalide: '{expr}'. "
                    "Format attendu: 'field operator value' "
                    "(opérateurs: =, !=, >, <, >=, <=, contains)"
                )
            }

        field, op, value = pattern.group(1), pattern.group(2).lower(), pattern.group(3).strip()

        # Strip quotes
        if (value.startswith('"') and value.endswith('"')) or \
           (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]

        results = []
        for rec in self._records:
            raw = rec.get(field)
            if raw is None:
                continue
            try:
                if op == "contains":
                    if value.lower() in str(raw).lower():
                        results.append(rec)
                elif op == "=":
                    if str(raw).lower() == value.lower():
                        results.append(rec)
                elif op == "!=":
                    if str(raw).lower() != value.lower():
                        results.append(rec)
                elif op == ">":
                    if float(raw) > float(value):
                        results.append(rec)
                elif op == "<":
                    if float(raw) < float(value):
                        results.append(rec)
                elif op == ">=":
                    if float(raw) >= float(value):
                        results.append(rec)
                elif op == "<=":
                    if float(raw) <= float(value):
                        results.append(rec)
            except (ValueError, TypeError):
                continue

            if len(results) >= limit:
                break

        return {
            "expression": expr,
            "matched": len(results),
            "results": results,
        }

    # ------------------------------------------------------------------ #
    #  Memory tools (re-used from base ToolExecutor)                       #
    # ------------------------------------------------------------------ #

    def _store_finding(self, params: Dict) -> Dict:
        key = params.get("key", "")
        value = params.get("value", "")
        category = params.get("category", "finding")
        confidence = float(params.get("confidence", 1.0))
        self.memory.store_semantic(key, value, category, confidence)
        return {"stored": key, "category": category}

    def _recall_facts(self, params: Dict) -> Dict:
        category = params.get("category")
        facts = self.memory.get_semantic_facts(category)
        return {"facts": facts, "count": len(facts)}
