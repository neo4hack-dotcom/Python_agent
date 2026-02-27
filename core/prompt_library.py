"""
core/prompt_library.py
======================
Bibliothèque de prompts sauvegardés avec persistance JSON.

Fonctionnalités :
- Sauvegarder / mettre à jour des prompts nommés
- Support des variables {var} dans les prompts
- Tags pour catégoriser et filtrer
- Compteur d'exécutions et historique
- Recherche plein texte
"""

import json
import os
import time
from typing import Dict, List, Optional

LIBRARY_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "prompts_library.json")


class PromptLibrary:
    """Bibliothèque de prompts persistants (stockés dans prompts_library.json)."""

    def __init__(self, library_file: str = LIBRARY_FILE):
        self.library_file = library_file
        self._prompts: Dict[str, Dict] = {}
        self._load()

    # ------------------------------------------------------------------ #
    #  Persistence                                                         #
    # ------------------------------------------------------------------ #

    def _load(self):
        """Charge la bibliothèque depuis le fichier JSON."""
        if os.path.exists(self.library_file):
            try:
                with open(self.library_file, encoding="utf-8") as f:
                    self._prompts = json.load(f)
            except (json.JSONDecodeError, IOError):
                self._prompts = {}
        else:
            self._prompts = {}

    def _save(self):
        """Persiste la bibliothèque dans le fichier JSON."""
        directory = os.path.dirname(self.library_file)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(self.library_file, "w", encoding="utf-8") as f:
            json.dump(self._prompts, f, indent=2, ensure_ascii=False)

    # ------------------------------------------------------------------ #
    #  CRUD                                                                #
    # ------------------------------------------------------------------ #

    def save(
        self,
        name: str,
        prompt: str,
        description: str = "",
        agent: str = "manager",
        tags: Optional[List[str]] = None,
        variables: Optional[Dict[str, str]] = None,
    ) -> Dict:
        """
        Sauvegarde ou met à jour un prompt dans la bibliothèque.

        Args:
            name        : Identifiant unique du prompt (ex: "audit_quality")
            prompt      : Texte du prompt (peut contenir des variables {var})
            description : Description courte
            agent       : Agent par défaut à utiliser
            tags        : Liste de tags pour filtrer/catégoriser
            variables   : Variables par défaut du prompt {nom: valeur_défaut}
        """
        entry = {
            "name": name,
            "prompt": prompt,
            "description": description,
            "agent": agent,
            "tags": tags or [],
            "variables": variables or {},
            "created_at": self._prompts.get(name, {}).get(
                "created_at", time.strftime("%Y-%m-%dT%H:%M:%S")
            ),
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "run_count": self._prompts.get(name, {}).get("run_count", 0),
            "last_run_at": self._prompts.get(name, {}).get("last_run_at"),
        }
        self._prompts[name] = entry
        self._save()
        return entry

    def get(self, name: str) -> Optional[Dict]:
        """Récupère un prompt par son nom. Retourne None si inexistant."""
        return self._prompts.get(name)

    def delete(self, name: str) -> bool:
        """Supprime un prompt. Retourne True si supprimé, False si inexistant."""
        if name in self._prompts:
            del self._prompts[name]
            self._save()
            return True
        return False

    def list_all(self, tag: Optional[str] = None) -> List[Dict]:
        """
        Liste tous les prompts, optionnellement filtrés par tag.
        Retourne une liste triée par nom.
        """
        prompts = list(self._prompts.values())
        if tag:
            prompts = [p for p in prompts if tag in p.get("tags", [])]
        return sorted(prompts, key=lambda p: p["name"])

    def render(self, name: str, variables: Optional[Dict[str, str]] = None) -> str:
        """
        Applique les variables à un prompt et retourne le texte final.
        Les variables dans le prompt sont au format {nom}.
        Les variables passées en argument écrasent les valeurs par défaut.

        Raises:
            KeyError: Si le prompt n'existe pas dans la bibliothèque.
        """
        entry = self.get(name)
        if not entry:
            raise KeyError(f"Prompt '{name}' introuvable dans la bibliothèque.")
        defaults = entry.get("variables", {})
        merged = {**defaults, **(variables or {})}
        text = entry["prompt"]
        for key, val in merged.items():
            text = text.replace(f"{{{key}}}", str(val))
        return text

    def increment_run_count(self, name: str):
        """Incrémente le compteur d'exécutions d'un prompt."""
        if name in self._prompts:
            self._prompts[name]["run_count"] = self._prompts[name].get("run_count", 0) + 1
            self._prompts[name]["last_run_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
            self._save()

    def search(self, query: str) -> List[Dict]:
        """Recherche des prompts par texte (dans nom, description, prompt, tags)."""
        q = query.lower()
        results = []
        for p in self._prompts.values():
            if (
                q in p["name"].lower()
                or q in p.get("description", "").lower()
                or q in p.get("prompt", "").lower()
                or any(q in t.lower() for t in p.get("tags", []))
            ):
                results.append(p)
        return sorted(results, key=lambda p: p["name"])

    def list_tags(self) -> List[str]:
        """Retourne la liste de tous les tags utilisés (triée, dédupliquée)."""
        tags: set = set()
        for p in self._prompts.values():
            tags.update(p.get("tags", []))
        return sorted(tags)

    def __len__(self) -> int:
        return len(self._prompts)

    def __contains__(self, name: str) -> bool:
        return name in self._prompts
