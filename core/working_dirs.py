"""
core/working_dirs.py
====================
Gestion des répertoires de travail définis dans la configuration.

Permet de définir des répertoires auxquels les agents ont accès, avec un mode
d'accès configurable (lecture seule, écriture seule, ou lecture+écriture).

Configuration dans config.json :
  "working_directories": [
    {
      "path": "/home/user/data",
      "mode": "read",          // "read" | "write" | "readwrite"
      "description": "Données sources en lecture seule",
      "label": "data"          // identifiant court optionnel
    }
  ]
"""

import os
from typing import Dict, List, Optional, Tuple


# --------------------------------------------------------------------------- #
#  WorkingDirectory                                                            #
# --------------------------------------------------------------------------- #

class WorkingDirectory:
    """Représente un répertoire de travail avec ses permissions."""

    MODES = {"read", "write", "readwrite"}

    def __init__(
        self,
        path: str,
        mode: str = "read",
        description: str = "",
        label: str = "",
    ):
        self.path = os.path.abspath(os.path.expanduser(path))
        self.mode = mode.lower()
        if self.mode not in self.MODES:
            raise ValueError(
                f"Mode invalide '{mode}'. Valeurs acceptées: {self.MODES}"
            )
        self.description = description
        self.label = label or os.path.basename(self.path)

    @property
    def can_read(self) -> bool:
        return self.mode in ("read", "readwrite")

    @property
    def can_write(self) -> bool:
        return self.mode in ("write", "readwrite")

    def exists(self) -> bool:
        return os.path.isdir(self.path)

    def to_dict(self) -> Dict:
        return {
            "path": self.path,
            "mode": self.mode,
            "description": self.description,
            "label": self.label,
            "exists": self.exists(),
            "can_read": self.can_read,
            "can_write": self.can_write,
        }

    @classmethod
    def from_dict(cls, d: Dict) -> "WorkingDirectory":
        return cls(
            path=d["path"],
            mode=d.get("mode", "read"),
            description=d.get("description", ""),
            label=d.get("label", ""),
        )

    def __repr__(self) -> str:
        return f"WorkingDirectory(path={self.path!r}, mode={self.mode!r})"


# --------------------------------------------------------------------------- #
#  WorkingDirManager                                                           #
# --------------------------------------------------------------------------- #

class WorkingDirManager:
    """
    Gestionnaire des répertoires de travail configurés.
    Fournit des méthodes de validation d'accès pour les agents.
    """

    def __init__(self, config: Optional[Dict] = None):
        self._dirs: List[WorkingDirectory] = []
        if config:
            self.load_from_config(config)

    def load_from_config(self, config: Dict):
        """Charge les répertoires depuis la section 'working_directories' de la config."""
        raw = config.get("working_directories", [])
        self._dirs = []
        for d in raw:
            try:
                self._dirs.append(WorkingDirectory.from_dict(d))
            except (KeyError, ValueError):
                pass

    def list_directories(self) -> List[Dict]:
        """Retourne la liste de tous les répertoires de travail configurés."""
        return [d.to_dict() for d in self._dirs]

    def get_readable(self) -> List[WorkingDirectory]:
        """Retourne les répertoires accessibles en lecture."""
        return [d for d in self._dirs if d.can_read]

    def get_writable(self) -> List[WorkingDirectory]:
        """Retourne les répertoires accessibles en écriture."""
        return [d for d in self._dirs if d.can_write]

    def check_path_access(
        self, path: str, mode: str = "read"
    ) -> Tuple[bool, str]:
        """
        Vérifie si un chemin est accessible dans le mode demandé.

        Returns:
            (allowed: bool, reason: str)
        """
        abs_path = os.path.abspath(os.path.expanduser(path))

        for wd in self._dirs:
            # Check if the path is inside this working directory
            try:
                rel = os.path.relpath(abs_path, wd.path)
            except ValueError:
                # Different drive on Windows
                continue

            if not rel.startswith(".."):
                # Path is inside this working directory
                if mode == "read" and not wd.can_read:
                    return False, (
                        f"Accès en lecture refusé pour '{abs_path}'. "
                        f"Le répertoire '{wd.path}' est configuré en mode '{wd.mode}'."
                    )
                if mode == "write" and not wd.can_write:
                    return False, (
                        f"Accès en écriture refusé pour '{abs_path}'. "
                        f"Le répertoire '{wd.path}' est configuré en mode '{wd.mode}'."
                    )
                return True, f"Accès autorisé via '{wd.label}' ({wd.mode})"

        if not self._dirs:
            # No working directories configured — allow all (backward compat)
            return True, "Aucun répertoire de travail configuré (accès libre)"

        return False, (
            f"Accès refusé pour '{abs_path}'. "
            f"Ce chemin n'appartient à aucun répertoire de travail configuré."
        )

    def assert_readable(self, path: str):
        """Lève une PermissionError si le chemin n'est pas accessible en lecture."""
        allowed, reason = self.check_path_access(path, mode="read")
        if not allowed:
            raise PermissionError(reason)

    def assert_writable(self, path: str):
        """Lève une PermissionError si le chemin n'est pas accessible en écriture."""
        allowed, reason = self.check_path_access(path, mode="write")
        if not allowed:
            raise PermissionError(reason)

    def summary(self) -> str:
        """Retourne un résumé textuel des répertoires configurés."""
        if not self._dirs:
            return "Aucun répertoire de travail configuré."
        lines = ["Répertoires de travail:"]
        for d in self._dirs:
            status = "OK" if d.exists() else "MANQUANT"
            lines.append(
                f"  [{status}] {d.label} ({d.mode}) → {d.path}"
                + (f"  — {d.description}" if d.description else "")
            )
        return "\n".join(lines)
