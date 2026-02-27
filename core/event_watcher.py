"""
core/event_watcher.py
=====================
Surveillance de répertoires pour déclencher des prompts sur événements filesystem.

Fonctionnalités :
- Surveillance de dossiers (nouveaux fichiers, modifications)
- Filtrage par extension ou pattern glob
- Déclenchement de prompts ou chaînes de prompts
- Cooldown pour éviter les déclenchements multiples
- Polling pur stdlib (pas d'inotify) pour portabilité maximale

Types d'événements :
  - new_file       : Nouveau fichier détecté dans le dossier surveillé
  - modified_file  : Fichier existant modifié (mtime a changé)
"""

import fnmatch
import json
import os
import threading
import time
import uuid
from datetime import datetime
from typing import Callable, Dict, List, Optional


# --------------------------------------------------------------------------- #
#  FolderWatcher — polling filesystem                                          #
# --------------------------------------------------------------------------- #

class FolderWatcher:
    """
    Surveille un dossier et déclenche un callback sur nouveaux fichiers / modifications.
    Utilise un polling périodique (compatible Windows, Linux, macOS).
    """

    def __init__(
        self,
        path: str,
        patterns: Optional[List[str]] = None,   # ex: ["*.json", "*.csv"], défaut: ["*"]
        recursive: bool = False,
        poll_interval: float = 5.0,             # secondes entre deux scans
        cooldown: float = 2.0,                  # délai avant de traiter un fichier
    ):
        self.path = os.path.abspath(path)
        self.patterns = patterns or ["*"]
        self.recursive = recursive
        self.poll_interval = poll_interval
        self.cooldown = cooldown
        self._known: Dict[str, float] = {}      # filepath → mtime
        self._pending: Dict[str, float] = {}    # filepath → time_detected
        self._callbacks: List[Callable] = []
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

    def _matches(self, filename: str) -> bool:
        return any(fnmatch.fnmatch(filename, pat) for pat in self.patterns)

    def _scan(self) -> Dict[str, float]:
        """Retourne {filepath: mtime} pour tous les fichiers matchant les patterns."""
        result: Dict[str, float] = {}
        try:
            if self.recursive:
                for root, _dirs, files in os.walk(self.path):
                    for fname in files:
                        if self._matches(fname):
                            fp = os.path.join(root, fname)
                            try:
                                result[fp] = os.path.getmtime(fp)
                            except OSError:
                                pass
            else:
                with os.scandir(self.path) as it:
                    for entry in it:
                        if entry.is_file() and self._matches(entry.name):
                            try:
                                result[entry.path] = entry.stat().st_mtime
                            except OSError:
                                pass
        except (PermissionError, FileNotFoundError):
            pass
        return result

    def on_new_file(self, callback: Callable) -> "FolderWatcher":
        """
        Enregistre un callback sur nouveaux fichiers / modifications.
        Signature: callback(filepath: str, event: dict)
        """
        self._callbacks.append(callback)
        return self

    def _fire(self, filepath: str, event_type: str):
        event = {
            "type": event_type,
            "filepath": filepath,
            "filename": os.path.basename(filepath),
            "directory": os.path.dirname(filepath),
            "detected_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        }
        for cb in self._callbacks:
            try:
                cb(filepath, event)
            except Exception:
                pass

    def _tick(self):
        """Un cycle de scan : détecte les changements et fire les events."""
        current = self._scan()
        now = time.time()

        # Detect new / modified files
        for fp, mtime in current.items():
            if fp not in self._known:
                with self._lock:
                    self._pending[fp] = now
            elif mtime > self._known.get(fp, 0):
                with self._lock:
                    self._pending[fp] = now

        # Fire events for files that have passed the cooldown
        with self._lock:
            ready = [fp for fp, t in self._pending.items() if now - t >= self.cooldown]
        for fp in ready:
            with self._lock:
                self._pending.pop(fp, None)
            if fp in current:
                event_type = "new_file" if fp not in self._known else "modified_file"
                self._fire(fp, event_type)

        self._known = current

    def start(self) -> "FolderWatcher":
        """Lance la surveillance en arrière-plan (thread daemon)."""
        if self._thread and self._thread.is_alive():
            return self
        # Snapshot initial pour ne pas déclencher sur les fichiers déjà existants
        self._known = self._scan()
        self._stop_event.clear()

        def _loop():
            while not self._stop_event.is_set():
                try:
                    self._tick()
                except Exception:
                    pass
                self._stop_event.wait(self.poll_interval)

        self._thread = threading.Thread(
            target=_loop,
            name=f"FolderWatcher:{self.path}",
            daemon=True,
        )
        self._thread.start()
        return self

    def stop(self):
        """Arrête la surveillance."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=10)

    def is_running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())


# --------------------------------------------------------------------------- #
#  EventTrigger — associe un FolderWatcher à des prompts                      #
# --------------------------------------------------------------------------- #

class EventTrigger:
    """Associe un événement filesystem à une chaîne de prompts."""

    def __init__(
        self,
        trigger_id: str,
        name: str,
        watch_path: str,
        patterns: List[str],
        prompts: List[Dict],            # liste de PromptChainItem.to_dict()
        agent: str = "manager",
        recursive: bool = False,
        poll_interval: float = 10.0,
        cooldown: float = 3.0,
        enabled: bool = True,
        inject_filepath: bool = True,   # {filepath}, {filename}, {directory} disponibles
        event_types: Optional[List[str]] = None,  # ["new_file"] | ["modified_file"] | None=all
    ):
        self.trigger_id = trigger_id
        self.name = name
        self.watch_path = watch_path
        self.patterns = patterns
        self.prompts = prompts
        self.agent = agent
        self.recursive = recursive
        self.poll_interval = poll_interval
        self.cooldown = cooldown
        self.enabled = enabled
        self.inject_filepath = inject_filepath
        self.event_types = event_types or ["new_file"]
        self._watcher: Optional[FolderWatcher] = None
        self._run_count = 0

    def to_dict(self) -> Dict:
        return {
            "trigger_id": self.trigger_id,
            "name": self.name,
            "watch_path": self.watch_path,
            "patterns": self.patterns,
            "prompts": self.prompts,
            "agent": self.agent,
            "recursive": self.recursive,
            "poll_interval": self.poll_interval,
            "cooldown": self.cooldown,
            "enabled": self.enabled,
            "inject_filepath": self.inject_filepath,
            "event_types": self.event_types,
            "run_count": self._run_count,
        }

    @classmethod
    def from_dict(cls, d: Dict) -> "EventTrigger":
        t = cls(
            trigger_id=d.get("trigger_id", str(uuid.uuid4())[:8]),
            name=d.get("name", "unnamed"),
            watch_path=d["watch_path"],
            patterns=d.get("patterns", ["*"]),
            prompts=d.get("prompts", []),
            agent=d.get("agent", "manager"),
            recursive=d.get("recursive", False),
            poll_interval=d.get("poll_interval", 10.0),
            cooldown=d.get("cooldown", 3.0),
            enabled=d.get("enabled", True),
            inject_filepath=d.get("inject_filepath", True),
            event_types=d.get("event_types", ["new_file"]),
        )
        t._run_count = d.get("run_count", 0)
        return t


# --------------------------------------------------------------------------- #
#  EventWatcherManager                                                         #
# --------------------------------------------------------------------------- #

TRIGGERS_FILE = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "event_triggers.json"
)


class EventWatcherManager:
    """Gère l'ensemble des triggers d'événements filesystem avec persistance."""

    def __init__(
        self,
        triggers_file: str = TRIGGERS_FILE,
        run_task_fn: Optional[Callable] = None,
        config: Optional[Dict] = None,
    ):
        self.triggers_file = triggers_file
        self._run_task_fn = run_task_fn
        self._config = config
        self._triggers: Dict[str, EventTrigger] = {}
        self._on_trigger_fire: Optional[Callable] = None
        self._on_trigger_done: Optional[Callable] = None
        self._load()

    def setup(self, run_task_fn: Callable, config: Dict):
        """Configure la fonction d'exécution et la configuration."""
        self._run_task_fn = run_task_fn
        self._config = config

    # ------------------------------------------------------------------ #
    #  Persistence                                                         #
    # ------------------------------------------------------------------ #

    def _load(self):
        if os.path.exists(self.triggers_file):
            try:
                with open(self.triggers_file, encoding="utf-8") as f:
                    data = json.load(f)
                for d in data:
                    t = EventTrigger.from_dict(d)
                    self._triggers[t.trigger_id] = t
            except (json.JSONDecodeError, IOError):
                pass

    def _save(self):
        directory = os.path.dirname(self.triggers_file)
        if directory:
            os.makedirs(directory, exist_ok=True)
        data = [t.to_dict() for t in self._triggers.values()]
        with open(self.triggers_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    # ------------------------------------------------------------------ #
    #  Trigger management                                                  #
    # ------------------------------------------------------------------ #

    def add_trigger(self, trigger: EventTrigger) -> EventTrigger:
        """Ajoute ou remplace un trigger."""
        self._triggers[trigger.trigger_id] = trigger
        self._save()
        return trigger

    def remove_trigger(self, trigger_id: str) -> bool:
        """Supprime un trigger et arrête son watcher. Retourne True si supprimé."""
        t = self._triggers.get(trigger_id)
        if t:
            if t._watcher:
                t._watcher.stop()
            del self._triggers[trigger_id]
            self._save()
            return True
        return False

    def enable_trigger(self, trigger_id: str, enabled: bool = True):
        """Active ou désactive un trigger."""
        t = self._triggers.get(trigger_id)
        if t:
            t.enabled = enabled
            self._save()

    def list_triggers(self) -> List[Dict]:
        return [t.to_dict() for t in self._triggers.values()]

    def on_trigger_fire(self, callback: Callable) -> "EventWatcherManager":
        """callback(trigger_dict, event_dict) quand un trigger est déclenché."""
        self._on_trigger_fire = callback
        return self

    def on_trigger_done(self, callback: Callable) -> "EventWatcherManager":
        """callback(trigger_dict, event_dict, results) quand les prompts sont terminés."""
        self._on_trigger_done = callback
        return self

    # ------------------------------------------------------------------ #
    #  Start / stop all watchers                                           #
    # ------------------------------------------------------------------ #

    def start_all(self):
        """Démarre tous les triggers actifs."""
        for trigger in self._triggers.values():
            if trigger.enabled:
                self._start_trigger(trigger)

    def stop_all(self):
        """Arrête tous les watchers actifs."""
        for trigger in self._triggers.values():
            if trigger._watcher:
                trigger._watcher.stop()

    def _start_trigger(self, trigger: EventTrigger):
        """Initialise et démarre le FolderWatcher pour un trigger."""
        watcher = FolderWatcher(
            path=trigger.watch_path,
            patterns=trigger.patterns,
            recursive=trigger.recursive,
            poll_interval=trigger.poll_interval,
            cooldown=trigger.cooldown,
        )

        def _on_file(filepath: str, event: Dict):
            if not trigger.enabled:
                return
            # Filtre sur le type d'événement
            if trigger.event_types and event.get("type") not in trigger.event_types:
                return
            # Exécution dans un thread séparé pour ne pas bloquer le watcher
            t = threading.Thread(
                target=self._handle_event,
                args=(trigger, filepath, event),
                daemon=True,
            )
            t.start()

        watcher.on_new_file(_on_file)
        watcher.start()
        trigger._watcher = watcher

    def _handle_event(self, trigger: EventTrigger, filepath: str, event: Dict):
        """Exécute les prompts d'un trigger en réponse à un événement."""
        if not self._run_task_fn or not self._config:
            return

        from core.prompt_queue import PromptQueue, PromptChainItem

        if self._on_trigger_fire:
            self._on_trigger_fire(trigger.to_dict(), event)

        # Build prompts avec injection du chemin de fichier
        pq = PromptQueue()
        for item_def in trigger.prompts:
            item = PromptChainItem.from_dict(item_def)
            if trigger.inject_filepath:
                item.variables.setdefault("filepath", filepath)
                item.variables.setdefault("filename", os.path.basename(filepath))
                item.variables.setdefault("directory", os.path.dirname(filepath))
            pq.add(item)

        results = pq.run(self._run_task_fn, self._config)
        trigger._run_count += 1
        self._save()

        if self._on_trigger_done:
            self._on_trigger_done(trigger.to_dict(), event, results)
