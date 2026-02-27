"""
core/scheduler.py
=================
Planification de prompts (ou chaînes de prompts) avec expressions cron.

Utilise uniquement la stdlib Python (threading, time) — aucune dépendance externe.

Types de planification supportés :
  - cron   : expression cron simplifiée "minute heure jour_du_mois mois jour_de_la_semaine"
  - interval: exécution toutes les N secondes
  - once   : exécution unique à une date/heure précise ("YYYY-MM-DD HH:MM:SS")
"""

import json
import os
import threading
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional


# --------------------------------------------------------------------------- #
#  Cron parser (simplifié, stdlib pure)                                        #
# --------------------------------------------------------------------------- #

def _parse_cron_field(field: str, min_val: int, max_val: int) -> List[int]:
    """Parse un champ cron : *, N, N-M, N,M,..., */N"""
    if field == "*":
        return list(range(min_val, max_val + 1))
    if field.startswith("*/"):
        step = int(field[2:])
        return list(range(min_val, max_val + 1, step))
    if "," in field:
        return [int(x) for x in field.split(",")]
    if "-" in field:
        a, b = field.split("-", 1)
        return list(range(int(a), int(b) + 1))
    return [int(field)]


def cron_matches(expr: str, dt: datetime) -> bool:
    """
    Vérifie si une expression cron correspond à un datetime.
    Format: "minute heure jour_du_mois mois jour_de_la_semaine"
    Exemples:
      "0 9 * * 1-5"   → tous les jours de semaine à 9h00
      "*/30 * * * *"  → toutes les 30 minutes
      "0 0 1 * *"     → le 1er de chaque mois à minuit
    """
    parts = expr.strip().split()
    if len(parts) != 5:
        raise ValueError(f"Expression cron invalide (5 champs requis): '{expr}'")
    minutes, hours, mdays, months, wdays = parts
    return (
        dt.minute in _parse_cron_field(minutes, 0, 59)
        and dt.hour in _parse_cron_field(hours, 0, 23)
        and dt.day in _parse_cron_field(mdays, 1, 31)
        and dt.month in _parse_cron_field(months, 1, 12)
        and dt.weekday() in [w % 7 for w in _parse_cron_field(wdays, 0, 6)]
    )


# --------------------------------------------------------------------------- #
#  ScheduledJob                                                                #
# --------------------------------------------------------------------------- #

class ScheduledJob:
    """Représente un job planifié (prompt unique ou chaîne de prompts)."""

    def __init__(
        self,
        job_id: str,
        name: str,
        prompts: List[Dict],        # liste de PromptChainItem.to_dict()
        schedule_type: str,         # "cron" | "interval" | "once"
        schedule_value: str,        # expr cron | nb secondes | "YYYY-MM-DD HH:MM:SS"
        agent: str = "manager",
        enabled: bool = True,
        max_runs: int = 0,          # 0 = illimité
        config_override: Optional[Dict] = None,
    ):
        self.job_id = job_id
        self.name = name
        self.prompts = prompts
        self.schedule_type = schedule_type
        self.schedule_value = schedule_value
        self.agent = agent
        self.enabled = enabled
        self.max_runs = max_runs
        self.config_override = config_override or {}
        self.run_count = 0
        self.last_run: Optional[datetime] = None
        self.next_run: Optional[datetime] = None
        self._compute_next_run()

    def _compute_next_run(self, from_dt: Optional[datetime] = None):
        """Calcule le prochain instant d'exécution."""
        now = from_dt or datetime.now()

        if self.schedule_type == "once":
            try:
                self.next_run = datetime.strptime(self.schedule_value, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                self.next_run = None

        elif self.schedule_type == "interval":
            try:
                seconds = int(self.schedule_value)
                self.next_run = (self.last_run or now) + timedelta(seconds=seconds)
            except ValueError:
                self.next_run = None

        elif self.schedule_type == "cron":
            # Cherche le prochain instant (résolution à la minute, max 1 semaine)
            candidate = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
            for _ in range(60 * 24 * 7):
                try:
                    if cron_matches(self.schedule_value, candidate):
                        self.next_run = candidate
                        return
                except ValueError:
                    break
                candidate += timedelta(minutes=1)
            self.next_run = None
        else:
            self.next_run = None

    def is_due(self, dt: Optional[datetime] = None) -> bool:
        """Retourne True si le job doit être lancé maintenant."""
        if not self.enabled:
            return False
        if self.max_runs > 0 and self.run_count >= self.max_runs:
            return False
        if self.next_run is None:
            return False
        now = dt or datetime.now()
        return now >= self.next_run

    def mark_ran(self):
        """Met à jour les compteurs après une exécution."""
        self.last_run = datetime.now()
        self.run_count += 1
        if self.schedule_type == "once":
            self.enabled = False
            self.next_run = None
        else:
            self._compute_next_run(from_dt=self.last_run)

    def to_dict(self) -> Dict:
        return {
            "job_id": self.job_id,
            "name": self.name,
            "prompts": self.prompts,
            "schedule_type": self.schedule_type,
            "schedule_value": self.schedule_value,
            "agent": self.agent,
            "enabled": self.enabled,
            "max_runs": self.max_runs,
            "config_override": self.config_override,
            "run_count": self.run_count,
            "last_run": self.last_run.strftime("%Y-%m-%d %H:%M:%S") if self.last_run else None,
            "next_run": self.next_run.strftime("%Y-%m-%d %H:%M:%S") if self.next_run else None,
        }

    @classmethod
    def from_dict(cls, d: Dict) -> "ScheduledJob":
        job = cls(
            job_id=d.get("job_id", str(uuid.uuid4())[:8]),
            name=d.get("name", "unnamed"),
            prompts=d.get("prompts", []),
            schedule_type=d.get("schedule_type", "cron"),
            schedule_value=d.get("schedule_value", "0 9 * * 1-5"),
            agent=d.get("agent", "manager"),
            enabled=d.get("enabled", True),
            max_runs=d.get("max_runs", 0),
            config_override=d.get("config_override", {}),
        )
        job.run_count = d.get("run_count", 0)
        if d.get("last_run"):
            try:
                job.last_run = datetime.strptime(d["last_run"], "%Y-%m-%d %H:%M:%S")
            except ValueError:
                pass
        # Recompute next_run based on updated state
        job._compute_next_run()
        return job


# --------------------------------------------------------------------------- #
#  PromptScheduler                                                             #
# --------------------------------------------------------------------------- #

SCHEDULE_FILE = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "scheduled_jobs.json"
)


class PromptScheduler:
    """
    Planificateur de prompts avec persistance JSON.
    Peut fonctionner comme daemon (thread de fond) ou en mode one-shot (tick manuel).
    """

    def __init__(
        self,
        schedule_file: str = SCHEDULE_FILE,
        tick_interval: float = 30.0,  # vérification toutes les 30 secondes
    ):
        self.schedule_file = schedule_file
        self.tick_interval = tick_interval
        self._jobs: Dict[str, ScheduledJob] = {}
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._run_task_fn: Optional[Callable] = None
        self._config: Optional[Dict] = None
        self._on_job_start: Optional[Callable] = None
        self._on_job_done: Optional[Callable] = None
        self._load()

    # ------------------------------------------------------------------ #
    #  Persistence                                                         #
    # ------------------------------------------------------------------ #

    def _load(self):
        """Charge les jobs depuis le fichier JSON."""
        if os.path.exists(self.schedule_file):
            try:
                with open(self.schedule_file, encoding="utf-8") as f:
                    data = json.load(f)
                for d in data:
                    job = ScheduledJob.from_dict(d)
                    self._jobs[job.job_id] = job
            except (json.JSONDecodeError, IOError, KeyError):
                pass

    def _save(self):
        """Persiste les jobs dans le fichier JSON."""
        directory = os.path.dirname(self.schedule_file)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with self._lock:
            data = [j.to_dict() for j in self._jobs.values()]
        with open(self.schedule_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

    # ------------------------------------------------------------------ #
    #  Job management                                                      #
    # ------------------------------------------------------------------ #

    def add_job(self, job: ScheduledJob) -> ScheduledJob:
        """Ajoute ou remplace un job planifié."""
        with self._lock:
            self._jobs[job.job_id] = job
        self._save()
        return job

    def remove_job(self, job_id: str) -> bool:
        """Supprime un job. Retourne True si supprimé."""
        with self._lock:
            if job_id in self._jobs:
                del self._jobs[job_id]
                self._save()
                return True
        return False

    def enable_job(self, job_id: str, enabled: bool = True):
        """Active ou désactive un job."""
        with self._lock:
            if job_id in self._jobs:
                self._jobs[job_id].enabled = enabled
                if enabled:
                    self._jobs[job_id]._compute_next_run()
        self._save()

    def list_jobs(self) -> List[Dict]:
        """Retourne la liste de tous les jobs avec leur état."""
        with self._lock:
            return [j.to_dict() for j in self._jobs.values()]

    def get_job(self, job_id: str) -> Optional[Dict]:
        with self._lock:
            j = self._jobs.get(job_id)
            return j.to_dict() if j else None

    # ------------------------------------------------------------------ #
    #  Setup & callbacks                                                   #
    # ------------------------------------------------------------------ #

    def setup(self, run_task_fn: Callable, config: Dict):
        """Configure la fonction d'exécution et la configuration."""
        self._run_task_fn = run_task_fn
        self._config = config

    def on_job_start(self, callback: Callable) -> "PromptScheduler":
        """callback(job_dict) appelé avant chaque exécution de job."""
        self._on_job_start = callback
        return self

    def on_job_done(self, callback: Callable) -> "PromptScheduler":
        """callback(job_dict, results) appelé après chaque exécution de job."""
        self._on_job_done = callback
        return self

    # ------------------------------------------------------------------ #
    #  Execution                                                           #
    # ------------------------------------------------------------------ #

    def _execute_job(self, job: ScheduledJob):
        """Exécute un job dans un thread dédié (non bloquant pour le scheduler)."""
        if not self._run_task_fn or not self._config:
            return

        from core.prompt_queue import PromptQueue, PromptChainItem

        if self._on_job_start:
            self._on_job_start(job.to_dict())

        # Merge config override
        merged_config = {**self._config}
        for k, v in job.config_override.items():
            if isinstance(v, dict) and isinstance(merged_config.get(k), dict):
                merged_config[k] = {**merged_config[k], **v}
            else:
                merged_config[k] = v

        # Build and run queue
        pq = PromptQueue()
        for item_def in job.prompts:
            pq.add(PromptChainItem.from_dict(item_def))

        results = pq.run(self._run_task_fn, merged_config)

        with self._lock:
            job.mark_ran()
        self._save()

        if self._on_job_done:
            self._on_job_done(job.to_dict(), results)

    def tick(self):
        """
        Vérifie les jobs dus et les lance (chacun dans son propre thread).
        Peut être appelé manuellement ou par le daemon interne.
        """
        now = datetime.now()
        with self._lock:
            due_jobs = [j for j in self._jobs.values() if j.is_due(now)]

        for job in due_jobs:
            t = threading.Thread(
                target=self._execute_job,
                args=(job,),
                daemon=True,
                name=f"Job:{job.job_id}",
            )
            t.start()

    def start_daemon(self):
        """Lance le scheduler en arrière-plan (thread daemon)."""
        if self._thread and self._thread.is_alive():
            return

        self._stop_event.clear()

        def _loop():
            while not self._stop_event.is_set():
                try:
                    self.tick()
                except Exception:
                    pass
                self._stop_event.wait(self.tick_interval)

        self._thread = threading.Thread(
            target=_loop, name="PromptScheduler", daemon=True
        )
        self._thread.start()

    def stop_daemon(self):
        """Arrête le scheduler daemon."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)

    def is_running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())
