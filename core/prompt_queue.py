"""
core/prompt_queue.py
====================
File d'attente de prompts avec exécution séquentielle (chaining).

Fonctionnalités :
- Définir une séquence de prompts à exécuter en bloc
- Injecter des prompts dans la queue pendant l'exécution (thread-safe)
- Passer le résultat d'un prompt comme contexte au suivant
- Callbacks sur début/fin de chaque prompt et fin de queue
"""

import queue
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Union


class PromptChainItem:
    """Un prompt dans une chaîne d'exécution."""

    def __init__(
        self,
        prompt: str,
        agent: str = "manager",
        pass_result: bool = False,
        result_key: str = "previous_result",
        label: str = "",
        variables: Optional[Dict[str, str]] = None,
    ):
        """
        Args:
            prompt      : Texte du prompt (peut contenir {previous_result} ou toute variable)
            agent       : Agent à utiliser pour ce prompt
            pass_result : Si True, injecte le résultat du prompt précédent dans le suivant
            result_key  : Clé de template pour le résultat précédent (défaut: previous_result)
            label       : Libellé pour l'affichage / logs
            variables   : Variables à substituer dans le prompt
        """
        self.prompt = prompt
        self.agent = agent
        self.pass_result = pass_result
        self.result_key = result_key
        self.label = label or prompt[:60]
        self.variables = variables or {}

    def render(self, previous_result: Optional[str] = None) -> str:
        """Applique les variables et le résultat précédent au prompt."""
        text = self.prompt
        for key, val in self.variables.items():
            text = text.replace(f"{{{key}}}", str(val))
        if previous_result and self.pass_result:
            text = text.replace(f"{{{self.result_key}}}", previous_result)
        return text

    def to_dict(self) -> Dict:
        return {
            "prompt": self.prompt,
            "agent": self.agent,
            "pass_result": self.pass_result,
            "result_key": self.result_key,
            "label": self.label,
            "variables": self.variables,
        }

    @classmethod
    def from_dict(cls, d: Dict) -> "PromptChainItem":
        return cls(
            prompt=d.get("prompt", ""),
            agent=d.get("agent", "manager"),
            pass_result=d.get("pass_result", False),
            result_key=d.get("result_key", "previous_result"),
            label=d.get("label", ""),
            variables=d.get("variables", {}),
        )

    @classmethod
    def from_str(cls, prompt: str, agent: str = "manager") -> "PromptChainItem":
        return cls(prompt=prompt, agent=agent)


class PromptQueue:
    """
    File d'attente de prompts avec exécution séquentielle.
    Thread-safe : supporte l'injection de prompts en cours d'exécution.
    """

    def __init__(self):
        self._queue: queue.Queue = queue.Queue()
        self._lock = threading.Lock()
        self._running = False
        self._stop_requested = False
        self._current_item: Optional[PromptChainItem] = None
        self._results: List[Dict] = []
        self._on_item_start: Optional[Callable] = None
        self._on_item_done: Optional[Callable] = None
        self._on_queue_done: Optional[Callable] = None

    # ------------------------------------------------------------------ #
    #  Queue management                                                    #
    # ------------------------------------------------------------------ #

    def add(
        self,
        item: Union["PromptChainItem", Dict, str],
        agent: str = "manager",
    ) -> None:
        """
        Ajoute un prompt à la queue (peut être appelé pendant l'exécution).

        Accepte :
        - un PromptChainItem
        - un dict (clés: prompt, agent, pass_result, label, variables, ...)
        - une chaîne de caractères (prompt brut)
        """
        if isinstance(item, str):
            item = PromptChainItem(prompt=item, agent=agent)
        elif isinstance(item, dict):
            item = PromptChainItem.from_dict(item)
        self._queue.put(item)

    def add_chain(
        self,
        items: List[Union["PromptChainItem", Dict, str]],
        agent: str = "manager",
    ) -> None:
        """Ajoute une liste de prompts à la queue."""
        for item in items:
            self.add(item, agent=agent)

    def clear(self) -> int:
        """Vide la queue. Retourne le nombre d'éléments supprimés."""
        count = 0
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
                count += 1
            except queue.Empty:
                break
        return count

    def stop(self):
        """Demande l'arrêt de l'exécution après le prompt en cours."""
        self._stop_requested = True

    def size(self) -> int:
        """Retourne le nombre de prompts en attente."""
        return self._queue.qsize()

    def is_running(self) -> bool:
        return self._running

    # ------------------------------------------------------------------ #
    #  Callbacks                                                           #
    # ------------------------------------------------------------------ #

    def on_item_start(self, callback: Callable) -> "PromptQueue":
        """callback(index: int, item: PromptChainItem, processed: int) avant chaque prompt."""
        self._on_item_start = callback
        return self

    def on_item_done(self, callback: Callable) -> "PromptQueue":
        """callback(index: int, item: PromptChainItem, entry: dict, duration: float) après."""
        self._on_item_done = callback
        return self

    def on_queue_done(self, callback: Callable) -> "PromptQueue":
        """callback(results: List[dict]) quand la queue est vide."""
        self._on_queue_done = callback
        return self

    # ------------------------------------------------------------------ #
    #  Execution                                                           #
    # ------------------------------------------------------------------ #

    def run(
        self,
        run_task_fn: Callable,
        config: Dict,
        **run_kwargs,
    ) -> List[Dict]:
        """
        Exécute tous les prompts de la queue séquentiellement.

        Args:
            run_task_fn : Fonction run_task(config, task, agent, **kwargs) → dict
            config      : Configuration du système
            **run_kwargs: Arguments supplémentaires pour run_task_fn (ex: allow_write)

        Returns:
            Liste des résultats de chaque prompt.
        """
        self._running = True
        self._stop_requested = False
        self._results = []
        previous_result_text: Optional[str] = None
        idx = 0

        try:
            while not self._queue.empty() and not self._stop_requested:
                try:
                    item = self._queue.get_nowait()
                except queue.Empty:
                    break

                self._current_item = item

                if self._on_item_start:
                    self._on_item_start(idx, item, len(self._results))

                rendered_prompt = item.render(previous_result=previous_result_text)
                t0 = time.time()

                try:
                    result = run_task_fn(
                        config=config,
                        task=rendered_prompt,
                        agent=item.agent,
                        **run_kwargs,
                    )
                    duration = time.time() - t0
                    entry = {
                        "index": idx,
                        "label": item.label,
                        "prompt": rendered_prompt,
                        "agent": item.agent,
                        "result": result,
                        "duration": round(duration, 2),
                        "status": "ok",
                    }
                    # Build plain-text summary for chaining into next prompt
                    if isinstance(result, dict):
                        previous_result_text = (
                            result.get("answer")
                            or result.get("summary")
                            or str(result)
                        )
                    else:
                        previous_result_text = str(result)

                except Exception as exc:
                    duration = time.time() - t0
                    entry = {
                        "index": idx,
                        "label": item.label,
                        "prompt": rendered_prompt,
                        "agent": item.agent,
                        "result": {"error": str(exc)},
                        "duration": round(duration, 2),
                        "status": "error",
                    }
                    previous_result_text = f"[ERREUR: {exc}]"

                self._results.append(entry)
                self._queue.task_done()

                if self._on_item_done:
                    self._on_item_done(idx, item, entry, duration)

                idx += 1

        finally:
            self._running = False
            self._current_item = None

        if self._on_queue_done:
            self._on_queue_done(self._results)

        return self._results

    def run_async(
        self,
        run_task_fn: Callable,
        config: Dict,
        **run_kwargs,
    ) -> threading.Thread:
        """Lance l'exécution de la queue dans un thread séparé. Non bloquant."""
        t = threading.Thread(
            target=self.run,
            args=(run_task_fn, config),
            kwargs=run_kwargs,
            daemon=True,
            name="PromptQueue",
        )
        t.start()
        return t

    @property
    def results(self) -> List[Dict]:
        return list(self._results)

    @property
    def current_item(self) -> Optional[PromptChainItem]:
        return self._current_item
