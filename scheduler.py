"""Event-driven task scheduler with delayed execution and cancellation."""

from __future__ import annotations

import heapq
import threading
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Tuple


@dataclass
class ScheduledTask:
    task_id: int
    run_at: float
    fn: Callable[[], None]
    cancelled: bool = False
    dispatched: bool = False
    completed: bool = False


class Scheduler:
    """A minimal task scheduler using a single dispatcher thread."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._tasks: Dict[int, ScheduledTask] = {}
        self._heap: List[Tuple[float, int, int]] = []
        self._seq = 0
        self._stop = False
        self._next_id = 1
        self._workers: List[threading.Thread] = []
        self._dispatcher = threading.Thread(target=self._run_loop, name="scheduler-dispatcher")
        self._dispatcher.daemon = True
        self._dispatcher.start()

    def schedule(self, delay_seconds: float, fn: Callable[[], None]) -> int:
        """Schedule a task after delay_seconds; returns a task id."""
        if delay_seconds < 0:
            raise ValueError("delay_seconds must be >= 0")
        run_at = time.monotonic() + delay_seconds
        return self.schedule_at(run_at, fn, allow_past=True)

    def schedule_at(self, run_at: float, fn: Callable[[], None], allow_past: bool = False) -> int:
        """Schedule a task at an absolute monotonic time."""
        now = time.monotonic()
        if run_at < now:
            if allow_past:
                run_at = now
            else:
                raise ValueError("run_at must be >= now")
        with self._cv:
            task_id = self._next_id
            self._next_id += 1
            task = ScheduledTask(task_id=task_id, run_at=run_at, fn=fn)
            self._tasks[task_id] = task
            heapq.heappush(self._heap, (run_at, self._seq, task_id))
            self._seq += 1
            self._cv.notify()
            return task_id

    def cancel(self, task_id: int) -> bool:
        """Cancel a pending task. Returns True if cancellation succeeded."""
        with self._cv:
            task = self._tasks.get(task_id)
            if task is None or task.dispatched or task.completed:
                return False
            task.cancelled = True
            self._tasks.pop(task_id, None)
            self._cv.notify()
            return True

    def shutdown(self, wait: bool = True) -> None:
        """Stop the dispatcher and optionally wait for running tasks."""
        with self._cv:
            self._stop = True
            self._cv.notify_all()
        if wait:
            self._dispatcher.join(timeout=2.0)
            for worker in list(self._workers):
                worker.join(timeout=2.0)

    def _run_loop(self) -> None:
        while True:
            with self._cv:
                while not self._stop and not self._heap:
                    self._cv.wait()
                if self._stop:
                    return
                now = time.monotonic()
                run_at, _seq, task_id = self._heap[0]
                if run_at > now:
                    self._cv.wait(timeout=run_at - now)
                    continue
                heapq.heappop(self._heap)
                task = self._tasks.pop(task_id, None)
                if task is None or task.cancelled:
                    continue
                task.dispatched = True
            worker = threading.Thread(target=self._run_task, args=(task,), name=f"task-{task_id}")
            worker.daemon = True
            with self._lock:
                self._workers.append(worker)
            worker.start()

    def _run_task(self, task: ScheduledTask) -> None:
        try:
            task.fn()
        finally:
            with self._lock:
                task.completed = True
                self._workers = [w for w in self._workers if w.is_alive()]
