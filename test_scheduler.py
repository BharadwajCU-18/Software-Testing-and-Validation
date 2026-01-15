import threading
import time
from unittest import mock

import pytest

from scheduler import Scheduler


def wait_for(predicate, timeout=1.0, interval=0.01):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if predicate():
            return True
        time.sleep(interval)
    return False


def test_delayed_execution():
    sched = Scheduler()
    ran = threading.Event()

    sched.schedule(0.05, ran.set)

    assert not ran.wait(0.02)
    assert ran.wait(0.5)
    sched.shutdown()


def test_invalid_times():
    sched = Scheduler()

    with pytest.raises(ValueError):
        sched.schedule(-0.1, lambda: None)

    with pytest.raises(ValueError):
        sched.schedule_at(time.monotonic() - 1.0, lambda: None)

    sched.shutdown()


def test_concurrent_dispatch():
    sched = Scheduler()
    started = []
    started_lock = threading.Lock()
    hold = threading.Event()

    def task():
        with started_lock:
            started.append(time.monotonic())
        hold.wait(0.5)

    for _ in range(3):
        sched.schedule(0.01, task)

    assert wait_for(lambda: len(started) == 3, timeout=0.5)
    hold.set()
    sched.shutdown()


def test_cancellation_rules():
    sched = Scheduler()
    ran = threading.Event()

    task_id = sched.schedule(0.05, ran.set)
    assert sched.cancel(task_id) is True
    assert sched.cancel(task_id) is False
    assert not ran.wait(0.2)

    assert sched.cancel(9999) is False
    sched.shutdown()


def test_cancel_during_dispatch_does_not_abort():
    sched = Scheduler()
    started = threading.Event()
    release = threading.Event()

    def task():
        started.set()
        release.wait(0.5)

    task_id = sched.schedule(0.01, task)
    assert started.wait(0.5)
    assert sched.cancel(task_id) is False
    release.set()
    sched.shutdown()


def test_boundary_zero_delay_executes():
    sched = Scheduler()
    ran = threading.Event()

    sched.schedule(0.0, ran.set)
    assert ran.wait(0.5)
    sched.shutdown()


def test_thread_safe_schedule_and_cancel():
    sched = Scheduler()
    executed = []
    executed_lock = threading.Lock()

    def record():
        with executed_lock:
            executed.append(1)

    def scheduler_thread():
        ids = []
        for _ in range(20):
            ids.append(sched.schedule(0.01, record))
        for task_id in ids[:10]:
            sched.cancel(task_id)

    threads = [threading.Thread(target=scheduler_thread) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert wait_for(lambda: len(executed) >= 20, timeout=1.0)
    sched.shutdown()


def test_schedule_uses_monotonic_time():
    sched = Scheduler()
    with mock.patch("scheduler.time.monotonic", return_value=5.0), mock.patch.object(
        Scheduler, "schedule_at", return_value=123
    ) as mocked:
        task_id = sched.schedule(2.0, lambda: None)
        assert task_id == 123
        mocked.assert_called_once()
        assert mocked.call_args.args[0] == 7.0
    sched.shutdown()
