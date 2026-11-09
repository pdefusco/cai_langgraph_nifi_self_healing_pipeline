"""The store's timed lock (NFH-111): a long hold or wait is logged with who held it."""

import sqlite3
import threading
import time

import pytest

from nifi_healer.store import db
from nifi_healer.store.db import Store


def test_a_long_hold_and_the_wait_behind_it_are_logged_with_the_method_and_its_caller(tmp_path, monkeypatch,
                                                                                      capsys):
    monkeypatch.setattr(db, "SLOW_LOCK_SECONDS", 0.05)
    store = Store(str(tmp_path / "healer.db"))
    held = threading.Event()

    def slow_read():
        with store.lock:
            with store.lock:        # re-entered, as one store method calling another does: logged once
                held.set()
                time.sleep(0.2)

    holder = threading.Thread(target=slow_read, name="poller-1")
    holder.start()
    held.wait()
    store.incident("INC-1")
    holder.join()

    lines = [line for line in capsys.readouterr().out.splitlines() if line.startswith("[store] slow")]

    assert len(lines) == 2
    assert "slow_read held the lock 0.2s after waiting 0.0s (poller-1, from" in lines[0]
    assert "incident held the lock 0.0s after waiting" in lines[1]
    assert "test_store_lock.py:test_a_long_hold" in lines[1]


def test_a_quick_store_call_logs_nothing(tmp_path, capsys):
    store = Store(str(tmp_path / "healer.db"))
    store.incident("INC-1")

    assert "[store]" not in capsys.readouterr().out


def test_a_batch_of_samples_is_one_transaction(tmp_path):
    # In autocommit each row was its own commit, an fsync each, with the lock held (NFH-111).
    store = Store(str(tmp_path / "healer.db"))
    traced = []
    store.conn.set_trace_callback(traced.append)
    store.add_samples("crn", [("a", 1.0, 1.0), ("b", 1.0, 2.0)])

    assert traced[0] == "BEGIN" and traced[-1] == "COMMIT"
    assert store.samples("crn", "a") == [(1.0, 1.0)]

    with pytest.raises(sqlite3.Error):
        store.add_samples("crn", [("c", 2.0, 1.0), ("d", 2.0, object())])

    assert store.samples("crn", "c") == [] and not store.conn.in_transaction
