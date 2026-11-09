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


def test_a_slow_batch_is_logged_as_its_store_method_and_caller(tmp_path, monkeypatch, capsys):
    # Live, every batch read "_transaction ... from contextlib.py:__exit__".
    monkeypatch.setattr(db, "SLOW_LOCK_SECONDS", -1)
    store = Store(str(tmp_path / "healer.db"))
    capsys.readouterr()
    store.add_samples("crn", [("a", 1.0, 1.0)])

    line = capsys.readouterr().out.strip()

    assert line.startswith("[store] slow: add_samples held the lock")
    assert "test_store_lock.py:test_a_slow_batch" in line


def test_the_page_cache_is_set_from_config(tmp_path, monkeypatch):
    monkeypatch.setattr(db.config, "DB_CACHE_MB", 64)
    store = Store(str(tmp_path / "healer.db"))

    assert store.conn.execute("PRAGMA cache_size").fetchone()[0] == -64 * 1024


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


def test_the_monitor_window_is_read_from_a_covering_index(tmp_path):
    # Live, a deployment's window read 2.5-4.4 s: a table row per sample on EFS (NFH-111).
    path = str(tmp_path / "healer.db")
    old = sqlite3.connect(path)
    old.executescript("CREATE TABLE kpi_samples (deployment_crn TEXT NOT NULL, kpi_key TEXT NOT NULL, "
                      "t REAL NOT NULL, value REAL, PRIMARY KEY (deployment_crn, kpi_key, t));"
                      "CREATE INDEX kpi_samples_by_time ON kpi_samples (deployment_crn, t);")
    old.close()
    store = Store(path)
    store.upsert_deployment("crn", "p1", {}, 0.0)
    store.add_samples("crn", [("a", 1.0, 1.0), ("b", 2.0, 2.0)])

    def plans(call):
        traced = []
        store.conn.set_trace_callback(traced.append)
        call()
        store.conn.set_trace_callback(None)
        reads = [sql for sql in traced if "FROM kpi_samples" in sql]
        return [" ".join(r[-1] for r in store.conn.execute("EXPLAIN QUERY PLAN " + sql)) for sql in reads]

    indexes = {r[0] for r in store.conn.execute("SELECT name FROM sqlite_master WHERE type = 'index' "
                                                "AND tbl_name = 'kpi_samples' AND sql IS NOT NULL")}

    assert indexes == {"kpi_samples_window"}
    assert ["COVERING INDEX kpi_samples_window" in p for p in plans(lambda: store.history("crn", 0, 1))] == [True]
    assert ["kpi_samples_window" in p for p in plans(lambda: store.first_sample_at("crn"))] == [True]
    assert ["kpi_samples_window" in p for p in plans(lambda: store.prune_samples(0))] == [True]
    assert store.history("crn", 0, 1) == {"a": {1: 1.0}, "b": {2: 2.0}}
