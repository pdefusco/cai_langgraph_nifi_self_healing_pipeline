import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest  # noqa: E402

import config  # noqa: E402


@pytest.fixture
def tenant(tmp_path, monkeypatch):
    """
    A fresh mock tenant in tmp_path with the named packs deployed:

        world, client, store = tenant("p1-throughput")
    """

    from nifi_healer.cdp import client as clients
    from nifi_healer.cdp.mock_client import MockClient
    from nifi_healer.sim import bootstrap, world as worlds
    from nifi_healer.store import db

    for name, path in (("MOCK_WORLD_FILE", "mock_world.json"), ("FLEET_FILE", "fleet.json"),
                       ("DB_PATH", "healer.db"), ("CHECKPOINT_PATH", "checkpoints.sqlite"),
                       ("EDITS_DIR", "edits"), ("CHROMA_DIR", "chroma"), ("DATA_DIR", "")):
        monkeypatch.setattr(config, name, str(tmp_path / path))

    monkeypatch.setattr(config, "MOCK", True)
    saved = (worlds._WORLD, clients._CLIENT, db._STORE["store"])

    def build(*names, history: float = 0):
        worlds._WORLD = None
        world = worlds.get_world()
        client = MockClient(world)
        clients.set_client(client)
        store = db.Store(config.DB_PATH)
        db.set_store(store)
        bootstrap.build(history=history, names=list(names))

        return world, client, store

    yield build

    worlds._WORLD, clients._CLIENT, db._STORE["store"] = saved
