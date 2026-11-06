import asyncio
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

import config_manager as storage


class IsolatedStorage(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.config_path = self.root / "config.json"
        self.tasks_path = self.root / "tasks.json"
        for name, path in [("CONFIG_FILE", self.config_path), ("TASKS_FILE", self.tasks_path)]:
            mock = patch.object(storage, name, str(path))
            mock.start()
            self.addCleanup(mock.stop)


class StorageTests(IsolatedStorage):
    def test_missing_files_use_independent_defaults(self):
        config = storage.load_config()
        config["imap_host"] = "changed"
        self.assertEqual(storage.load_config()["imap_host"], "")
        self.assertEqual(storage.load_tasks(), [])

    def test_round_trip_preserves_defaults_and_unicode(self):
        storage.save_config({"imap_host": "邮件.example", "imap_pass": "secret"})
        storage.save_config({"concurrency": 4})
        self.assertEqual(storage.load_config()["imap_host"], "邮件.example")
        self.assertEqual(storage.load_config()["imap_pass"], "secret")
        self.assertEqual(storage.load_config()["concurrency"], 4)
        storage.save_tasks([{"id": "task-1", "message": "等待执行"}])
        self.assertEqual(storage.load_tasks()[0]["message"], "等待执行")

    def test_serialization_failure_preserves_original(self):
        for path, save, value in [
            (self.config_path, storage.save_config, {"imap_host": object()}),
            (self.tasks_path, storage.save_tasks, [{"id": object()}]),
        ]:
            with self.subTest(path=path.name):
                original = b'{"imap_host":"original"}' if path == self.config_path else b'[{"id":"original"}]'
                path.write_bytes(original)
                with self.assertRaises(TypeError):
                    save(value)
                self.assertEqual(path.read_bytes(), original)
        self.assertEqual(sorted(p.name for p in self.root.iterdir()), ["config.json", "tasks.json"])

    def test_replace_failure_preserves_original_and_cleans_temporary_file(self):
        original = b'[{"id":"original"}]'
        self.tasks_path.write_bytes(original)
        with patch.object(storage.os, "replace", side_effect=OSError("disk failure")):
            with self.assertRaises(OSError):
                storage.save_tasks([{"id": "new"}])
        self.assertEqual(self.tasks_path.read_bytes(), original)
        self.assertEqual(list(self.root.iterdir()), [self.tasks_path])

    def test_corrupt_config_is_reported_and_not_overwritten(self):
        original = b'{"imap_pass":'
        self.config_path.write_bytes(original)
        with self.assertRaises(ValueError):
            storage.save_config({"concurrency": 4})
        self.assertEqual(self.config_path.read_bytes(), original)

    def test_sync_failure_preserves_original_and_cleans_temporary_file(self):
        original = b'[{"id":"original"}]'
        self.tasks_path.write_bytes(original)
        with patch.object(storage.os, "fsync", side_effect=OSError("sync failure")):
            with self.assertRaises(OSError):
                storage.save_tasks([{"id": "new"}])
        self.assertEqual(self.tasks_path.read_bytes(), original)
        self.assertEqual(list(self.root.iterdir()), [self.tasks_path])

    def test_corrupt_task_file_is_reported(self):
        self.tasks_path.write_bytes(b'[{"id":')
        with self.assertRaises(ValueError):
            storage.load_tasks()

    def test_invalid_task_structure_is_reported(self):
        for value in [{"id": "not-a-list"}, ["not-an-object"], None]:
            with self.subTest(value=value):
                self.tasks_path.write_text(json.dumps(value), encoding="utf-8")
                with self.assertRaises(ValueError):
                    storage.load_tasks()

    def test_invalid_config_structure_is_reported(self):
        for value in [[], None, "not-an-object"]:
            with self.subTest(value=value):
                self.config_path.write_text(json.dumps(value), encoding="utf-8")
                with self.assertRaises(ValueError):
                    storage.load_config()

    def test_read_permission_error_is_not_silenced(self):
        self.config_path.write_text("{}", encoding="utf-8")
        with patch("builtins.open", side_effect=PermissionError("denied")):
            with self.assertRaises(PermissionError):
                storage.load_config()


class ApplicationTests(IsolatedStorage):
    def load_application(self, records):
        storage.save_tasks(records)
        spec = importlib.util.spec_from_file_location("state_test_app", Path(storage.__file__).with_name("app.py"))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_pending_tasks_are_not_rewritten_at_startup(self):
        with patch.object(storage, "save_tasks", wraps=storage.save_tasks) as save:
            self.load_application([{"id": "task-1", "status": "PENDING"}])
        # One write creates the fixture; normal startup should perform none.
        self.assertEqual(save.call_count, 1)

    def test_restart_recovers_all_interrupted_states_once(self):
        states = ["RUNNING", "VERIFYING", "WAITING_CAPTCHA", "QUEUED_CAPTCHA", "QUEUED", "WAITING_EMAIL"]
        records = [{"id": status, "status": status} for status in states]
        records.append({"id": "done", "status": "SUCCESS"})
        with patch.object(storage, "save_tasks", wraps=storage.save_tasks) as save:
            module = self.load_application(records)
        self.assertEqual(save.call_count, 2)  # fixture + one recovery write
        self.assertEqual([t["status"] for t in module.tasks], ["PENDING"] * len(states) + ["SUCCESS"])
        self.assertEqual(storage.load_tasks(), module.tasks)

    def test_http_refresh_preserves_worker_reference_and_persisted_result(self):
        module = self.load_application([{"id": "task-1", "status": "RUNNING"}])
        worker_record = module.tasks[0]
        worker_record["status"] = "RUNNING"
        storage.save_tasks(module.tasks)
        with TestClient(module.app) as client:
            self.assertEqual(client.get("/api/tasks").status_code, 200)
            self.assertIs(module.tasks[0], worker_record)
            worker_record["status"] = "SUCCESS"
            client.portal.call(module.on_task_status_change, worker_record)
            self.assertEqual(client.get("/api/tasks").json()[0]["status"], "SUCCESS")
        self.assertEqual(storage.load_tasks()[0]["status"], "SUCCESS")

    def test_websocket_refresh_preserves_worker_reference_and_masks_passwords(self):
        storage.save_config({"imap_pass": "imap-secret", "proxy_password": "proxy-secret"})
        module = self.load_application([{"id": "task-1", "status": "PENDING"}])
        worker_record = module.tasks[0]
        with TestClient(module.app) as client:
            with client.websocket_connect("/ws") as ws:
                initial = ws.receive_json()
                self.assertEqual(initial["type"], "INIT")
                self.assertEqual(initial["config"]["imap_pass"], "******")
                self.assertEqual(initial["config"]["proxy_password"], "******")
                self.assertIs(module.tasks[0], worker_record)
                worker_record["status"] = "SUCCESS"
                client.portal.call(module.on_task_status_change, worker_record)
                self.assertEqual(ws.receive_json()["data"]["status"], "SUCCESS")
        self.assertEqual(module.active_websockets, [])
        self.assertEqual(storage.load_tasks()[0]["status"], "SUCCESS")

    def test_corrupt_task_file_is_not_overwritten_at_startup(self):
        original = b'[{"id":'
        with patch.object(storage, "save_tasks"):
            self.tasks_path.write_bytes(original)
            with self.assertRaises(ValueError):
                self.load_application([])
        self.assertEqual(self.tasks_path.read_bytes(), original)

    def test_websocket_initial_send_failure_removes_connection(self):
        module = self.load_application([])
        ws = AsyncMock()
        ws.send_json.side_effect = OSError("connection closed")
        with self.assertRaises(OSError):
            asyncio.run(module.websocket_endpoint(ws))
        self.assertEqual(module.active_websockets, [])


if __name__ == "__main__":
    unittest.main()
