import asyncio
import base64
from email.message import EmailMessage
import importlib.util
import json
from pathlib import Path
import ssl
import threading
import time
import unittest
from unittest.mock import AsyncMock, Mock, patch

from fastapi import HTTPException
from fastapi.testclient import TestClient

import config_manager as storage
import email_service
from test_state import IsolatedStorage


def record(identifier="task-1", status="PENDING"):
    return {"id": identifier, "email": f"{identifier}@example.invalid", "password": "fixture", "status": status}


class RegressionTests(IsolatedStorage):
    def load_application(self, records=()):
        storage.save_tasks(list(records))
        spec = importlib.util.spec_from_file_location("regression_app", Path(storage.__file__).with_name("app.py"))
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_repeated_start_preserves_every_active_state(self):
        module = self.load_application()

        async def scenario():
            states = ["RUNNING", "WAITING_CAPTCHA", "QUEUED_CAPTCHA", "QUEUED", "WAITING_EMAIL", "VERIFYING"]
            module.tasks = [record(str(i), state) for i, state in enumerate(states)]
            module.is_running_batch = True
            existing = asyncio.create_task(asyncio.Event().wait())
            module.batch_queue_task = existing
            try:
                await module.start_batch_api()
                self.assertEqual([t["status"] for t in module.tasks], states)
                self.assertIs(module.batch_queue_task, existing)
            finally:
                existing.cancel()
                await asyncio.gather(existing, return_exceptions=True)
        asyncio.run(scenario())

    def test_auto_start_and_repeated_start_share_one_execution(self):
        module = self.load_application()

        async def scenario():
            gate, entered = asyncio.Event(), asyncio.Event()
            executions = []

            async def execute(task, **kwargs):
                task["status"] = "RUNNING"
                executions.append(task["id"])
                entered.set()
                await gate.wait()
                task["status"] = "SUCCESS"

            worker = Mock(execute_task=execute)
            with patch.object(module, "RegisterWorker", return_value=worker):
                await module.add_tasks_api(module.BatchAddRequest(batch_text="audit@example.invalid", auto_start=True))
                handle = module.batch_queue_task
                self.assertIsNotNone(handle)
                try:
                    await module.start_batch_api()
                    await asyncio.wait_for(entered.wait(), 1)
                    await module.start_batch_api()
                    self.assertIs(module.batch_queue_task, handle)
                    self.assertEqual(module.tasks[0]["status"], "RUNNING")
                finally:
                    gate.set()
                    await asyncio.wait_for(handle, 2)
                self.assertEqual(len(executions), 1)
                self.assertEqual(module.active_task_ids, set())
                self.assertFalse(module.is_running_batch)
        asyncio.run(scenario())

    def test_retry_rejects_duplicate_and_leaves_other_pending_tasks_alone(self):
        module = self.load_application([record(), record("other")])

        async def scenario():
            worker = Mock(execute_task=AsyncMock(side_effect=lambda task, **kwargs: task.update(status="SUCCESS")))
            with patch.object(module, "RegisterWorker", return_value=worker):
                await module.retry_task_api("task-1")
                handle = module.batch_queue_task
                with self.assertRaises(HTTPException) as error:
                    await module.retry_task_api("task-1")
                self.assertEqual(error.exception.status_code, 409)
                await asyncio.wait_for(handle, 2)
                self.assertEqual(worker.execute_task.await_count, 1)
                self.assertEqual(module.tasks[1]["status"], "PENDING")
        asyncio.run(scenario())

    def test_single_retries_obey_global_concurrency(self):
        storage.save_config({"concurrency": 2})
        module = self.load_application([record(str(i)) for i in range(6)])

        async def scenario():
            active = 0
            maximum = 0
            executions = []

            async def execute(task, **kwargs):
                nonlocal active, maximum
                active += 1
                maximum = max(maximum, active)
                executions.append(task["id"])
                await asyncio.sleep(0.01)
                task["status"] = "SUCCESS"
                active -= 1

            with patch.object(module, "RegisterWorker", return_value=Mock(execute_task=execute)):
                for i in range(6):
                    await module.retry_task_api(str(i))
                await asyncio.wait_for(module.batch_queue_task, 2)
            self.assertEqual(maximum, 2)
            self.assertEqual(len(set(executions)), 6)
        asyncio.run(scenario())

    def test_pause_before_queue_runs_and_resume_preserves_pending_work(self):
        module = self.load_application([record()])

        async def scenario():
            worker = Mock(execute_task=AsyncMock(side_effect=lambda task, **kwargs: task.update(status="SUCCESS")))
            with patch.object(module, "RegisterWorker", return_value=worker):
                await module.retry_task_api("task-1")
                handle = module.batch_queue_task
                await module.pause_batch_api()
                await asyncio.wait_for(handle, 2)
                self.assertEqual(worker.execute_task.await_count, 0)
                self.assertEqual(module.scheduled_task_ids, {"task-1"})
                await module.start_batch_api()
                await asyncio.wait_for(module.batch_queue_task, 2)
                self.assertEqual(worker.execute_task.await_count, 1)
        asyncio.run(scenario())

    def test_worker_exception_marks_failed_and_releases_reservation(self):
        module = self.load_application([record()])

        async def scenario():
            with patch.object(module, "RegisterWorker", return_value=Mock(execute_task=AsyncMock(side_effect=RuntimeError("fixture failure")))):
                await module.retry_task_api("task-1")
                await asyncio.wait_for(module.batch_queue_task, 2)
            self.assertEqual(module.tasks[0]["status"], "FAILED")
            self.assertEqual(module.active_task_ids, set())
            self.assertFalse(module.is_running_batch)
        asyncio.run(scenario())

    def test_completed_queue_entry_does_not_restart_an_empty_queue(self):
        module = self.load_application([record(status="SUCCESS")])

        async def scenario():
            module.scheduled_task_ids.add("task-1")
            module.ensure_batch_running(2)
            handle = module.batch_queue_task
            await asyncio.wait_for(handle, 2)
            self.assertIsNone(module.batch_queue_task)
            self.assertEqual(module.scheduled_task_ids, set())
        asyncio.run(scenario())

    def test_delete_and_clear_reject_active_task(self):
        module = self.load_application([record()])
        module.active_task_ids.add("task-1")
        with TestClient(module.app) as client:
            self.assertEqual(client.delete("/api/tasks/task-1").status_code, 409)
            self.assertEqual(client.post("/api/tasks/clear", json="all").status_code, 409)
        self.assertEqual(len(module.tasks), 1)

    def test_invalid_config_returns_422_without_changing_disk(self):
        storage.save_config({"imap_pass": "secret-fixture"})
        before = self.config_path.read_bytes()
        module = self.load_application()
        invalid = [{"concurrency": value} for value in ["2", 0, 11, True, 2.5, None]]
        invalid += [{"imap_port": 0}, {"imap_ssl": "false"}, {"captcha_timeout_sec": -1}, {"proxy_strategy": "unknown"}, {"unknown": 1}]
        with TestClient(module.app) as client:
            for payload in invalid:
                with self.subTest(payload=payload):
                    response = client.post("/api/config", json=payload)
                    self.assertEqual(response.status_code, 422)
                    self.assertNotIn("secret-fixture", response.text)
                    self.assertEqual(self.config_path.read_bytes(), before)
            self.assertEqual(client.post("/api/config", json={"concurrency": 3, "imap_pass": "******"}).status_code, 200)
        self.assertEqual(storage.load_config()["concurrency"], 3)
        self.assertEqual(storage.load_config()["imap_pass"], "secret-fixture")

    def test_invalid_start_concurrency_is_rejected_before_state_changes(self):
        module = self.load_application([record(status="FAILED")])
        with TestClient(module.app) as client, patch.object(module, "RegisterWorker", return_value=Mock(execute_task=AsyncMock())):
            for value in [0, 11, "2", True, 1.5]:
                self.assertEqual(client.post("/api/tasks/start", json={"concurrency": value}).status_code, 422)
        self.assertEqual(module.tasks[0]["status"], "FAILED")
        self.assertIsNone(module.batch_queue_task)

    def test_missing_password_uses_generator_and_explicit_password_is_preserved(self):
        module = self.load_application()
        with TestClient(module.app) as client, patch.object(module, "generate_random_password", side_effect=["random-one", "random-two"]) as generate:
            self.assertEqual(client.post("/api/tasks/add", json={"batch_text": "one@example.invalid\ntwo@example.invalid"}).status_code, 200)
            self.assertEqual([t["password"] for t in module.tasks], ["random-one", "random-two"])
            client.post("/api/tasks/add", json={"batch_text": "three@example.invalid", "default_password": "explicit"})
            self.assertEqual(module.tasks[-1]["password"], "explicit")
            self.assertEqual(generate.call_count, 2)

    def test_guest_and_malformed_tokens_are_excluded_from_export(self):
        module = self.load_application()

        def token(payload):
            encoded = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
            return f"header.{encoded}.signature"

        real = token({"email": "real@example.invalid"})
        values = [token({"email": "guest-audit@example.invalid"}), "broken.not-json.token", token([]), real, "only.two", 123]
        storage.save_tasks([{**record(str(i)), "jwt": value} for i, value in enumerate(values)])
        with TestClient(module.app) as client:
            response = client.get("/api/export-tokens")
        self.assertEqual(response.text, real)

    def test_oauth_network_and_json_parsing_do_not_block_the_event_loop(self):
        import http_client
        module = self.load_application([record()])

        async def scenario():
            entered = threading.Event()
            release = threading.Event()
            released_from_event_loop = threading.Event()
            observed = []

            def slow_parse():
                entered.set()
                release.wait(1)
                observed.append(released_from_event_loop.is_set())
                return {"code": 1}

            response = Mock(json=slow_parse)
            with patch.object(http_client.requests, "request", return_value=response):
                operation = asyncio.create_task(module.oauth_cli_start_api("task-1"))
                try:
                    for _ in range(100):
                        if entered.is_set():
                            break
                        await asyncio.sleep(0.001)
                    self.assertTrue(entered.is_set())
                    released_from_event_loop.set()
                finally:
                    release.set()
                    await asyncio.wait_for(operation, 2)
            self.assertEqual(observed, [True])
        asyncio.run(scenario())

    def test_oauth_network_failure_returns_502(self):
        module = self.load_application([record()])
        with TestClient(module.app) as client, patch.object(module, "request_json", new=AsyncMock(side_effect=OSError("fixture"))):
            self.assertEqual(client.post("/api/oauth-cli/start/task-1").status_code, 502)

    def test_zcode_flow_uses_async_requests_for_init_and_poll(self):
        import zcode_token
        page = Mock()
        page.goto = AsyncMock()
        page.wait_for_timeout = AsyncMock()
        page.evaluate = AsyncMock(return_value=True)
        page.locator.return_value.first.is_visible = AsyncMock(return_value=False)
        responses = [
            {"code": 0, "data": {"flow_id": "fixture", "authorize_url": "https://example.invalid/authorize"}},
            {"data": {"status": "ready", "token": "fixture-token", "user": {}, "zai": {}}},
        ]
        with patch.object(zcode_token, "request_json", new=AsyncMock(side_effect=responses)) as request:
            result = asyncio.run(zcode_token.fetch_zcode_cli_token(page, requests_proxy="http://127.0.0.1:9999"))
        self.assertEqual(result["cli_jwt"], "fixture-token")
        self.assertEqual([call.args[0] for call in request.await_args_list], ["POST", "GET"])
        self.assertEqual(request.await_args_list[1].kwargs["proxies"]["https"], "http://127.0.0.1:9999")


class EmailRegressionTests(unittest.TestCase):
    def test_dns_fallback_retains_certificate_and_hostname_checks(self):
        with patch.object(email_service, "resolve_host", return_value="192.0.2.10"), patch.object(email_service, "ResolvedIMAP4SSL") as connect:
            email_service.EmailService(host="mail.example.invalid")._connect()
        host, port, resolved, context = connect.call_args.args
        self.assertEqual((host, resolved), ("mail.example.invalid", "192.0.2.10"))
        self.assertTrue(context.check_hostname)
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)

    def test_resolved_socket_uses_original_hostname_for_sni(self):
        connection = object.__new__(email_service.ResolvedIMAP4SSL)
        connection.host, connection.port = "mail.example.invalid", 993
        connection.resolved_host = "192.0.2.10"
        connection.ssl_context = Mock()
        raw = Mock()
        with patch.object(email_service.socket, "create_connection", return_value=raw) as connect:
            connection._create_socket(15)
        connect.assert_called_once_with(("192.0.2.10", 993), 15)
        connection.ssl_context.wrap_socket.assert_called_once_with(raw, server_hostname="mail.example.invalid")

    def test_bad_certificate_closes_unwrapped_socket(self):
        connection = object.__new__(email_service.ResolvedIMAP4SSL)
        connection.host, connection.port, connection.resolved_host = "mail.example.invalid", 993, "192.0.2.10"
        connection.ssl_context = Mock()
        connection.ssl_context.wrap_socket.side_effect = ssl.SSLCertVerificationError("untrusted")
        raw = Mock()
        with patch.object(email_service.socket, "create_connection", return_value=raw):
            with self.assertRaises(ssl.SSLCertVerificationError):
                connection._create_socket(15)
        raw.close.assert_called_once()

    def test_mail_filter_uses_internaldate_and_ignores_forged_date_header(self):
        old = EmailMessage()
        old["To"] = "audit@example.invalid"
        old["Date"] = "Fri, 01 Jan 2099 00:00:00 +0000"
        old.set_content("https://chat.z.ai/auth/verify_email?token=OLD")
        fresh = EmailMessage()
        fresh["To"] = "audit@example.invalid"
        fresh["Date"] = "Sat, 01 Jan 2000 00:00:00 +0000"
        fresh.set_content("https://chat.z.ai/auth/verify_email?token=NEW")
        mail = Mock()
        mail.search.return_value = ("OK", [b"1 2"])
        # Search iterates newest UID first; the first matching message is too old.
        mail.fetch.side_effect = [
            ("OK", [(b'2 (INTERNALDATE "01-Jan-2000 00:00:00 +0000" RFC822 {1}', old.as_bytes()), b")"]),
            ("OK", [(b'1 (INTERNALDATE "06-Nov-2026 12:00:00 +0000" RFC822 {1}', fresh.as_bytes()), b")"]),
        ]
        since = 1793966399.5  # 2026-11-06 11:59:59.5 UTC
        with patch.object(email_service.EmailService, "_connect", return_value=mail):
            link = email_service.EmailService().find_verification_link("audit@example.invalid", since_timestamp=since, timeout=1)
        self.assertEqual(link, "https://chat.z.ai/auth/verify_email?token=NEW")
        self.assertEqual(mail.fetch.call_count, 2)
        mail.logout.assert_called_once()

    def test_mail_from_submission_second_is_accepted(self):
        message = EmailMessage()
        message["To"] = "audit@example.invalid"
        message.set_content("https://chat.z.ai/auth/verify_email?token=SAME_SECOND")
        metadata = b'1 (INTERNALDATE "06-Nov-2026 12:00:00 +0000" RFC822 {1}'
        received = time.mktime(email_service.imaplib.Internaldate2tuple(metadata))
        mail = Mock()
        mail.search.return_value = ("OK", [b"1"])
        mail.fetch.return_value = ("OK", [(metadata, message.as_bytes())])
        with patch.object(email_service.EmailService, "_connect", return_value=mail):
            link = email_service.EmailService().find_verification_link("audit@example.invalid", since_timestamp=received + 0.75, timeout=1)
        self.assertEqual(link, "https://chat.z.ai/auth/verify_email?token=SAME_SECOND")


if __name__ == "__main__":
    unittest.main()
