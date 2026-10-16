"""End-to-end integration and concurrency tests for Multithreaded File Transfer."""

from pathlib import Path
import hashlib
import os
import socket
import tempfile
import threading
import time
import unittest

from client import FileTransferClient
from server import FileTransferServer


def get_free_port() -> int:
    """Find a dynamically available TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def compute_sha256(path: Path) -> str:
    """Compute hex SHA-256 digest of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


class TestFileTransferIntegration(unittest.TestCase):
    """Integration test suite testing full client-server file exchange and edge cases."""

    def setUp(self) -> None:
        self.server_dir = tempfile.TemporaryDirectory()
        self.client_dir = tempfile.TemporaryDirectory()
        self.server_path = Path(self.server_dir.name)
        self.client_path = Path(self.client_dir.name)

        self.port = get_free_port()
        self.server = FileTransferServer(
            host="127.0.0.1", port=self.port, storage_dir=self.server_path
        )

        self.server_thread = threading.Thread(target=self.server.start, daemon=True)
        self.server_thread.start()
        # Give server thread a moment to bind and listen
        time.sleep(0.15)

    def tearDown(self) -> None:
        self.server.stop()
        self.server_thread.join(timeout=2.0)
        self.server_dir.cleanup()
        self.client_dir.cleanup()

    def test_upload_small_text_file(self) -> None:
        """Verify uploading a small text file creates identical copy on server."""
        test_file = self.client_path / "hello.txt"
        test_content = "Hello, Academic Multithreaded Network File Transfer System!\n"
        test_file.write_text(test_content, encoding="utf-8")

        client = FileTransferClient(host="127.0.0.1", port=self.port, download_dir=self.client_path)
        self.assertTrue(client.connect())
        self.assertTrue(client.upload_file(str(test_file), show_progress=False))
        client.disconnect()

        server_file = self.server_path / "hello.txt"
        self.assertTrue(server_file.exists())
        self.assertEqual(server_file.read_text(encoding="utf-8"), test_content)

    def test_upload_binary_file_hash_verification(self) -> None:
        """Verify binary file upload integrity with SHA-256 hash comparison."""
        bin_file = self.client_path / "data.bin"
        # Generate 256 KB of pseudorandom binary payload
        random_bytes = os.urandom(256 * 1024)
        bin_file.write_bytes(random_bytes)
        expected_hash = hashlib.sha256(random_bytes).hexdigest()

        client = FileTransferClient(host="127.0.0.1", port=self.port, download_dir=self.client_path)
        self.assertTrue(client.connect())
        self.assertTrue(client.upload_file(str(bin_file), show_progress=False))
        client.disconnect()

        server_bin = self.server_path / "data.bin"
        self.assertTrue(server_bin.exists())
        self.assertEqual(compute_sha256(server_bin), expected_hash)

    def test_upload_zero_byte_file(self) -> None:
        """Verify uploading an empty 0-byte file creates 0-byte file on server."""
        empty_file = self.client_path / "empty.txt"
        empty_file.touch()

        client = FileTransferClient(host="127.0.0.1", port=self.port, download_dir=self.client_path)
        self.assertTrue(client.connect())
        self.assertTrue(client.upload_file(str(empty_file), show_progress=False))
        client.disconnect()

        server_empty = self.server_path / "empty.txt"
        self.assertTrue(server_empty.exists())
        self.assertEqual(server_empty.stat().st_size, 0)

    def test_download_file_successfully(self) -> None:
        """Verify downloading a file from server reproduces exact file on client."""
        server_file = self.server_path / "source.txt"
        content = "Line 1: Computer Networks\nLine 2: Socket Programming\n" * 50
        server_file.write_text(content, encoding="utf-8")

        client = FileTransferClient(host="127.0.0.1", port=self.port, download_dir=self.client_path)
        self.assertTrue(client.connect())
        self.assertTrue(client.download_file("source.txt", show_progress=False))
        client.disconnect()

        downloaded_file = self.client_path / "source.txt"
        self.assertTrue(downloaded_file.exists())
        self.assertEqual(downloaded_file.read_text(encoding="utf-8"), content)

    def test_download_missing_file_reports_error(self) -> None:
        """Verify downloading a nonexistent file returns false and does not create file."""
        client = FileTransferClient(host="127.0.0.1", port=self.port, download_dir=self.client_path)
        self.assertTrue(client.connect())
        success = client.download_file("nonexistent_file.xyz", show_progress=False)
        self.assertFalse(success)
        client.disconnect()

        self.assertFalse((self.client_path / "nonexistent_file.xyz").exists())

    def test_list_files_correctness(self) -> None:
        """Verify file list metadata returns all expected regular files."""
        (self.server_path / "file1.txt").write_text("123", encoding="utf-8")
        (self.server_path / "file2.pdf").write_bytes(b"%PDF-1.4...")
        # Add a subfolder and hidden file which must NOT be exposed
        (self.server_path / "subdir").mkdir()
        (self.server_path / ".hidden").touch()

        client = FileTransferClient(host="127.0.0.1", port=self.port, download_dir=self.client_path)
        self.assertTrue(client.connect())
        file_list = client.list_files()
        client.disconnect()

        self.assertIsNotNone(file_list)
        names = [f["name"] for f in file_list]
        self.assertIn("file1.txt", names)
        self.assertIn("file2.pdf", names)
        self.assertNotIn("subdir", names)
        self.assertNotIn(".hidden", names)

    def test_prevent_accidental_file_overwrite(self) -> None:
        """Verify server rejects overwriting an existing file unless overwrite=True."""
        existing = self.server_path / "protected.txt"
        existing.write_text("Original content", encoding="utf-8")

        # Local file with different content
        local_file = self.client_path / "protected.txt"
        local_file.write_text("New malicious or accidental overwrite", encoding="utf-8")

        client = FileTransferClient(host="127.0.0.1", port=self.port, download_dir=self.client_path)
        self.assertTrue(client.connect())
        # Attempt upload with overwrite=False
        success = client.upload_file(str(local_file), overwrite=False, show_progress=False)
        self.assertFalse(success)

        # Confirm original content is untouched
        self.assertEqual(existing.read_text(encoding="utf-8"), "Original content")

        # Now attempt with overwrite=True
        success_overwrite = client.upload_file(str(local_file), overwrite=True, show_progress=False)
        self.assertTrue(success_overwrite)
        self.assertEqual(
            existing.read_text(encoding="utf-8"), "New malicious or accidental overwrite"
        )
        client.disconnect()

    def test_path_traversal_rejection(self) -> None:
        """Verify path traversal attempts are detected, sanitized, or rejected."""
        # When client specifies path with directory separators, sanitize_filename strips them
        # Let's test client sanitize and raw protocol injection
        from protocol import CMD_DOWNLOAD, CMD_UPLOAD, make_error_response, recv_message, send_message

        # Direct protocol injection attempting traversal
        raw_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        raw_sock.connect(("127.0.0.1", self.port))

        try:
            # Attempt to download outside storage directory using relative path
            send_message(
                raw_sock,
                {"command": CMD_DOWNLOAD, "filename": "../../../boot.ini"},
            )
            response = recv_message(raw_sock)
            # Must return error (either invalid filename 400 or access denied 403 or not found)
            self.assertEqual(response.get("status"), "ERROR")
        finally:
            raw_sock.close()

    def test_server_unavailable(self) -> None:
        """Verify client handles connection attempt when server is offline gracefully."""
        unused_port = get_free_port()
        client = FileTransferClient(host="127.0.0.1", port=unused_port, download_dir=self.client_path)
        # Must return False and not raise an uncaught exception
        connected = client.connect()
        self.assertFalse(connected)
        self.assertFalse(client.is_connected)

    def test_client_disconnect_during_upload_cleans_up(self) -> None:
        """Verify server aborts and deletes .part file when client drops connection mid-upload."""
        raw_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        raw_sock.connect(("127.0.0.1", self.port))

        from protocol import CMD_UPLOAD, recv_message, send_message
        target_name = "aborted_upload.bin"
        declared_size = 50000

        # Send upload request
        send_message(
            raw_sock,
            {"command": CMD_UPLOAD, "filename": target_name, "size": declared_size},
        )
        resp = recv_message(raw_sock)
        self.assertEqual(resp.get("status"), "OK")

        # Send only 500 bytes and abruptly close the socket
        raw_sock.sendall(b"A" * 500)
        raw_sock.close()

        # Wait briefly for server handler thread to detect EOF and perform cleanup
        time.sleep(0.3)

        # Neither target file nor .part file should remain in server storage
        self.assertFalse((self.server_path / target_name).exists())
        self.assertFalse((self.server_path / f"{target_name}.part").exists())

    def test_two_way_transfer_with_delivery_receipt(self) -> None:
        """Verify two-way file exchange between users and delivery confirmation receipts."""
        alice_dir = tempfile.TemporaryDirectory()
        bob_dir = tempfile.TemporaryDirectory()
        alice_path = Path(alice_dir.name)
        bob_path = Path(bob_dir.name)

        try:
            # 1. Connect and register Alice and Bob
            client_alice = FileTransferClient(host="127.0.0.1", port=self.port, download_dir=alice_path)
            client_bob = FileTransferClient(host="127.0.0.1", port=self.port, download_dir=bob_path)

            self.assertTrue(client_alice.connect())
            self.assertTrue(client_bob.connect())

            self.assertTrue(client_alice.login("Alice", "password123") or client_alice.register("Alice", "password123"))
            self.assertTrue(client_bob.login("Bob", "password123") or client_bob.register("Bob", "password123"))

            # 2. Alice sends a file to Bob
            alice_file = alice_path / "homework.py"
            alice_content = "print('Hello Bob! Here is the completed assignment.')\n"
            alice_file.write_text(alice_content, encoding="utf-8")

            sent_ok = client_alice.send_file_to_user(recipient="Bob", local_path_str=str(alice_file), show_progress=False)
            self.assertTrue(sent_ok)

            # 3. Verify Alice's outbox shows PENDING_RECEIPT before Bob downloads
            outbox_alice = client_alice.view_outbox()
            self.assertIsNotNone(outbox_alice)
            self.assertEqual(len(outbox_alice), 1)
            self.assertEqual(outbox_alice[0]["recipient"].lower(), "bob")
            self.assertEqual(outbox_alice[0]["status"], "PENDING_RECEIPT")
            self.assertIsNone(outbox_alice[0]["received_at"])

            # 4. Bob checks inbox and downloads homework.py
            inbox_bob = client_bob.view_inbox()
            self.assertIsNotNone(inbox_bob)
            self.assertEqual(len(inbox_bob), 1)
            self.assertEqual(inbox_bob[0]["sender"].lower(), "alice")
            self.assertEqual(inbox_bob[0]["filename"], "homework.py")

            tx_id = inbox_bob[0]["transfer_id"]
            download_ok = client_bob.download_inbox_file(transfer_id=tx_id, show_progress=False)
            self.assertTrue(download_ok)

            # Verify downloaded file content
            bob_received_file = bob_path / "homework.py"
            self.assertTrue(bob_received_file.exists())
            self.assertEqual(bob_received_file.read_text(encoding="utf-8"), alice_content)

            # 5. Alice checks outbox again: DELIVERY CONFIRMATION is verified!
            outbox_alice_updated = client_alice.view_outbox()
            self.assertEqual(outbox_alice_updated[0]["status"], "RECEIVED_BY_USER")
            self.assertIsNotNone(outbox_alice_updated[0]["received_at"])

            # 6. Two-Way Reply: Bob sends a reply file back to Alice!
            bob_reply = bob_path / "feedback.txt"
            bob_content = "Great job Alice! Checked and grade is 100/100.\n"
            bob_reply.write_text(bob_content, encoding="utf-8")

            reply_ok = client_bob.send_file_to_user(recipient="Alice", local_path_str=str(bob_reply), show_progress=False)
            self.assertTrue(reply_ok)

            # Alice checks inbox and downloads Bob's reply
            inbox_alice = client_alice.view_inbox()
            self.assertEqual(len(inbox_alice), 1)
            self.assertEqual(inbox_alice[0]["sender"].lower(), "bob")

            reply_tx_id = inbox_alice[0]["transfer_id"]
            alice_dl_ok = client_alice.download_inbox_file(transfer_id=reply_tx_id, show_progress=False)
            self.assertTrue(alice_dl_ok)

            # Bob checks outbox: confirms Alice received his reply
            outbox_bob = client_bob.view_outbox()
            self.assertEqual(outbox_bob[0]["status"], "RECEIVED_BY_USER")
            self.assertIsNotNone(outbox_bob[0]["received_at"])

            client_alice.disconnect()
            client_bob.disconnect()
        finally:
            alice_dir.cleanup()
            bob_dir.cleanup()


class TestConcurrentClients(unittest.TestCase):

    """Tests high-concurrency multi-client simultaneous operations."""

    def setUp(self) -> None:
        self.server_dir = tempfile.TemporaryDirectory()
        self.client_dir = tempfile.TemporaryDirectory()
        self.server_path = Path(self.server_dir.name)
        self.client_path = Path(self.client_dir.name)

        self.port = get_free_port()
        self.server = FileTransferServer(
            host="127.0.0.1", port=self.port, storage_dir=self.server_path
        )
        self.server_thread = threading.Thread(target=self.server.start, daemon=True)
        self.server_thread.start()
        time.sleep(0.15)

    def tearDown(self) -> None:
        self.server.stop()
        self.server_thread.join(timeout=2.0)
        self.server_dir.cleanup()
        self.client_dir.cleanup()

    def test_multiple_concurrent_uploads_and_downloads(self) -> None:
        """Verify multiple clients upload, download, and list simultaneously without error."""
        # Prepopulate a shared download target file on the server
        shared_server_file = self.server_path / "shared_notes.txt"
        shared_content = "Distributed Systems, Concurrency, and OS Threads\n" * 500
        shared_server_file.write_text(shared_content, encoding="utf-8")

        results = {}
        errors = []

        def worker_upload(client_id: int, file_size_kb: int):
            try:
                c_dir = self.client_path / f"client_{client_id}"
                c_dir.mkdir(parents=True, exist_ok=True)
                test_file = c_dir / f"payload_{client_id}.bin"
                data = os.urandom(file_size_kb * 1024)
                test_file.write_bytes(data)
                expected_hash = hashlib.sha256(data).hexdigest()

                c = FileTransferClient(host="127.0.0.1", port=self.port, download_dir=c_dir)
                if not c.connect():
                    errors.append(f"Client {client_id} failed to connect")
                    return
                ok = c.upload_file(str(test_file), show_progress=False)
                c.disconnect()

                if not ok:
                    errors.append(f"Client {client_id} upload returned False")
                    return

                # Verify server file integrity
                srv_file = self.server_path / f"payload_{client_id}.bin"
                if not srv_file.exists():
                    errors.append(f"Server file for client {client_id} not found")
                    return
                if compute_sha256(srv_file) != expected_hash:
                    errors.append(f"Hash mismatch for client {client_id}")
                    return

                results[client_id] = "SUCCESS"
            except Exception as e:
                errors.append(f"Client {client_id} exception: {e}")

        def worker_download(client_id: int):
            try:
                c_dir = self.client_path / f"client_{client_id}"
                c_dir.mkdir(parents=True, exist_ok=True)
                c = FileTransferClient(host="127.0.0.1", port=self.port, download_dir=c_dir)
                if not c.connect():
                    errors.append(f"Downloader {client_id} failed to connect")
                    return
                ok = c.download_file("shared_notes.txt", show_progress=False)
                c.disconnect()

                if not ok:
                    errors.append(f"Downloader {client_id} failed download")
                    return

                down_file = c_dir / "shared_notes.txt"
                if not down_file.exists() or down_file.read_text(encoding="utf-8") != shared_content:
                    errors.append(f"Downloader {client_id} content mismatch")
                    return

                results[client_id] = "SUCCESS"
            except Exception as e:
                errors.append(f"Downloader {client_id} exception: {e}")

        def worker_lister(client_id: int):
            try:
                c = FileTransferClient(host="127.0.0.1", port=self.port)
                if not c.connect():
                    errors.append(f"Lister {client_id} failed to connect")
                    return
                # Perform multiple rapid listing requests
                for _ in range(5):
                    fl = c.list_files()
                    if fl is None:
                        errors.append(f"Lister {client_id} got None")
                        return
                    time.sleep(0.02)
                c.disconnect()
                results[client_id] = "SUCCESS"
            except Exception as e:
                errors.append(f"Lister {client_id} exception: {e}")

        # Spawn 5 concurrent clients with mixed operations
        threads = [
            threading.Thread(target=worker_upload, args=(1, 100)),
            threading.Thread(target=worker_upload, args=(2, 200)),
            threading.Thread(target=worker_download, args=(3,)),
            threading.Thread(target=worker_download, args=(4,)),
            threading.Thread(target=worker_lister, args=(5,)),
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10.0)

        self.assertEqual(errors, [], f"Concurrency errors occurred: {errors}")
        self.assertEqual(len(results), 5)


class TestUserAccountsAndTwoWayTransfer(unittest.TestCase):
    """Integration tests for user registration, auto-generated User ID, and two-way transfers."""

    def setUp(self) -> None:
        self.server_dir = tempfile.TemporaryDirectory()
        self.alice_dir = tempfile.TemporaryDirectory()
        self.bob_dir = tempfile.TemporaryDirectory()
        self.server_path = Path(self.server_dir.name)
        self.alice_path = Path(self.alice_dir.name)
        self.bob_path = Path(self.bob_dir.name)

        self.port = get_free_port()
        self.server = FileTransferServer(
            host="127.0.0.1", port=self.port, storage_dir=self.server_path
        )
        self.server_thread = threading.Thread(target=self.server.start, daemon=True)
        self.server_thread.start()
        time.sleep(0.15)

    def tearDown(self) -> None:
        self.server.stop()
        self.server_thread.join(timeout=2.0)
        self.server_dir.cleanup()
        self.alice_dir.cleanup()
        self.bob_dir.cleanup()

    def test_preseeded_dummy_accounts_and_login_by_email_uid_or_username(self) -> None:
        """Verify pre-seeded dummy accounts Alice and Bob can login by Email, UID, or Username."""
        client = FileTransferClient(host="127.0.0.1", port=self.port, download_dir=self.alice_path)
        self.assertTrue(client.connect())

        # 1. Login with email "alice@network.edu"
        self.assertTrue(client.login("alice@network.edu", "password123"))
        self.assertEqual(client.username, "alice")
        self.assertEqual(client.user_id, "UID-1001")
        self.assertEqual(client.email, "alice@network.edu")

        # 2. Login with user ID "UID-1002" (Bob)
        self.assertTrue(client.login("UID-1002", "password123"))
        self.assertEqual(client.username, "bob")
        self.assertEqual(client.user_id, "UID-1002")
        self.assertEqual(client.email, "bob@network.edu")

        # 3. Login with username "bob"
        self.assertTrue(client.login("bob", "password123"))
        self.assertEqual(client.user_id, "UID-1002")

        # 4. Invalid password fails
        self.assertFalse(client.login("alice@network.edu", "wrong_pass"))

        # 5. Non-existent user fails
        self.assertFalse(client.login("nobody@network.edu", "password123"))

        client.disconnect()

    def test_user_registration_with_email_auto_generates_unique_id(self) -> None:
        """Verify registering with an email address auto-generates a Unique ID (UID-XXXX)."""
        client = FileTransferClient(host="127.0.0.1", port=self.port, download_dir=self.alice_path)
        self.assertTrue(client.connect())

        unique_suffix = int(time.time() * 1000) % 100000
        email = f"charlie_{unique_suffix}@network.edu"
        
        ok = client.register(email_or_username=email, password="pass1234", email=email)
        self.assertTrue(ok)
        self.assertTrue(client.user_id.startswith("UID-"))
        self.assertEqual(client.email, email)
        charlie_uid = client.user_id

        # Verify login via Email works
        self.assertTrue(client.login(email, "pass1234"))
        self.assertEqual(client.user_id, charlie_uid)

        # Verify login via the generated Unique ID works
        self.assertTrue(client.login(charlie_uid, "pass1234"))
        self.assertEqual(client.email, email)

        # Duplicate email registration must be rejected
        self.assertFalse(client.register(email, "pass1234"))

        client.disconnect()

    def test_two_way_transfer_addressing_by_user_id_and_username(self) -> None:
        """Verify two-way file exchange where recipient is targeted by User ID or Username, with ACKs."""
        alice_client = FileTransferClient(
            host="127.0.0.1", port=self.port, download_dir=self.alice_path
        )
        bob_client = FileTransferClient(
            host="127.0.0.1", port=self.port, download_dir=self.bob_path
        )
        self.assertTrue(alice_client.connect())
        self.assertTrue(bob_client.connect())

        self.assertTrue(alice_client.login("alice@network.edu", "password123"))
        self.assertTrue(bob_client.login("bob@network.edu", "password123"))

        # Step 1: Alice sends a document addressed to Bob's Unique ID: "UID-1002"
        alice_file = self.alice_path / "alice_project_report.txt"
        alice_content = "Confidential OS Project Report from Alice to Bob UID-1002\n"
        alice_file.write_text(alice_content, encoding="utf-8")

        sent_ok = alice_client.send_file_to_user(
            recipient="UID-1002",
            local_path_str=str(alice_file),
            show_progress=False,
        )
        self.assertTrue(sent_ok, "Alice sending to UID-1002 failed")

        # Verify Alice's outbox initially shows delivery pending
        alice_outbox = alice_client.view_outbox()
        self.assertIsNotNone(alice_outbox)
        self.assertEqual(len(alice_outbox), 1)
        self.assertEqual(alice_outbox[0]["filename"], "alice_project_report.txt")
        self.assertEqual(alice_outbox[0]["recipient"], "bob")
        self.assertEqual(alice_outbox[0]["recipient_uid"], "UID-1002")
        self.assertEqual(alice_outbox[0]["status"], "PENDING_RECEIPT")
        transfer_id = alice_outbox[0]["transfer_id"]

        # Step 2: Bob checks his inbox and downloads the file
        bob_inbox = bob_client.view_inbox()
        self.assertIsNotNone(bob_inbox)
        self.assertEqual(len(bob_inbox), 1)
        self.assertEqual(bob_inbox[0]["transfer_id"], transfer_id)
        self.assertEqual(bob_inbox[0]["sender"], "alice")
        self.assertEqual(bob_inbox[0]["sender_uid"], "UID-1001")

        # Bob downloads and confirms receipt
        recv_ok = bob_client.download_inbox_file(transfer_id=transfer_id, show_progress=False)
        self.assertTrue(recv_ok, "Bob receiving inbox file failed")

        downloaded_file = self.bob_path / "alice_project_report.txt"
        self.assertTrue(downloaded_file.exists())
        self.assertEqual(downloaded_file.read_text(encoding="utf-8"), alice_content)

        # Step 3: Alice checks her outbox again -> status must now be confirmed (RECEIVED_BY_USER)!
        updated_outbox = alice_client.view_outbox()
        self.assertEqual(len(updated_outbox), 1)
        self.assertEqual(updated_outbox[0]["status"], "RECEIVED_BY_USER")
        self.assertIsNotNone(updated_outbox[0]["received_at"])

        # Step 4: Two-way reply: Bob sends a response addressed to Alice's Unique ID: "UID-1001"
        bob_reply = self.bob_path / "bob_review_feedback.txt"
        bob_content = "Feedback received and approved by Bob!\n"
        bob_reply.write_text(bob_content, encoding="utf-8")

        reply_ok = bob_client.send_file_to_user(
            recipient="UID-1001",
            local_path_str=str(bob_reply),
            show_progress=False,
        )
        self.assertTrue(reply_ok, "Bob sending to UID-1001 failed")

        # Alice checks inbox and receives reply
        alice_inbox = alice_client.view_inbox()
        self.assertEqual(len(alice_inbox), 1)
        self.assertEqual(alice_inbox[0]["filename"], "bob_review_feedback.txt")
        self.assertEqual(alice_inbox[0]["sender"], "bob")
        self.assertEqual(alice_inbox[0]["sender_uid"], "UID-1002")

        recv_reply_ok = alice_client.download_inbox_file(
            transfer_id=alice_inbox[0]["transfer_id"], show_progress=False
        )
        self.assertTrue(recv_reply_ok)

        # Bob checks outbox -> confirmed delivery
        bob_outbox = bob_client.view_outbox()
        self.assertEqual(len(bob_outbox), 1)
        self.assertEqual(bob_outbox[0]["status"], "RECEIVED_BY_USER")

        alice_client.disconnect()
        bob_client.disconnect()


if __name__ == "__main__":
    unittest.main()

