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


if __name__ == "__main__":
    unittest.main()
