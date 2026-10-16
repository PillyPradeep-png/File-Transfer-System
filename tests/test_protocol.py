"""Unit tests for protocol message framing, streaming, and error handling."""

from pathlib import Path
import json
import socket
import struct
import tempfile
import threading
import time
import unittest

from config import MAX_CONTROL_MESSAGE_SIZE
from protocol import (
    CMD_LIST,
    CMD_UPLOAD,
    HEADER_FORMAT,
    HEADER_SIZE,
    ConnectionClosedError,
    ProtocolError,
    recv_exact,
    recv_file_stream,
    recv_message,
    send_file_stream,
    send_message,
)


class TestProtocolFraming(unittest.TestCase):
    """Tests for protocol control message encoding, decoding, and socket stream handling."""

    def setUp(self) -> None:
        self.sock_client, self.sock_server = socket.socketpair()
        self.sock_client.settimeout(2.0)
        self.sock_server.settimeout(2.0)

    def tearDown(self) -> None:
        self.sock_client.close()
        self.sock_server.close()

    def test_encode_and_decode_message(self) -> None:
        """Verify normal transmission of structured JSON control messages."""
        msg = {
            "command": CMD_UPLOAD,
            "filename": "document.pdf",
            "size": 1048576,
            "overwrite": False,
        }
        send_message(self.sock_client, msg)
        received = recv_message(self.sock_server)
        self.assertEqual(received, msg)

    def test_partial_header_handling(self) -> None:
        """Verify recv_exact handles partial header reads fragmented over the stream."""
        msg = {"command": CMD_LIST}
        payload = json.dumps(msg).encode("utf-8")
        header = struct.pack(HEADER_FORMAT, len(payload))
        full_packet = header + payload

        def send_fragmented():
            # Send 2 bytes of header, then remaining 2 bytes + payload
            self.sock_client.sendall(full_packet[:2])
            time.sleep(0.05)
            self.sock_client.sendall(full_packet[2:])

        sender = threading.Thread(target=send_fragmented)
        sender.start()

        received = recv_message(self.sock_server)
        sender.join()

        self.assertEqual(received, msg)

    def test_partial_body_handling(self) -> None:
        """Verify recv_message handles payloads received in single-byte chunks."""
        msg = {"command": CMD_UPLOAD, "test_data": "A" * 200}
        payload = json.dumps(msg).encode("utf-8")
        header = struct.pack(HEADER_FORMAT, len(payload))

        def send_trickle():
            self.sock_client.sendall(header)
            for byte in payload:
                self.sock_client.sendall(bytes([byte]))
                time.sleep(0.001)

        sender = threading.Thread(target=send_trickle)
        sender.start()

        received = recv_message(self.sock_server)
        sender.join()

        self.assertEqual(received, msg)

    def test_disconnected_peer_detection(self) -> None:
        """Verify ConnectionClosedError is raised when the remote peer closes cleanly before sending."""
        self.sock_client.close()
        with self.assertRaises(ConnectionClosedError):
            recv_message(self.sock_server)

    def test_premature_disconnect_during_payload(self) -> None:
        """Verify ProtocolError is raised if peer closes halfway through declared message."""
        fake_header = struct.pack(HEADER_FORMAT, 100)
        self.sock_client.sendall(fake_header + b"12345")  # Send only 5 of 100 bytes
        self.sock_client.close()

        with self.assertRaises(ProtocolError):
            recv_message(self.sock_server)

    def test_oversized_control_message_rejection(self) -> None:
        """Verify that headers declaring payload exceeding MAX_CONTROL_MESSAGE_SIZE are rejected."""
        oversized = MAX_CONTROL_MESSAGE_SIZE + 1024
        header = struct.pack(HEADER_FORMAT, oversized)
        self.sock_client.sendall(header)

        with self.assertRaises(ProtocolError) as ctx:
            recv_message(self.sock_server)
        self.assertIn("exceeds limit", str(ctx.exception))

    def test_malformed_json_rejection(self) -> None:
        """Verify ProtocolError is raised if payload is not valid JSON."""
        bad_json = b"{invalid_json_payload: not closed"
        header = struct.pack(HEADER_FORMAT, len(bad_json))
        self.sock_client.sendall(header + bad_json)

        with self.assertRaises(ProtocolError) as ctx:
            recv_message(self.sock_server)
        self.assertIn("Malformed JSON", str(ctx.exception))


class TestFileStreamIOMock(unittest.TestCase):
    """Tests for file streaming and chunked transfer over socketpair."""

    def setUp(self) -> None:
        self.sock_client, self.sock_server = socket.socketpair()
        self.sock_client.settimeout(2.0)
        self.sock_server.settimeout(2.0)
        self.test_dir = tempfile.TemporaryDirectory()
        self.work_path = Path(self.test_dir.name)

    def tearDown(self) -> None:
        self.sock_client.close()
        self.sock_server.close()
        self.test_dir.cleanup()

    def test_zero_byte_file_transfer(self) -> None:
        """Verify zero-byte file transfers complete cleanly and create a 0-byte file."""
        src = self.work_path / "empty.txt"
        src.touch()
        dst = self.work_path / "empty_out.txt"

        def client_send():
            send_file_stream(self.sock_client, src, file_size=0)

        sender = threading.Thread(target=client_send)
        sender.start()

        recv_file_stream(self.sock_server, dst, file_size=0)
        sender.join()

        self.assertTrue(dst.exists())
        self.assertEqual(dst.stat().st_size, 0)

    def test_partial_file_cleanup_on_interrupt(self) -> None:
        """Verify partial .part file is deleted if socket connection drops mid-transfer."""
        dst = self.work_path / "interrupted.bin"
        declared_size = 10000

        # Server sends only 200 bytes, then terminates connection
        def server_half_send():
            self.sock_server.sendall(b"X" * 200)
            self.sock_server.close()

        sender = threading.Thread(target=server_half_send)
        sender.start()

        with self.assertRaises(ProtocolError):
            recv_file_stream(self.sock_client, dst, file_size=declared_size)

        sender.join()
        # Verify both target file and .part file are removed
        self.assertFalse(dst.exists())
        self.assertFalse(dst.with_suffix(".bin.part").exists())


if __name__ == "__main__":
    unittest.main()
