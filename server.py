"""Multithreaded Network File Transfer Server.

Implements a multithreaded TCP server where each connected client is handled
in an independent thread. Supports concurrent file uploads, downloads,
file listings, and safe filename validation.
"""

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List
import argparse
import logging
import os
import socket
import sys
import threading
import time

from config import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    LISTEN_BACKLOG,
    LOGS_DIR,
    MAX_FILE_SIZE,
    SERVER_STORAGE_DIR,
    SOCKET_TIMEOUT,
    ensure_directories_exist,
    is_safe_path,
    sanitize_filename,
)
from protocol import (
    CMD_DOWNLOAD,
    CMD_LIST,
    CMD_QUIT,
    CMD_UPLOAD,
    ConnectionClosedError,
    ProtocolError,
    make_error_response,
    make_ok_response,
    recv_file_stream,
    recv_message,
    send_file_stream,
    send_message,
)


class FileLockManager:
    """Manages fine-grained, per-file locks for concurrency control.

    Ensures that concurrent transfers of different files proceed in parallel,
    while simultaneous operations on the same file are properly serialized to
    prevent race conditions and file corruption.
    """

    def __init__(self) -> None:
        self._master_lock = threading.Lock()
        self._file_locks: Dict[str, threading.Lock] = {}
        self._ref_counts: Dict[str, int] = {}

    @contextmanager
    def acquire(self, filename: str):
        """Context manager to acquire an exclusive lock on a specific filename."""
        with self._master_lock:
            if filename not in self._file_locks:
                self._file_locks[filename] = threading.Lock()
                self._ref_counts[filename] = 0
            self._ref_counts[filename] += 1
            file_lock = self._file_locks[filename]

        file_lock.acquire()
        try:
            yield
        finally:
            file_lock.release()
            with self._master_lock:
                self._ref_counts[filename] -= 1
                if self._ref_counts[filename] == 0:
                    del self._file_locks[filename]
                    del self._ref_counts[filename]


class FileTransferServer:
    """High-performance multithreaded TCP File Transfer Server."""

    def __init__(
        self,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        storage_dir: Path = SERVER_STORAGE_DIR,
    ) -> None:
        self.host = host
        self.port = port
        self.storage_dir = Path(storage_dir).resolve()
        self.file_locks = FileLockManager()
        self.is_running = False
        self.server_socket: socket.socket | None = None
        self.client_threads: List[threading.Thread] = []
        self._threads_lock = threading.Lock()
        self.logger = logging.getLogger("FileServer")

    def start(self) -> None:
        """Bind TCP socket, start listening, and loop accepting client connections."""
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        self.is_running = True

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Enable SO_REUSEADDR for rapid restart during testing and development
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(LISTEN_BACKLOG)
            # Short timeout on listening socket allows checking self.is_running periodically
            self.server_socket.settimeout(1.0)
            self.logger.info(
                "============================================================"
            )
            self.logger.info("Multithreaded Network File Transfer Server Started")
            self.logger.info(f"Listening on TCP: {self.host}:{self.port}")
            self.logger.info(f"Storage Directory: {self.storage_dir}")
            self.logger.info(
                "============================================================"
            )
        except OSError as err:
            self.logger.error(f"Failed to bind server to {self.host}:{self.port}: {err}")
            self.stop()
            raise

        try:
            while self.is_running:
                try:
                    client_socket, client_address = self.server_socket.accept()
                except socket.timeout:
                    continue
                except OSError:
                    if not self.is_running:
                        break
                    raise

                self.logger.info(
                    f"[CONNECT] New client connection accepted from {client_address[0]}:{client_address[1]}"
                )

                # Spawn a dedicated worker thread for each connected client
                client_thread = threading.Thread(
                    target=self._handle_client,
                    args=(client_socket, client_address),
                    name=f"ClientWorker-{client_address[0]}:{client_address[1]}",
                    daemon=True,
                )
                with self._threads_lock:
                    self.client_threads.append(client_thread)
                client_thread.start()

        except KeyboardInterrupt:
            self.logger.info("KeyboardInterrupt received. Initiating server shutdown...")
        finally:
            self.stop()

    def stop(self) -> None:
        """Gracefully stop server socket and clean up active threads."""
        if not self.is_running and self.server_socket is None:
            return

        self.logger.info("Shutting down server...")
        self.is_running = False

        if self.server_socket:
            try:
                self.server_socket.close()
            except OSError:
                pass
            self.server_socket = None

        with self._threads_lock:
            active_threads = [t for t in self.client_threads if t.is_alive()]
        if active_threads:
            self.logger.info(
                f"Waiting for {len(active_threads)} active client transfers to finish..."
            )
            for t in active_threads:
                t.join(timeout=2.0)

        self.logger.info("Server stopped successfully.")

    def _handle_client(
        self, client_socket: socket.socket, client_address: tuple[str, int]
    ) -> None:
        """Handle all requests from an individual client connection."""
        addr_str = f"{client_address[0]}:{client_address[1]}"
        # Set socket timeout for idle communication to detect dropped connections
        client_socket.settimeout(SOCKET_TIMEOUT)

        try:
            while self.is_running:
                try:
                    request = recv_message(client_socket)
                except ConnectionClosedError:
                    self.logger.info(f"[DISCONNECT] Client {addr_str} disconnected cleanly.")
                    break
                except socket.timeout:
                    self.logger.warning(f"[TIMEOUT] Client {addr_str} timed out. Closing connection.")
                    break
                except ProtocolError as p_err:
                    self.logger.warning(f"[PROTOCOL ERROR] From {addr_str}: {p_err}")
                    try:
                        send_message(
                            client_socket,
                            make_error_response(f"Protocol error: {p_err}", code=400),
                        )
                    except OSError:
                        pass
                    break

                command = request.get("command")
                self.logger.info(f"[REQUEST] {addr_str} -> Command: {command}")

                if command == CMD_LIST:
                    self._handle_list(client_socket, addr_str)
                elif command == CMD_UPLOAD:
                    self._handle_upload(client_socket, request, addr_str)
                elif command == CMD_DOWNLOAD:
                    self._handle_download(client_socket, request, addr_str)
                elif command == CMD_QUIT:
                    self.logger.info(f"[QUIT] Client {addr_str} issued QUIT command.")
                    send_message(client_socket, make_ok_response("Goodbye"))
                    break
                else:
                    self.logger.warning(f"[INVALID] Unknown command '{command}' from {addr_str}")
                    send_message(
                        client_socket,
                        make_error_response(f"Unknown command: '{command}'", code=400),
                    )

        except (ConnectionResetError, ConnectionAbortedError) as err:
            self.logger.warning(f"[CONNECTION RESET] Client {addr_str} aborted: {err}")
        except Exception as exc:
            self.logger.error(
                f"[UNEXPECTED ERROR] Exception while handling {addr_str}: {exc}",
                exc_info=True,
            )
            try:
                send_message(
                    client_socket,
                    make_error_response("Internal server error occurred.", code=500),
                )
            except OSError:
                pass
        finally:
            try:
                client_socket.close()
            except OSError:
                pass
            with self._threads_lock:
                if threading.current_thread() in self.client_threads:
                    self.client_threads.remove(threading.current_thread())
            self.logger.info(f"[CLOSED] Connection with {addr_str} closed.")

    def _handle_list(self, client_socket: socket.socket, addr_str: str) -> None:
        """Handle LIST command: scan server_storage and return metadata of regular files."""
        files_info: List[Dict[str, Any]] = []

        try:
            for item in self.storage_dir.iterdir():
                # Ignore subdirectories, temporary .part files, and hidden files
                if item.is_file() and not item.name.startswith(".") and not item.name.endswith(".part"):
                    stat = item.stat()
                    ext = item.suffix.lower()
                    file_type = ext[1:] if ext else "unknown"
                    files_info.append(
                        {
                            "name": item.name,
                            "size": stat.st_size,
                            "type": file_type,
                            "modified": time.strftime(
                                "%Y-%m-%d %H:%M:%S", time.localtime(stat.st_mtime)
                            ),
                        }
                    )

            files_info.sort(key=lambda x: x["name"].lower())
            self.logger.info(f"[LIST] Sending {len(files_info)} file entries to {addr_str}")
            send_message(
                client_socket,
                make_ok_response(
                    f"Retrieved {len(files_info)} files.",
                    data={"files": files_info, "count": len(files_info)},
                ),
            )
        except OSError as err:
            self.logger.error(f"[LIST ERROR] Could not read storage directory: {err}")
            send_message(
                client_socket,
                make_error_response(f"Failed to list directory: {err}", code=500),
            )

    def _handle_upload(
        self, client_socket: socket.socket, request: Dict[str, Any], addr_str: str
    ) -> None:
        """Handle UPLOAD command: receive file metadata and chunked stream into server_storage."""
        raw_filename = request.get("filename")
        declared_size = request.get("size")
        allow_overwrite = bool(request.get("overwrite", False))

        if not raw_filename or declared_size is None:
            send_message(
                client_socket,
                make_error_response(
                    "Missing 'filename' or 'size' parameter for UPLOAD.", code=400
                ),
            )
            return

        try:
            filename = sanitize_filename(raw_filename)
        except ValueError as err:
            self.logger.warning(f"[UPLOAD REJECT] Malformed filename from {addr_str}: {err}")
            send_message(client_socket, make_error_response(str(err), code=400))
            return

        target_path = self.storage_dir / filename
        if not is_safe_path(self.storage_dir, target_path):
            self.logger.warning(
                f"[SECURITY ALERT] Path traversal attempted by {addr_str}: '{raw_filename}'"
            )
            send_message(
                client_socket,
                make_error_response("Access denied: Invalid filename path.", code=403),
            )
            return

        if not isinstance(declared_size, int) or declared_size < 0:
            send_message(
                client_socket,
                make_error_response("Invalid file size parameter.", code=400),
            )
            return

        if declared_size > MAX_FILE_SIZE:
            send_message(
                client_socket,
                make_error_response(
                    f"File exceeds maximum allowed size ({MAX_FILE_SIZE} bytes).",
                    code=413,
                ),
            )
            return

        # Acquire lock for this filename to prevent concurrent overwrite collisions
        with self.file_locks.acquire(filename):
            if target_path.exists() and not allow_overwrite:
                self.logger.warning(
                    f"[UPLOAD REJECT] File '{filename}' already exists on server."
                )
                send_message(
                    client_socket,
                    make_error_response(
                        f"File '{filename}' already exists on server.",
                        code=409,
                        data={"exists": True, "filename": filename},
                    ),
                )
                return

            # Acknowledge readiness to receive file stream
            self.logger.info(
                f"[UPLOAD START] Receiving '{filename}' ({declared_size} bytes) from {addr_str}"
            )
            send_message(
                client_socket,
                make_ok_response(
                    "Ready to receive file data.",
                    data={"filename": filename, "expected_size": declared_size},
                ),
            )

            # Receive chunked file stream safely
            try:
                bytes_received = recv_file_stream(
                    sock=client_socket,
                    destination_path=target_path,
                    file_size=declared_size,
                )
                self.logger.info(
                    f"[UPLOAD COMPLETE] Successfully received '{filename}' ({bytes_received} bytes) from {addr_str}"
                )
                send_message(
                    client_socket,
                    make_ok_response(
                        f"File '{filename}' uploaded successfully.",
                        data={"filename": filename, "received_size": bytes_received},
                    ),
                )
            except Exception as exc:
                self.logger.error(
                    f"[UPLOAD FAILED] Incomplete upload of '{filename}' from {addr_str}: {exc}"
                )
                try:
                    send_message(
                        client_socket,
                        make_error_response(f"Upload failed: {exc}", code=500),
                    )
                except OSError:
                    pass

    def _handle_download(
        self, client_socket: socket.socket, request: Dict[str, Any], addr_str: str
    ) -> None:
        """Handle DOWNLOAD command: validate file existence, send metadata, and stream chunks."""
        raw_filename = request.get("filename")
        if not raw_filename:
            send_message(
                client_socket,
                make_error_response("Missing 'filename' parameter.", code=400),
            )
            return

        try:
            filename = sanitize_filename(raw_filename)
        except ValueError as err:
            self.logger.warning(f"[DOWNLOAD REJECT] Invalid filename from {addr_str}: {err}")
            send_message(client_socket, make_error_response(str(err), code=400))
            return

        target_path = self.storage_dir / filename
        if not is_safe_path(self.storage_dir, target_path):
            self.logger.warning(
                f"[SECURITY ALERT] Path traversal attempted by {addr_str}: '{raw_filename}'"
            )
            send_message(
                client_socket,
                make_error_response("Access denied: Invalid filename path.", code=403),
            )
            return

        with self.file_locks.acquire(filename):
            if not target_path.is_file():
                self.logger.warning(
                    f"[DOWNLOAD NOT FOUND] Requested file '{filename}' does not exist."
                )
                send_message(
                    client_socket,
                    make_error_response(
                        f"File '{filename}' not found on server.", code=404
                    ),
                )
                return

            try:
                file_size = target_path.stat().st_size
            except OSError as err:
                self.logger.error(f"[DOWNLOAD ERROR] Cannot read stat for '{filename}': {err}")
                send_message(
                    client_socket,
                    make_error_response(f"Unable to access file: {err}", code=500),
                )
                return

            self.logger.info(
                f"[DOWNLOAD START] Serving '{filename}' ({file_size} bytes) to {addr_str}"
            )
            # Send file metadata first
            send_message(
                client_socket,
                make_ok_response(
                    "File found. Transfer commencing.",
                    data={"filename": filename, "size": file_size},
                ),
            )

            # Wait for client acknowledgment before sending stream
            try:
                ack = recv_message(client_socket)
                if ack.get("status") != "OK":
                    self.logger.warning(
                        f"[DOWNLOAD CANCELLED] Client {addr_str} cancelled download: {ack.get('message')}"
                    )
                    return
            except Exception as exc:
                self.logger.warning(f"[DOWNLOAD ACK ERROR] From {addr_str}: {exc}")
                return

            # Stream chunks to client
            try:
                bytes_sent = send_file_stream(
                    sock=client_socket,
                    file_path=target_path,
                    file_size=file_size,
                )
                self.logger.info(
                    f"[DOWNLOAD COMPLETE] Successfully sent '{filename}' ({bytes_sent} bytes) to {addr_str}"
                )
            except Exception as exc:
                self.logger.error(
                    f"[DOWNLOAD FAILED] Error streaming '{filename}' to {addr_str}: {exc}"
                )


def setup_server_logging(log_to_file: bool = True) -> None:
    """Configure thread-safe logging with timestamps and thread names."""
    ensure_directories_exist()
    formatter = logging.Formatter(
        "[%(asctime)s] [%(threadName)-24s] [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    root_logger = logging.getLogger("FileServer")
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()

    # Console stdout handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Optional File handler inside logs/ directory
    if log_to_file:
        file_handler = logging.FileHandler(
            LOGS_DIR / "server.log", mode="a", encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)


def main() -> None:
    """Command-line entrypoint for the file transfer server."""
    parser = argparse.ArgumentParser(
        description="Multithreaded Network File Transfer Server (TCP/IP)"
    )
    parser.add_argument(
        "--host",
        type=str,
        default=DEFAULT_HOST,
        help=f"IP address to bind the server to (default: {DEFAULT_HOST}. Use 0.0.0.0 for LAN).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help=f"Port number to listen on (default: {DEFAULT_PORT}).",
    )
    parser.add_argument(
        "--storage",
        type=str,
        default=str(SERVER_STORAGE_DIR),
        help=f"Directory for server storage (default: {SERVER_STORAGE_DIR}).",
    )
    args = parser.parse_args()

    setup_server_logging()
    server = FileTransferServer(
        host=args.host, port=args.port, storage_dir=Path(args.storage)
    )
    server.start()


if __name__ == "__main__":
    main()
