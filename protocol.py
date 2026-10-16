"""TCP Protocol implementation for the Multithreaded Network File Transfer System.

Defines message framing (4-byte length prefix + UTF-8 JSON payload),
exact byte transmission helpers, streaming chunked file I/O, progress tracking,
and standard request/response message formats.
"""

from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple
import json
import socket
import struct
import time

from config import (
    BUFFER_SIZE,
    MAX_CONTROL_MESSAGE_SIZE,
    MAX_FILE_SIZE,
)

# Protocol framing constants
HEADER_FORMAT = "!I"  # 4-byte unsigned big-endian integer
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

# Standard protocol operations
CMD_LIST = "LIST"
CMD_UPLOAD = "UPLOAD"
CMD_DOWNLOAD = "DOWNLOAD"
CMD_QUIT = "QUIT"

# Two-way user-to-user transfer operations & delivery confirmation
CMD_REGISTER = "REGISTER"          # Bind client connection to a username (e.g. Alice, Bob)
CMD_USERS = "USERS"                # Retrieve list of online/known users
CMD_SEND_TO = "SEND_TO"            # Send a file directly targeted to a specific recipient
CMD_INBOX = "INBOX"                # Query incoming files sent to current user
CMD_OUTBOX = "OUTBOX"              # Query sent files with end-to-end delivery receipts
CMD_RECEIVE_INBOX = "RECEIVE_INBOX" # Download a file from inbox & trigger recipient ACK

# Standard response statuses
STATUS_OK = "OK"
STATUS_ERROR = "ERROR"

# Delivery receipt statuses
DELIVERY_PENDING = "PENDING_RECEIPT"     # Received by server, waiting for recipient to download
DELIVERY_CONFIRMED = "RECEIVED_BY_USER"  # Recipient has downloaded & confirmed receipt

# Type alias for progress callback: (bytes_transferred, total_bytes, percent, speed_bps)
ProgressCallback = Callable[[int, int, float, float], None]


class ProtocolError(Exception):
    """Raised when a protocol framing or formatting rule is violated."""
    pass


class ConnectionClosedError(Exception):
    """Raised when the remote peer gracefully closes the connection."""
    pass


def recv_exact(sock: socket.socket, num_bytes: int) -> bytes:
    """Read exactly num_bytes from a TCP stream.

    TCP is a byte stream and may return fragments of varying sizes.
    Loops until all requested bytes are gathered or the peer closes.

    Args:
        sock: The active TCP socket.
        num_bytes: The exact number of bytes required.

    Returns:
        The accumulated bytes of length num_bytes.

    Raises:
        ConnectionClosedError: If connection is closed before any bytes are read.
        ProtocolError: If connection is closed after partial bytes are read.
        socket.timeout: If socket timeout triggers before bytes are read.
    """
    if num_bytes < 0:
        raise ValueError(f"Requested bytes cannot be negative: {num_bytes}")
    if num_bytes == 0:
        return b""

    chunks = bytearray()
    while len(chunks) < num_bytes:
        remaining = num_bytes - len(chunks)
        try:
            chunk = sock.recv(min(remaining, BUFFER_SIZE))
        except (ConnectionResetError, ConnectionAbortedError) as err:
            raise ProtocolError(f"Connection aborted by peer: {err}") from err

        if not chunk:
            if len(chunks) == 0:
                raise ConnectionClosedError("Remote peer closed the connection.")
            raise ProtocolError(
                f"Connection closed prematurely. Expected {num_bytes} bytes, received {len(chunks)}."
            )
        chunks.extend(chunk)

    return bytes(chunks)


def send_message(sock: socket.socket, message: Dict[str, Any]) -> None:
    """Serialize and send a JSON control message with a 4-byte length prefix.

    Args:
        sock: The destination TCP socket.
        message: Dictionary representing the protocol command/response.

    Raises:
        ProtocolError: If payload exceeds MAX_CONTROL_MESSAGE_SIZE.
    """
    try:
        payload_bytes = json.dumps(message, ensure_ascii=False).encode("utf-8")
    except (TypeError, ValueError) as err:
        raise ProtocolError(f"Failed to encode JSON message: {err}") from err

    payload_len = len(payload_bytes)
    if payload_len > MAX_CONTROL_MESSAGE_SIZE:
        raise ProtocolError(
            f"Control message exceeds max size ({payload_len} > {MAX_CONTROL_MESSAGE_SIZE} bytes)."
        )

    header = struct.pack(HEADER_FORMAT, payload_len)
    sock.sendall(header + payload_bytes)


def recv_message(sock: socket.socket) -> Dict[str, Any]:
    """Read and decode a framed JSON control message from the TCP stream.

    Args:
        sock: The active TCP socket.

    Returns:
        The deserialized message dictionary.

    Raises:
        ConnectionClosedError: If peer closed cleanly prior to header.
        ProtocolError: If framing header or JSON body is invalid/oversized.
    """
    header_data = recv_exact(sock, HEADER_SIZE)
    (payload_len,) = struct.unpack(HEADER_FORMAT, header_data)

    if payload_len > MAX_CONTROL_MESSAGE_SIZE:
        raise ProtocolError(
            f"Incoming control message declared size {payload_len} exceeds limit of {MAX_CONTROL_MESSAGE_SIZE} bytes."
        )

    payload_bytes = recv_exact(sock, payload_len)
    try:
        parsed = json.loads(payload_bytes.decode("utf-8"))
        if not isinstance(parsed, dict):
            raise ProtocolError("Received message payload is not a JSON object.")
        return parsed
    except json.JSONDecodeError as err:
        raise ProtocolError(f"Malformed JSON payload received: {err}") from err


def send_file_stream(
    sock: socket.socket,
    file_path: Path,
    file_size: int,
    chunk_size: int = BUFFER_SIZE,
    progress_callback: Optional[ProgressCallback] = None,
) -> int:
    """Stream file content in chunks through a TCP socket.

    Args:
        sock: The destination TCP socket.
        file_path: Path to the local file to read.
        file_size: Declared total size of the file in bytes.
        chunk_size: Size of read chunks (default 64 KB).
        progress_callback: Optional callback for progress display.

    Returns:
        Total bytes successfully sent.

    Raises:
        FileNotFoundError: If the source file does not exist.
        ProtocolError: If file changes size or EOF is reached prematurely.
    """
    bytes_sent = 0
    start_time = time.time()

    with open(file_path, "rb") as f:
        while bytes_sent < file_size:
            to_read = min(chunk_size, file_size - bytes_sent)
            chunk = f.read(to_read)
            if not chunk:
                raise ProtocolError(
                    f"Unexpected EOF while reading '{file_path.name}'. Sent {bytes_sent}/{file_size} bytes."
                )
            sock.sendall(chunk)
            bytes_sent += len(chunk)

            if progress_callback:
                elapsed = max(time.time() - start_time, 0.001)
                speed = bytes_sent / elapsed
                percent = (bytes_sent / file_size * 100.0) if file_size > 0 else 100.0
                progress_callback(bytes_sent, file_size, percent, speed)

    # Initial or zero-byte callback
    if file_size == 0 and progress_callback:
        progress_callback(0, 0, 100.0, 0.0)

    return bytes_sent


def recv_file_stream(
    sock: socket.socket,
    destination_path: Path,
    file_size: int,
    chunk_size: int = BUFFER_SIZE,
    progress_callback: Optional[ProgressCallback] = None,
) -> int:
    """Receive exact file_size bytes from a TCP socket and write to disk.

    Writes to a temporary `.part` file first to guarantee atomic write
    and prevent incomplete files if the transfer is interrupted.

    Args:
        sock: The source TCP socket.
        destination_path: Final target path for the received file.
        file_size: Total bytes expected to receive.
        chunk_size: Buffer size for socket reads.
        progress_callback: Optional callback for progress display.

    Returns:
        Total bytes successfully received.

    Raises:
        ProtocolError: If transfer fails or declared size limits are exceeded.
    """
    if file_size > MAX_FILE_SIZE:
        raise ProtocolError(
            f"File size {file_size} exceeds maximum allowable limit ({MAX_FILE_SIZE} bytes)."
        )

    destination_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = destination_path.with_name(f"{destination_path.name}.part")

    bytes_received = 0
    start_time = time.time()

    try:
        with open(temp_path, "wb") as f:
            while bytes_received < file_size:
                to_read = min(chunk_size, file_size - bytes_received)
                chunk = sock.recv(to_read)
                if not chunk:
                    raise ProtocolError(
                        f"Connection closed during file transfer. Received {bytes_received}/{file_size} bytes."
                    )
                f.write(chunk)
                bytes_received += len(chunk)

                if progress_callback:
                    elapsed = max(time.time() - start_time, 0.001)
                    speed = bytes_received / elapsed
                    percent = (bytes_received / file_size * 100.0) if file_size > 0 else 100.0
                    progress_callback(bytes_received, file_size, percent, speed)

        # Handle zero-byte file notification
        if file_size == 0 and progress_callback:
            progress_callback(0, 0, 100.0, 0.0)

        # Atomic replacement: rename temporary part file to target file
        if temp_path.exists():
            temp_path.replace(destination_path)

    except Exception:
        # Clean up partial/corrupted file on any failure
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass
        raise

    return bytes_received


def make_ok_response(message: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Helper to build standard success response."""
    payload: Dict[str, Any] = {
        "status": STATUS_OK,
        "code": 200,
        "message": message,
    }
    if data:
        payload["data"] = data
    return payload


def make_error_response(message: str, code: int = 400, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Helper to build standard error response."""
    payload: Dict[str, Any] = {
        "status": STATUS_ERROR,
        "code": code,
        "message": message,
    }
    if data:
        payload["data"] = data
    return payload
