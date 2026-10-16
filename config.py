"""Configuration parameters for the Multithreaded Network File Transfer System.

Defines default network addresses, ports, chunk sizes, buffer limits,
timeout intervals, and directory storage locations.
"""

from pathlib import Path
import os
import re

# Network settings
DEFAULT_HOST: str = "127.0.0.1"
DEFAULT_PORT: int = 5000
LISTEN_BACKLOG: int = 50

# Transfer buffer and protocol constraints
BUFFER_SIZE: int = 64 * 1024  # 64 KB chunk size for streaming file I/O
MAX_CONTROL_MESSAGE_SIZE: int = 64 * 1024  # 64 KB max control JSON length
MAX_FILE_SIZE: int = 5 * 1024 * 1024 * 1024  # 5 GB file transfer limit
SOCKET_TIMEOUT: float = 30.0  # Socket timeout in seconds for idle operations

# Base Directories (Relative to project root or configured directory)
BASE_DIR = Path(__file__).resolve().parent
SERVER_STORAGE_DIR = BASE_DIR / "server_storage"
CLIENT_DOWNLOAD_DIR = BASE_DIR / "downloads"
LOGS_DIR = BASE_DIR / "logs"


def ensure_directories_exist() -> None:
    """Create essential operational directories if they do not exist."""
    for directory in (SERVER_STORAGE_DIR, CLIENT_DOWNLOAD_DIR, LOGS_DIR):
        directory.mkdir(parents=True, exist_ok=True)


def sanitize_filename(filename: str) -> str:
    """Sanitize client-provided filename to prevent directory traversal or malformed names.

    Extracts the basename and strips dangerous characters.
    """
    if not filename:
        raise ValueError("Filename cannot be empty.")

    # Extract base name to strip any directory elements (both / and \)
    cleaned = os.path.basename(filename.replace("\\", "/"))
    # Remove null bytes and path separators
    cleaned = cleaned.replace("\0", "").strip()

    # Disallow relative traversal tokens explicitly
    if cleaned in ("", ".", ".."):
        raise ValueError(f"Invalid filename: '{filename}'")

    # Limit filename length to a safe boundary
    if len(cleaned) > 255:
        cleaned = cleaned[:255]

    return cleaned


def is_safe_path(base_directory: Path, target_path: Path) -> bool:
    """Verify that target_path resolves strictly within base_directory.

    Prevents path traversal attacks (e.g. symlink escapes, relative paths).
    """
    try:
        resolved_base = base_directory.resolve()
        resolved_target = target_path.resolve()
        # Ensure target starts with the base directory path
        return (
            resolved_target == resolved_base
            or resolved_base in resolved_target.parents
        )
    except (ValueError, RuntimeError):
        return False
