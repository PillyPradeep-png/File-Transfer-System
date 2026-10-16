"""User Account Management, Authentication, and Unique ID Generation.

Handles user registration, automatic unique User ID generation, secure password hashing,
authentication, and resolution by either Username or User ID. Pre-seeds two dummy test accounts.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional
import hashlib
import json
import os
import secrets
import threading
import time

USERS_DB_PATH = Path(__file__).resolve().parent / "users_db.json"


class UserManager:
    """Thread-safe persistent user account registry and authenticator."""

    def __init__(self, db_path: Path = USERS_DB_PATH) -> None:
        self.db_path = db_path
        self._lock = threading.Lock()
        self.users: Dict[str, Dict[str, Any]] = {}  # user_id -> user_dict
        self._load_or_seed()

    def _hash_password(self, password: str, salt: Optional[str] = None) -> tuple[str, str]:
        """Hash a password using PBKDF2-HMAC-SHA256 with cryptographic salt."""
        if not salt:
            salt = secrets.token_hex(16)
        pwd_hash = hashlib.pbkdf2_hmac(
            "sha256", password.encode("utf-8"), salt.encode("utf-8"), 100000
        ).hex()
        return pwd_hash, salt

    def _load_or_seed(self) -> None:
        """Load user database from disk or seed default dummy accounts (Alice & Bob)."""
        with self._lock:
            if self.db_path.exists():
                try:
                    with open(self.db_path, "r", encoding="utf-8") as f:
                        self.users = json.load(f)
                    if self.users:
                        return
                except (json.JSONDecodeError, OSError):
                    self.users = {}

            # Seed default dummy accounts for demonstration and testing
            self.users = {}
            self._create_user_internal("alice", "password123", force_uid="UID-1001")
            self._create_user_internal("bob", "password123", force_uid="UID-1002")
            self._save()

    def _save(self) -> None:
        """Persist user registry to JSON file safely."""
        try:
            temp_file = self.db_path.with_suffix(".tmp")
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(self.users, f, indent=2)
            temp_file.replace(self.db_path)
        except OSError:
            pass

    def _generate_user_id(self) -> str:
        """Generate next sequential or unique user ID (e.g. UID-1003)."""
        existing_numbers = []
        for uid in self.users.keys():
            if uid.startswith("UID-") and uid[4:].isdigit():
                existing_numbers.append(int(uid[4:]))
        next_num = max(existing_numbers, default=1000) + 1
        return f"UID-{next_num}"

    def _create_user_internal(
        self, username: str, password: str, force_uid: Optional[str] = None
    ) -> Dict[str, Any]:
        clean_user = username.strip().lower()
        pwd_hash, salt = self._hash_password(password)
        uid = force_uid or self._generate_user_id()
        user_record = {
            "user_id": uid,
            "username": clean_user,
            "password_hash": pwd_hash,
            "salt": salt,
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        self.users[uid] = user_record
        return user_record

    def register(self, username: str, password: str) -> Dict[str, Any]:
        """Register a new user with a unique username and auto-generated User ID.

        Args:
            username: User-selected display name (case-insensitive uniqueness).
            password: User secret password.

        Returns:
            Dictionary with user_id, username, and created_at.

        Raises:
            ValueError: If username is empty or already exists, or password too short.
        """
        clean_user = username.strip()
        if not clean_user:
            raise ValueError("Username cannot be empty.")
        if len(clean_user) < 2 or len(clean_user) > 30:
            raise ValueError("Username must be between 2 and 30 characters.")
        if not password or len(password) < 4:
            raise ValueError("Password must be at least 4 characters long.")

        with self._lock:
            # Check for unique username
            for u in self.users.values():
                if u["username"].lower() == clean_user.lower():
                    raise ValueError(f"Username '{clean_user}' is already taken.")

            user_record = self._create_user_internal(clean_user, password)
            self._save()

            return {
                "user_id": user_record["user_id"],
                "username": user_record["username"],
                "created_at": user_record["created_at"],
            }

    def authenticate(self, identifier: str, password: str) -> Optional[Dict[str, Any]]:
        """Authenticate by either Username or User ID.

        Args:
            identifier: Either user_id (e.g. UID-1001) or username (e.g. alice).
            password: The raw password to verify.

        Returns:
            User dictionary without password/salt if valid, None otherwise.
        """
        clean_id = identifier.strip().lower()
        with self._lock:
            found_user = None
            for u in self.users.values():
                if u["user_id"].lower() == clean_id or u["username"].lower() == clean_id:
                    found_user = u
                    break

            if not found_user:
                return None

            expected_hash, _ = self._hash_password(password, found_user["salt"])
            if secrets.compare_digest(found_user["password_hash"], expected_hash):
                return {
                    "user_id": found_user["user_id"],
                    "username": found_user["username"],
                    "created_at": found_user["created_at"],
                }
            return None

    def find_user(self, identifier: str) -> Optional[Dict[str, Any]]:
        """Resolve either a User ID or Username to a public user record."""
        clean_id = identifier.strip().lower()
        with self._lock:
            for u in self.users.values():
                if u["user_id"].lower() == clean_id or u["username"].lower() == clean_id:
                    return {
                        "user_id": u["user_id"],
                        "username": u["username"],
                    }
        return None

    def list_users(self) -> List[Dict[str, str]]:
        """Return public list of all registered users."""
        with self._lock:
            return [
                {"user_id": u["user_id"], "username": u["username"]}
                for u in sorted(self.users.values(), key=lambda x: x["user_id"])
            ]


# Singleton instance
user_manager = UserManager()
