"""Multithreaded Network File Transfer Client.

Provides an interactive CLI menu and programmatic client class to connect,
list server files, upload files with progress tracking, download files safely,
and handle disconnections gracefully.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional
import argparse
import os
import socket
import sys
import time

from config import (
    CLIENT_DOWNLOAD_DIR,
    DEFAULT_HOST,
    DEFAULT_PORT,
    SOCKET_TIMEOUT,
    ensure_directories_exist,
    is_safe_path,
    sanitize_filename,
)
from protocol import (
    CMD_DOWNLOAD,
    CMD_INBOX,
    CMD_LIST,
    CMD_LOGIN,
    CMD_OUTBOX,
    CMD_QUIT,
    CMD_RECEIVE_INBOX,
    CMD_REGISTER,
    CMD_SEND_TO,
    CMD_UPLOAD,
    CMD_USERS,
    DELIVERY_CONFIRMED,
    DELIVERY_PENDING,
    ConnectionClosedError,
    ProtocolError,
    make_error_response,
    make_ok_response,
    recv_file_stream,
    recv_message,
    send_file_stream,
    send_message,
)


def format_bytes(size: float) -> str:
    """Format byte counts into human-readable representation."""
    if size < 0:
        return "0 B"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024.0:
            return f"{size:6.2f} {unit}"
        size /= 1024.0
    return f"{size:6.2f} PB"


def display_progress(
    transferred: int, total: int, percent: float, speed_bps: float
) -> None:
    """Render a dynamic CLI progress bar with transferred bytes and transfer rate."""
    bar_width = 30
    if total > 0:
        filled_len = int(bar_width * (transferred / total))
        arrow = ">" if filled_len < bar_width else ""
        bar = "=" * filled_len + arrow
        bar = bar.ljust(bar_width)
    else:
        bar = "=" * bar_width

    speed_str = f"{format_bytes(speed_bps)}/s"
    progress_line = (
        f"\r[{bar}] {percent:5.1f}% "
        f"({format_bytes(transferred).strip()} / {format_bytes(total).strip()}) "
        f"@ {speed_str.strip()}    "
    )
    sys.stdout.write(progress_line)
    sys.stdout.flush()

    if transferred >= total and total > 0:
        sys.stdout.write("\n")
        sys.stdout.flush()


class FileTransferClient:
    """Client for TCP File Transfer operations with interactive and programmatic capabilities."""

    def __init__(
        self,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        download_dir: Path = CLIENT_DOWNLOAD_DIR,
    ) -> None:
        self.host = host
        self.port = port
        self.download_dir = Path(download_dir).resolve()
        self.sock: Optional[socket.socket] = None
        self.is_connected = False
        self.username: Optional[str] = None
        self.user_id: Optional[str] = None

    def register(self, username: str, password: str = "password123") -> bool:
        """Create new account with username and password; receives auto-generated unique User ID."""
        if not self.is_connected or not self.sock:
            print("[-] Error: You must connect to the server first.")
            return False

        try:
            send_message(
                self.sock,
                {"command": CMD_REGISTER, "username": username, "password": password},
            )
            resp = recv_message(self.sock)
            if resp.get("status") == "OK":
                data = resp.get("data", {})
                self.username = data.get("username", username)
                self.user_id = data.get("user_id")
                print(f"[+] Account registered successfully!")
                print(f"[+] Username: {self.username}")
                print(f"[+] Unique User ID: {self.user_id}")
                return True
            else:
                print(f"[-] Registration failed: {resp.get('message')}")
                return False
        except Exception as err:
            print(f"[-] Registration error: {err}")
            return False

    def login(self, identifier: str, password: str = "password123") -> bool:
        """Authenticate using either Username or unique User ID."""
        if not self.is_connected or not self.sock:
            print("[-] Error: You must connect to the server first.")
            return False

        try:
            send_message(
                self.sock,
                {"command": CMD_LOGIN, "identifier": identifier, "password": password},
            )
            resp = recv_message(self.sock)
            if resp.get("status") == "OK":
                data = resp.get("data", {})
                self.username = data.get("username", identifier)
                self.user_id = data.get("user_id")
                print(f"[+] Logged in successfully!")
                print(f"[+] Active User: {self.username} (User ID: {self.user_id})")
                return True
            else:
                print(f"[-] Login failed: {resp.get('message')}")
                return False
        except Exception as err:
            print(f"[-] Login error: {err}")
            return False

    def list_online_users(self) -> Optional[List[Dict[str, Any]]]:
        """Query and display list of users with both Username and User ID."""
        if not self.is_connected or not self.sock:
            print("[-] Error: You must connect to the server first.")
            return None

        try:
            send_message(self.sock, {"command": CMD_USERS})
            resp = recv_message(self.sock)
            if resp.get("status") != "OK":
                print(f"[-] Error: {resp.get('message')}")
                return None

            users = resp.get("data", {}).get("users", [])
            print("\n" + "=" * 55)
            print(f"{'USER ID':<14} {'USERNAME':<20} {'STATUS':<15}")
            print("-" * 55)
            if not users:
                print(" (No registered users found)")
            else:
                for u in users:
                    uid = u.get("user_id", "N/A")
                    uname = u.get("username", "unknown")
                    is_you = " (You)" if (uname == self.username or uid == self.user_id) else ""
                    online_badge = "Online" if u.get("is_online") else "Offline"
                    print(f"{uid:<14} {uname + is_you:<20} {online_badge:<15}")
            print("=" * 55 + "\n")
            return users
        except Exception as err:
            print(f"[-] Error fetching users: {err}")
            return None

    def send_file_to_user(
        self,
        recipient: str,
        local_path_str: str,
        show_progress: bool = True,
    ) -> bool:
        """Send a file targeted to another specific user, with delivery tracking."""
        if not self.is_connected or not self.sock:
            print("[-] Error: You must connect to the server first.")
            return False

        local_path = Path(local_path_str).expanduser().resolve()
        if not local_path.is_file():
            print(f"[-] Error: Source file '{local_path_str}' does not exist.")
            return False

        try:
            file_size = local_path.stat().st_size
        except OSError as err:
            print(f"[-] Error reading file stats: {err}")
            return False

        clean_filename = sanitize_filename(local_path.name)
        print(f"[*] Preparing two-way transfer of '{clean_filename}' ({format_bytes(file_size).strip()}) to '{recipient}'...")

        try:
            send_message(
                self.sock,
                {
                    "command": CMD_SEND_TO,
                    "sender": self.username,
                    "recipient": recipient,
                    "filename": clean_filename,
                    "size": file_size,
                },
            )

            resp = recv_message(self.sock)
            if resp.get("status") != "OK":
                print(f"[-] Transfer rejected: {resp.get('message')}")
                return False

            start_ts = time.time()
            callback = display_progress if show_progress else None
            sent = send_file_stream(
                sock=self.sock,
                file_path=local_path,
                file_size=file_size,
                progress_callback=callback,
            )

            completion = recv_message(self.sock)
            if completion.get("status") == "OK":
                elapsed = max(time.time() - start_ts, 0.001)
                speed = sent / elapsed
                print(
                    f"\n[+] Two-Way Transfer Sent! '{clean_filename}' delivered to server for '{recipient}'."
                )
                print(f"[ACK: LEVEL 1 RECEIPT - Server Stored & Queued for '{recipient}']")
                print(f"   Transfer ID: {completion.get('data', {}).get('transfer_id')}")
                print(f"   Rate: {format_bytes(speed).strip()}/s in {elapsed:.2f}s")
                print("   (Check 'View Outbox' later to see when the recipient downloads/receives it!)")
                return True
            else:
                print(f"\n[-] Server reported error: {completion.get('message')}")
                return False

        except Exception as err:
            print(f"\n[-] Transfer error: {err}")
            return False

    def view_inbox(self) -> Optional[List[Dict[str, Any]]]:
        """View incoming files sent to this user by other peers."""
        if not self.is_connected or not self.sock:
            print("[-] Error: You must connect to the server first.")
            return None

        try:
            send_message(self.sock, {"command": CMD_INBOX, "username": self.username})
            resp = recv_message(self.sock)
            if resp.get("status") != "OK":
                print(f"[-] Error: {resp.get('message')}")
                return None

            inbox = resp.get("data", {}).get("inbox", [])
            print("\n" + "=" * 85)
            print(f"INBOX FOR '{self.username or 'Current User'}' (Incoming Files)")
            print("=" * 85)
            print(f"{'TRANSFER ID':<20} {'FROM':<15} {'FILENAME':<24} {'SIZE':<10} {'STATUS':<14}")
            print("-" * 85)

            if not inbox:
                print(" (No incoming files in your inbox)")
            else:
                for item in inbox:
                    status_badge = "[NEW / UNREAD]" if item.get("status") == DELIVERY_PENDING else "[DOWNLOADED]"
                    print(
                        f"{item.get('transfer_id'):<20} {item.get('sender'):<15} {item.get('filename'):<24} {format_bytes(item.get('size', 0)).strip():<10} {status_badge:<14}"
                    )
            print("=" * 85 + "\n")
            return inbox
        except Exception as err:
            print(f"[-] Error fetching inbox: {err}")
            return None

    def view_outbox(self) -> Optional[List[Dict[str, Any]]]:
        """View sent files and real-time delivery confirmation receipts (Did recipient receive it?)."""
        if not self.is_connected or not self.sock:
            print("[-] Error: You must connect to the server first.")
            return None

        try:
            send_message(self.sock, {"command": CMD_OUTBOX, "username": self.username})
            resp = recv_message(self.sock)
            if resp.get("status") != "OK":
                print(f"[-] Error: {resp.get('message')}")
                return None

            outbox = resp.get("data", {}).get("outbox", [])
            print("\n" + "=" * 90)
            print(f"OUTBOX FOR '{self.username or 'Current User'}' (Sent Files & Delivery Confirmation Receipts)")
            print("=" * 90)
            print(f"{'TO':<14} {'FILENAME':<22} {'SENT AT':<18} {'DELIVERY STATUS'}")
            print("-" * 90)

            if not outbox:
                print(" (No outgoing transfers recorded yet)")
            else:
                for item in outbox:
                    recip = item.get("recipient", "Unknown")
                    fname = item.get("filename", "file")
                    sent_at = item.get("sent_at", "")
                    if item.get("status") == DELIVERY_CONFIRMED:
                        rcv_time = item.get("received_at", "N/A")
                        receipt_str = f"[CONFIRMED: RECEIVED BY {recip} at {rcv_time}]"
                    else:
                        receipt_str = f"[PENDING: SENT TO SERVER (Waiting for {recip} to download)]"

                    print(f"{recip:<14} {fname:<22} {sent_at:<18} {receipt_str}")
            print("=" * 90 + "\n")
            return outbox
        except Exception as err:
            print(f"[-] Error fetching outbox: {err}")
            return None

    def download_inbox_file(
        self,
        transfer_id: str,
        show_progress: bool = True,
    ) -> bool:
        """Download incoming file from inbox and trigger delivery confirmation back to sender."""
        if not self.is_connected or not self.sock:
            print("[-] Error: You must connect to the server first.")
            return False

        try:
            send_message(
                self.sock,
                {"command": CMD_RECEIVE_INBOX, "transfer_id": transfer_id, "username": self.username},
            )
            resp = recv_message(self.sock)
            if resp.get("status") != "OK":
                print(f"[-] Server error: {resp.get('message')}")
                return False

            data = resp.get("data", {})
            filename = data.get("filename", "received_file")
            file_size = data.get("size", 0)
            sender = data.get("sender", "Peer")

            target_path = self.download_dir / sanitize_filename(filename)
            print(f"[*] Downloading '{filename}' ({format_bytes(file_size).strip()}) sent by '{sender}'...")

            # Send readiness ACK
            send_message(self.sock, make_ok_response("Ready to receive stream"))

            start_ts = time.time()
            callback = display_progress if show_progress else None
            received = recv_file_stream(
                sock=self.sock,
                destination_path=target_path,
                file_size=file_size,
                progress_callback=callback,
            )

            elapsed = max(time.time() - start_ts, 0.001)
            speed = received / elapsed
            print(
                f"\n[+] Successfully received '{filename}' from '{sender}'!"
            )
            print(f"[+] Saved to: {target_path}")
            print(f"[CONFIRMED: DELIVERY RECEIPT - Notified sender '{sender}' that you received the file!]")
            return True

        except Exception as err:
            print(f"\n[-] Error receiving inbox file: {err}")
            return False



    def connect(self, host: Optional[str] = None, port: Optional[int] = None) -> bool:
        """Establish a TCP connection to the server."""
        if host:
            self.host = host
        if port:
            self.port = port

        if self.is_connected:
            print(f"[!] Already connected to server at {self.host}:{self.port}")
            return True

        self.download_dir.mkdir(parents=True, exist_ok=True)
        print(f"[*] Attempting to connect to {self.host}:{self.port}...")

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(SOCKET_TIMEOUT)
            sock.connect((self.host, self.port))
            self.sock = sock
            self.is_connected = True
            self.connected_time = time.time()
            print(f"[+] Successfully connected to {self.host}:{self.port}")
            return True
        except ConnectionRefusedError:
            print(
                f"[-] Connection failed: Server is not running or port {self.port} is blocked."
            )
            self._reset_connection()
            return False
        except socket.timeout:
            print(f"[-] Connection failed: Connection attempt to {self.host}:{self.port} timed out.")
            self._reset_connection()
            return False
        except Exception as err:
            print(f"[-] Connection error: {err}")
            self._reset_connection()
            return False

    def disconnect(self) -> None:
        """Safely disconnect from the server by notifying it with QUIT command."""
        if not self.is_connected or not self.sock:
            print("[!] Not currently connected to any server.")
            return

        try:
            send_message(self.sock, {"command": CMD_QUIT})
            # Await acknowledgment if possible
            self.sock.settimeout(2.0)
            recv_message(self.sock)
        except Exception:
            pass
        finally:
            self._reset_connection()
            print("[+] Disconnected safely from server.")

    def _reset_connection(self) -> None:
        """Internal cleanup of socket reference and connection status."""
        if self.sock:
            try:
                self.sock.close()
            except OSError:
                pass
        self.sock = None
        self.is_connected = False
        self.connected_time = None

    def list_files(self) -> Optional[List[Dict[str, Any]]]:
        """Request file listing from the server and display in tabular format."""
        if not self.is_connected or not self.sock:
            print("[-] Error: You must connect to the server first.")
            return None

        try:
            send_message(self.sock, {"command": CMD_LIST})
            response = recv_message(self.sock)

            if response.get("status") != "OK":
                print(f"[-] Server error: {response.get('message')}")
                return None

            files = response.get("data", {}).get("files", [])
            print("\n" + "=" * 70)
            print(f"{'FILENAME':<30} {'SIZE':<14} {'TYPE':<10} {'MODIFIED':<18}")
            print("=" * 70)

            if not files:
                print(" (No files currently hosted on server)")
            else:
                for f in files:
                    size_str = format_bytes(f.get("size", 0)).strip()
                    file_type = f.get("type", "file")
                    modified = f.get("modified", "N/A")
                    print(
                        f"{f.get('name', 'unknown'):<30} {size_str:<14} {file_type:<10} {modified:<18}"
                    )
            print("=" * 70 + "\n")
            return files

        except (ConnectionClosedError, ConnectionResetError, BrokenPipeError):
            print("[-] Connection lost while requesting file list.")
            self._reset_connection()
            return None
        except Exception as err:
            print(f"[-] Error listing files: {err}")
            return None

    def upload_file(
        self,
        local_path_str: str,
        overwrite: bool = False,
        show_progress: bool = True,
    ) -> bool:
        """Upload a local file to the server with chunked transfer and progress bar."""
        if not self.is_connected or not self.sock:
            print("[-] Error: You must connect to the server first.")
            return False

        local_path = Path(local_path_str).expanduser().resolve()
        if not local_path.is_file():
            print(f"[-] Error: Source file '{local_path_str}' does not exist or is not a file.")
            return False

        try:
            file_size = local_path.stat().st_size
        except OSError as err:
            print(f"[-] Error accessing file '{local_path}': {err}")
            return False

        clean_filename = sanitize_filename(local_path.name)
        print(f"[*] Preparing to upload '{clean_filename}' ({format_bytes(file_size).strip()})...")

        try:
            # 1. Send UPLOAD intent with filename and declared size
            send_message(
                self.sock,
                {
                    "command": CMD_UPLOAD,
                    "filename": clean_filename,
                    "size": file_size,
                    "overwrite": overwrite,
                },
            )

            # 2. Wait for server readiness or conflict response
            response = recv_message(self.sock)
            if response.get("status") != "OK":
                if response.get("code") == 409:
                    print(f"[-] Upload rejected: {response.get('message')}")
                    print("[!] Use the overwrite option if you want to replace it.")
                else:
                    print(f"[-] Upload failed: {response.get('message')}")
                return False

            # 3. Stream file chunks to server
            start_ts = time.time()
            callback = display_progress if show_progress else None
            sent = send_file_stream(
                sock=self.sock,
                file_path=local_path,
                file_size=file_size,
                progress_callback=callback,
            )

            # 4. Wait for server confirmation
            completion = recv_message(self.sock)
            if completion.get("status") == "OK":
                elapsed = max(time.time() - start_ts, 0.001)
                speed = sent / elapsed
                print(
                    f"\n[+] Upload complete: '{clean_filename}' ({format_bytes(sent).strip()}) transferred in {elapsed:.2f}s ({format_bytes(speed).strip()}/s)."
                )
                return True
            else:
                print(f"\n[-] Server reported error after transfer: {completion.get('message')}")
                return False

        except (ConnectionClosedError, ConnectionResetError, BrokenPipeError):
            print("\n[-] Connection lost during file upload.")
            self._reset_connection()
            return False
        except Exception as err:
            print(f"\n[-] Upload error: {err}")
            return False

    def download_file(
        self,
        remote_filename: str,
        overwrite: bool = False,
        show_progress: bool = True,
    ) -> bool:
        """Download a file from the server to the local downloads directory."""
        if not self.is_connected or not self.sock:
            print("[-] Error: You must connect to the server first.")
            return False

        try:
            clean_name = sanitize_filename(remote_filename)
        except ValueError as err:
            print(f"[-] Invalid filename: {err}")
            return False

        target_local_path = self.download_dir / clean_name
        if not is_safe_path(self.download_dir, target_local_path):
            print("[-] Download aborted: Filename resolves outside target directory.")
            return False

        # Collision check on local download folder
        if target_local_path.exists() and not overwrite:
            print(
                f"[!] File '{clean_name}' already exists in your local downloads folder ({target_local_path})."
            )
            return False

        try:
            # 1. Send DOWNLOAD request
            send_message(
                self.sock,
                {"command": CMD_DOWNLOAD, "filename": clean_name},
            )

            # 2. Wait for file metadata response
            response = recv_message(self.sock)
            if response.get("status") != "OK":
                print(f"[-] Server response: {response.get('message')}")
                return False

            file_info = response.get("data", {})
            file_size = file_info.get("size", 0)

            print(
                f"[*] Commencing download of '{clean_name}' ({format_bytes(file_size).strip()})..."
            )

            # 3. Acknowledge and proceed
            send_message(self.sock, make_ok_response("Ready to receive stream"))

            # 4. Stream chunks from server into target file
            start_ts = time.time()
            callback = display_progress if show_progress else None
            received = recv_file_stream(
                sock=self.sock,
                destination_path=target_local_path,
                file_size=file_size,
                progress_callback=callback,
            )

            elapsed = max(time.time() - start_ts, 0.001)
            speed = received / elapsed
            print(
                f"\n[+] Download complete: '{clean_name}' saved to '{target_local_path}' ({format_bytes(received).strip()} in {elapsed:.2f}s, {format_bytes(speed).strip()}/s)."
            )
            return True

        except (ConnectionClosedError, ConnectionResetError, BrokenPipeError):
            print("\n[-] Connection lost during download.")
            self._reset_connection()
            return False
        except Exception as err:
            print(f"\n[-] Download error: {err}")
            return False

    def view_status(self) -> None:
        """Display current network connection parameters and session duration."""
        print("\n" + "=" * 45)
        print("CONNECTION STATUS")
        print("=" * 45)
        print(f"Status:          {'CONNECTED' if self.is_connected else 'DISCONNECTED'}")
        print(f"Target Server:   {self.host}:{self.port}")
        print(f"Local Downloads: {self.download_dir}")
        if self.is_connected and self.connected_time:
            uptime = int(time.time() - self.connected_time)
            print(f"Session Uptime:  {uptime} seconds")
        print("=" * 45 + "\n")


def run_interactive_cli() -> None:
    """Run interactive Command-Line Interface menu loop with two-way transfer and delivery receipts."""
    ensure_directories_exist()
    client = FileTransferClient()

    while True:
        try:
            if client.is_connected and client.username:
                user_status = f"CONNECTED as '{client.username}'"
            elif client.is_connected:
                user_status = "CONNECTED (Anonymous)"
            else:
                user_status = "DISCONNECTED"

            menu = f"""
============================================================
           MULTITHREADED NETWORK FILE TRANSFER
           Status: {user_status}
============================================================
 [ TWO-WAY PEER-TO-PEER TRANSFERS WITH DELIVERY ACKS ]
  1. Connect / Login as Username
  2. Send File to Another User (Two-Way Targeted Transfer)
  3. View Inbox & Receive Files (Triggers Delivery ACK)
  4. View Outbox & Delivery Receipts (Delivery Tracker)
  5. List Online Users

 [ PUBLIC SERVER STORAGE (CENTRALIZED REPOSITORY) ]
  6. List Server Public Files
  7. Upload Public File
  8. Download Public File

 [ SYSTEM CONTROLS ]
  9. View Connection Status
  10. Disconnect
  11. Exit
============================================================
"""
            print(menu)
            choice = input("Choose an option: ").strip()

            if choice == "1":
                default_h = client.host
                default_p = client.port
                host_input = input(f"Enter server host [{default_h}]: ").strip()
                port_input = input(f"Enter server port [{default_p}]: ").strip()

                host = host_input if host_input else default_h
                try:
                    port = int(port_input) if port_input else default_p
                except ValueError:
                    print("[-] Invalid port number. Keeping default.")
                    port = default_p

                if client.connect(host=host, port=port):
                    uname = input("Enter your username (e.g. Alice, Bob): ").strip()
                    if uname:
                        client.register(uname)

            elif choice == "2":
                if not client.is_connected:
                    print("[-] Please connect to the server first (Option 1).")
                    continue
                if not client.username:
                    uname = input("Enter your username first: ").strip()
                    if uname:
                        client.register(uname)

                recipient = input("Enter recipient username (e.g. Bob): ").strip()
                if not recipient:
                    print("[-] Recipient username cannot be blank.")
                    continue
                filepath = input("Enter path to file you want to send: ").strip()
                client.send_file_to_user(recipient=recipient, local_path_str=filepath)

            elif choice == "3":
                if not client.is_connected:
                    print("[-] Please connect to the server first (Option 1).")
                    continue
                if not client.username:
                    uname = input("Enter your username to view your inbox: ").strip()
                    if uname:
                        client.register(uname)
                inbox = client.view_inbox()
                if inbox:
                    dl_choice = input("Enter Transfer ID to download & acknowledge receipt (or Enter to skip): ").strip()
                    if dl_choice:
                        client.download_inbox_file(dl_choice)

            elif choice == "4":
                if not client.is_connected:
                    print("[-] Please connect to the server first (Option 1).")
                    continue
                if not client.username:
                    uname = input("Enter your username to check outbox: ").strip()
                    if uname:
                        client.register(uname)
                client.view_outbox()

            elif choice == "5":
                if not client.is_connected:
                    print("[-] Please connect to the server first (Option 1).")
                    continue
                client.list_online_users()

            elif choice == "6":
                client.list_files()

            elif choice == "7":
                if not client.is_connected:
                    print("[-] You must connect to the server first (Option 1).")
                    continue
                path_str = input("Enter path to file you wish to upload: ").strip()
                overwrite_choice = input("Overwrite if file already exists on server? (y/N): ").strip().lower()
                overwrite = overwrite_choice == "y"
                client.upload_file(path_str, overwrite=overwrite)

            elif choice == "8":
                if not client.is_connected:
                    print("[-] You must connect to the server first (Option 1).")
                    continue
                file_to_download = input("Enter filename to download from server: ").strip()
                target = client.download_dir / file_to_download
                overwrite = False
                if target.exists():
                    ans = input(f"File '{file_to_download}' exists in downloads/. Overwrite? (y/N): ").strip().lower()
                    if ans != "y":
                        print("[!] Download cancelled by user to avoid overwrite.")
                        continue
                    overwrite = True
                client.download_file(file_to_download, overwrite=overwrite)

            elif choice == "9":
                client.view_status()

            elif choice == "10":
                client.disconnect()

            elif choice == "11":
                if client.is_connected:
                    client.disconnect()
                print("Exiting application. Goodbye!")
                break

            else:
                print("[-] Invalid option selected. Please choose a number between 1 and 11.")

        except (KeyboardInterrupt, EOFError):
            print("\nKeyboard interrupt detected. Exiting...")
            if client.is_connected:
                client.disconnect()
            break


def main() -> None:
    """CLI script entry point."""
    parser = argparse.ArgumentParser(
        description="Multithreaded Network File Transfer Client (Two-Way Transfer & Delivery ACKs)"
    )
    parser.add_argument(
        "--host",
        type=str,
        default=DEFAULT_HOST,
        help=f"Server host IP (default: {DEFAULT_HOST}).",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help=f"Server port (default: {DEFAULT_PORT}).",
    )
    parser.add_argument(
        "--user",
        type=str,
        default=None,
        help="Login as username for two-way transfers.",
    )
    parser.add_argument(
        "--send-to",
        type=str,
        default=None,
        help="Recipient username for two-way file send.",
    )
    parser.add_argument(
        "--file",
        type=str,
        default=None,
        help="Path of file to send to recipient.",
    )
    parser.add_argument(
        "--inbox",
        action="store_true",
        help="Check incoming files in inbox.",
    )
    parser.add_argument(
        "--outbox",
        action="store_true",
        help="Check sent files and delivery receipts in outbox.",
    )
    parser.add_argument(
        "--upload",
        type=str,
        default=None,
        help="Directly upload specified file to public storage and exit.",
    )
    parser.add_argument(
        "--download",
        type=str,
        default=None,
        help="Directly download specified file and exit.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Directly list server files and exit.",
    )
    args = parser.parse_args()

    # Scripted CLI actions
    if any([args.send_to, args.inbox, args.outbox, args.upload, args.download, args.list]):
        client = FileTransferClient(host=args.host, port=args.port)
        if not client.connect():
            sys.exit(1)
        try:
            if args.user:
                client.register(args.user)

            if args.send_to and args.file:
                ok = client.send_file_to_user(recipient=args.send_to, local_path_str=args.file)
                if not ok:
                    sys.exit(1)
            elif args.inbox:
                client.view_inbox()
            elif args.outbox:
                client.view_outbox()
            elif args.list:
                client.list_files()
            elif args.upload:
                if not client.upload_file(args.upload, overwrite=True):
                    sys.exit(1)
            elif args.download:
                if not client.download_file(args.download, overwrite=True):
                    sys.exit(1)
        finally:
            client.disconnect()
    else:
        run_interactive_cli()


if __name__ == "__main__":
    main()
