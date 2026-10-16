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
        self.connected_time: Optional[float] = None

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
    """Run interactive Command-Line Interface menu loop."""
    ensure_directories_exist()
    client = FileTransferClient()

    banner = """
========================================
  MULTITHREADED NETWORK FILE TRANSFER
========================================
1. Connect to Server
2. List Server Files
3. Upload File
4. Download File
5. View Connection Status
6. Disconnect
7. Exit
========================================
"""

    while True:
        try:
            print(banner)
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

                client.connect(host=host, port=port)

            elif choice == "2":
                client.list_files()

            elif choice == "3":
                if not client.is_connected:
                    print("[-] You must connect to the server first (Option 1).")
                    continue
                path_str = input("Enter path to file you wish to upload: ").strip()
                # Check if user wants overwrite flag if file exists
                overwrite_choice = input("Overwrite if file already exists on server? (y/N): ").strip().lower()
                overwrite = overwrite_choice == "y"
                client.upload_file(path_str, overwrite=overwrite)

            elif choice == "4":
                if not client.is_connected:
                    print("[-] You must connect to the server first (Option 1).")
                    continue
                # Optionally offer to show list first
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

            elif choice == "5":
                client.view_status()

            elif choice == "6":
                client.disconnect()

            elif choice == "7":
                if client.is_connected:
                    client.disconnect()
                print("Exiting application. Goodbye!")
                break

            else:
                print("[-] Invalid option selected. Please choose a number between 1 and 7.")

        except (KeyboardInterrupt, EOFError):
            print("\nKeyboard interrupt detected. Exiting...")
            if client.is_connected:
                client.disconnect()
            break


def main() -> None:
    """CLI script entry point."""
    parser = argparse.ArgumentParser(
        description="Multithreaded Network File Transfer Client"
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
        "--upload",
        type=str,
        default=None,
        help="Directly upload specified file and exit.",
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

    # If CLI flags were supplied for direct scripted execution
    if args.upload or args.download or args.list:
        client = FileTransferClient(host=args.host, port=args.port)
        if not client.connect():
            sys.exit(1)
        try:
            if args.list:
                client.list_files()
            if args.upload:
                success = client.upload_file(args.upload, overwrite=True)
                if not success:
                    sys.exit(1)
            if args.download:
                success = client.download_file(args.download, overwrite=True)
                if not success:
                    sys.exit(1)
        finally:
            client.disconnect()
    else:
        # Default interactive menu mode
        run_interactive_cli()


if __name__ == "__main__":
    main()
