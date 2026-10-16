# Multithreaded Network File Transfer System (TCP/IP)

A high-performance, concurrent, client-server file transfer system implemented in Python using native TCP sockets and multithreading. Designed for academic demonstration of Computer Networks, Operating Systems, concurrent programming, and synchronization.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Problem Statement](#2-problem-statement)
3. [Objectives](#3-objectives)
4. [Features](#4-features)
5. [Technologies Used](#5-technologies-used)
6. [System Architecture](#6-system-architecture)
7. [Project Directory Structure](#7-project-directory-structure)
8. [Explanation of TCP Sockets](#8-explanation-of-tcp-sockets)
9. [Explanation of the Client-Server Model](#9-explanation-of-the-client-server-model)
10. [Explanation of Multithreading](#10-explanation-of-multithreading)
11. [Explanation of Concurrency and Synchronization](#11-explanation-of-concurrency-and-synchronization)
12. [Application-Level Protocol Specification](#12-application-level-protocol-specification)
13. [Installation & Requirements](#13-installation--requirements)
14. [How to Start the Server](#14-how-to-start-the-server)
15. [How to Start One Client](#15-how-to-start-one-client)
16. [How to Start Multiple Clients (Concurrency Demo)](#16-how-to-start-multiple-clients-concurrency-demo)
17. [How to Test File Uploads and Downloads](#17-how-to-test-file-uploads-and-downloads)
18. [LAN Setup (Connecting Computers on the Same Network)](#18-lan-setup-connecting-computers-on-the-same-network)
19. [Firewall and Port Troubleshooting](#19-firewall-and-port-troubleshooting)
20. [Running Automated Tests](#20-running-automated-tests)
21. [Known Limitations](#21-known-limitations)
22. [Security Considerations](#22-security-considerations)
23. [Future Enhancements](#23-future-enhancements)
24. [Academic Demonstration Checklist](#24-academic-demonstration-checklist)

---

## 1. Project Overview

In networked environments, file transfer is a fundamental service. Real-world systems like FTP, HTTP/2, and cloud storage gateways must handle multiple client connections simultaneously without allowing a slow or stalled client to starve other clients of I/O resources.

This project delivers a multithreaded network file transfer application built strictly on standard library Python (`socket` and `threading`). It provides reliable chunked transfers, an application-level message-framing protocol, granular per-file concurrency synchronization, and a clean command-line user interface.

---

## 2. Problem Statement

Standard raw TCP sockets provide an unstructured byte stream without message boundaries or transaction states. Building a robust file transfer system requires solving four core engineering challenges:
1. **TCP Stream Demarcation**: Preventing "TCP stickiness" (message concatenation) or fragmentation through explicit framing.
2. **Concurrent Client Handling**: Ensuring multiple clients can upload, download, and query file indexes simultaneously without blocking one another.
3. **Race Condition Prevention**: Preventing file corruption when multiple threads access files, while avoiding a coarse-grained global lock that halts concurrency.
4. **Reliability and Fault Tolerance**: Handling sudden network disconnects, partial reads/writes, malformed packets, and path traversal vulnerabilities.

---

## 3. Objectives

* Build a multi-client, multithreaded TCP file transfer server.
* Design an extensible binary header + JSON control protocol on top of TCP.
* Stream files in configurable 64 KB chunks rather than loading entire files into memory.
* Provide dynamic terminal progress reporting (bytes transferred, percentage, speed in MB/s).
* Enforce path sanitization and collision prevention against file overwrites.
* Provide an automated test suite verifying protocol framing, file integrity (SHA-256), and concurrent load.

---

## 4. Features

* **Multi-Client Concurrency**: Each incoming client connection is serviced by an independent worker thread.
* **Granular Concurrency Control**: Per-file lock management (`FileLockManager`) serializes conflicting operations on the same filename while allowing transfers of different files in full parallel throughput.
* **Chunked Streaming I/O**: Files are streamed in 64 KB buffers, enabling transfers of arbitrarily large files (gigabytes) with minimal RAM utilization.
* **Path Traversal Protection**: Rejects or sanitizes `../`, absolute paths, null bytes, and malicious filenames.
* **Safe Overwrite Protection**: Uploads check if a file already exists; transfers are rejected with HTTP-style `409 Conflict` unless explicit overwrite permission is confirmed.
* **Atomic Writes & Cleanup**: In-flight downloads and uploads are written to `.part` files first. If a connection drops, partial files are automatically removed to prevent corruption.
* **Live Progress & Transfer Rate**: Visual progress bar indicating percentage completion, total transferred bytes, and transfer speed.
* **Zero-Byte File Handling**: Safely transfers empty files without division-by-zero or protocol hangs.
* **Thread-Safe Diagnostic Logging**: Timestamps, thread names, IP addresses, and transfer milestones are recorded in terminal output and `logs/server.log`.

---

## 5. Technologies Used

* **Language**: Python 3 (Tested on Python 3.8 - 3.13)
* **Networking**: Native TCP sockets (`socket` module)
* **Concurrency**: Native OS threads (`threading` module, `threading.Lock`)
* **Binary Framing**: Byte serialization and unpacking (`struct` module)
* **Control Serialization**: UTF-8 JSON payloads (`json` module)
* **Testing**: Python unit test framework (`unittest` module)
* **Portability**: Pure standard library — runs without modification on Windows, Linux, and macOS.

---

## 6. System Architecture

```
+-----------------------------------------------------------------------------------+
|                                CLIENT APPLICATION                                 |
|                                                                                   |
|  [ Interactive CLI Menu ] <---> [ Client Controller ] <---> [ Streaming Engine ]  |
+-----------------------------------------------------------------------------------+
                                         │
                                   TCP Stream (IP:Port)
                                         │
+-----------------------------------------------------------------------------------+
|                                SERVER APPLICATION                                 |
|                                                                                   |
|                           [ TCP Listening Socket ]                                |
|                                      │                                            |
|                               (accept() loop)                                     |
|                                      │                                            |
|                  ┌───────────────────┼───────────────────┐                        |
|                  ▼                   ▼                   ▼                        |
|          [Worker Thread 1]   [Worker Thread 2]   [Worker Thread N]                |
|                  │                   │                   │                        |
|                  └───────────────────┼───────────────────┘                        |
|                                      ▼                                            |
|                        [ FileLockManager (Per-File) ]                             |
|                                      ▼                                            |
|                  [ Central Server Storage (server_storage/) ]                     |
+-----------------------------------------------------------------------------------+
```

---

## 7. Project Directory Structure

```
multithreaded-file-transfer/
├── server.py                # Server socket engine & per-client thread handlers
├── client.py                # Interactive CLI menu & client transfer implementation
├── web_server.py            # Modern Web Dashboard & HTTP-to-TCP socket bridge
├── protocol.py              # Framing, exact-byte helpers, chunk streaming & error models
├── config.py                # Host/port, buffer sizes, timeouts, and security limits
├── requirements.txt         # Environment declaration (Standard Library only)
├── README.md                # Comprehensive project documentation
├── .gitignore               # Git exclusion rules
├── server_storage/          # Central repository for uploaded files on server
│   └── .gitkeep
├── downloads/               # Target directory for client downloads
│   └── .gitkeep
├── logs/                    # Central log directory
│   └── .gitkeep
└── tests/
    ├── __init__.py          # Test package marker
    ├── test_protocol.py     # Framing, partial reads, and edge-case unit tests
    └── test_file_transfer.py# Integration, file integrity, and multi-client tests
```

---

## 8. Explanation of TCP Sockets

The **Transmission Control Protocol (TCP)** operates at the Transport Layer of the OSI / Internet model.

* **Connection-Oriented**: Requires a three-way handshake (`SYN`, `SYN-ACK`, `ACK`) before data exchange begins.
* **Byte Stream Abstraction**: TCP does not transmit discrete packets to the application; it provides an continuous, ordered stream of bytes. A single `send()` call on the sender side may arrive across multiple `recv()` calls on the receiver, or multiple `send()` calls may be coalesced into a single `recv()`.
* **Reliability**: TCP handles retransmission of lost packets, packet ordering via sequence numbers, checksum validation, and flow/congestion control (`TCP window`).

---

## 9. Explanation of the Client-Server Model

The system follows an asymmetric client-server distributed architecture:
* **Server**: Binds to a known network interface and port, enters a listening state (`listen()`), and passively waits for client connection requests. It provides centralized shared resources (file storage).
* **Client**: Actively initiates a connection to the server's network address (`connect()`), presents requests (commands), and displays results to the user.

---

## 10. Explanation of Multithreading

In an operating system, a **Process** represents an executing program with its own private address space and resources. A **Thread** is the smallest unit of execution within a process that can be scheduled by the OS kernel. Multiple threads within the same process share the same memory space, code segments, and open file descriptors.

### Why Multithreading in This Server?
If a server handled clients in a single-threaded sequential loop, a client uploading a 2 GB video would block the entire server for several minutes. No other client could connect or list files.

By dedicating an independent `threading.Thread` to each client connection:
1. The main server thread remains free to execute `accept()` immediately for new incoming connections.
2. While Thread 1 is waiting on network I/O from Client 1, Thread 2 can read from disk for Client 2.
3. Slow or disconnected clients have zero impact on other active clients.

---

## 11. Explanation of Concurrency and Synchronization

### Shared State & Race Conditions
Because all threads share the same process memory and filesystem, a race condition occurs when two or more threads attempt to read or modify the same file simultaneously without coordination.
* *Example*: Client A starts uploading a file named `report.docx`. Client B simultaneously requests to download `report.docx`. Without synchronization, Client B would download an incomplete, corrupted file.

### Synchronization Strategy in This Project
* **Coarse Lock (Anti-pattern)**: Putting a single global `threading.Lock` around all server operations would destroy concurrency, turning the server into an effectively single-threaded application.
* **Fine-Grained Per-File Locking (`FileLockManager`)**:
  This project implements a dynamic per-file locking registry:
  ```python
  class FileLockManager:
      def __init__(self):
          self._master_lock = threading.Lock()
          self._file_locks = {}
          self._ref_counts = {}
  ```
  When a client accesses `data.bin`, the server acquires an exclusive lock *only for that filename*.
  - Client A uploading `fileA.txt` and Client B downloading `fileB.pdf` execute in **full parallel throughput**.
  - A second client attempting to modify `fileA.txt` is safely serialized until Client A completes.
  - When all threads finish with a file, its lock is garbage-collected to prevent memory leaks.

---

## 12. Application-Level Protocol Specification

To transmit structured control commands alongside arbitrary binary files without data corruption, we define a two-phase protocol:

### Phase 1: Control Message Framing
Every control packet consists of:
1. **Length Header (4 bytes)**: Fixed-size 32-bit unsigned integer in network byte order (Big-Endian: `!I`).
2. **Control Payload (N bytes)**: UTF-8 encoded JSON object.

```
+---------------------------+-----------------------------------------------+
|  Header (4 Bytes, !I)     |  JSON Payload (UTF-8 Encoded)                 |
|  Payload Length = N       |  {"command": "UPLOAD", "filename": "...", ...}|
+---------------------------+-----------------------------------------------+
```

### Phase 2: Streaming Data Transfer
Following the control handshake, file contents are transferred in binary chunks without JSON encoding. The exact byte count is governed by the declared `size` attribute:
* **Upload**: Client sends `UPLOAD` metadata -> Server sends `OK` -> Client streams exact bytes -> Server sends completion `OK`.
* **Download**: Client sends `DOWNLOAD` -> Server sends file metadata -> Client sends `OK` ack -> Server streams exact bytes.

### Control Command Definitions

| Command | Direction | Parameters | Example Payload |
|---|---|---|---|
| `LIST` | Client -> Server | None | `{"command": "LIST"}` |
| `UPLOAD` | Client -> Server | `filename`, `size`, `overwrite` | `{"command": "UPLOAD", "filename": "doc.pdf", "size": 1024, "overwrite": false}` |
| `DOWNLOAD` | Client -> Server | `filename` | `{"command": "DOWNLOAD", "filename": "doc.pdf"}` |
| `QUIT` | Client -> Server | None | `{"command": "QUIT"}` |

### Standard Response Codes
* `200 OK`: Operation succeeded.
* `400 Bad Request`: Missing parameters, malformed JSON, or invalid command.
* `403 Forbidden`: Path traversal attempt or access restriction.
* `404 Not Found`: File not found in server storage.
* `409 Conflict`: File already exists and overwrite was not permitted.
* `413 File Too Large`: Declared size exceeds server limit (5 GB).
* `500 Internal Error`: Filesystem or server runtime exception.

---

## 13. Installation & Requirements

### System Requirements
* Operating System: Windows 10/11, macOS, or Linux.
* Python: Version 3.8 or higher.
* Dependencies: None (Standard Library only).

### Clone / Navigate to Directory
```bash
cd c:\Users\bhanu\OneDrive\Desktop\OS
```

Verify Python installation:
```bash
py --version
# Or on Linux/macOS:
python3 --version
```

---

## 14. How to Start the Server

### Localhost Mode (Default: 127.0.0.1:5000)
Run the server with default parameters:
```bash
py server.py
# Or: python server.py / python3 server.py
```

### Custom Port or Interface
```bash
py server.py --host 0.0.0.0 --port 5000
```
* Using `--host 0.0.0.0` instructs the server to bind to all available network adapters, allowing connections from other computers on the LAN.

---

## 15. How to Start One Client

Open a separate terminal window and launch the interactive client:
```bash
py client.py
```

You will see the main menu:
```
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
Choose an option:
```

### Scripted / One-Shot Client Commands
You can also use command-line flags without entering the interactive loop:
```bash
# List files:
py client.py --host 127.0.0.1 --port 5000 --list

# Upload a file:
py client.py --host 127.0.0.1 --port 5000 --upload "path/to/my_file.pdf"

# Download a file:
py client.py --host 127.0.0.1 --port 5000 --download "my_file.pdf"
```

---

## 16. How to Start Multiple Clients (Concurrency Demo)

To observe multithreading and concurrency in real time:

1. **Terminal 1**: Start the server:
   ```bash
   py server.py
   ```
2. **Terminal 2**: Start Client 1:
   ```bash
   py client.py
   ```
3. **Terminal 3**: Start Client 2:
   ```bash
   py client.py
   ```
4. **Terminal 4**: Start Client 3:
   ```bash
   py client.py
   ```

* Connect all three clients using Option 1.
* In Client 1, upload a large file.
* While Client 1 is uploading, immediately choose Option 2 (List Files) or Option 4 (Download) in Client 2 and Client 3.
* Observe that the server handles Client 2 and Client 3 immediately in their own worker threads without waiting for Client 1 to complete.

---

## 17. How to Test File Uploads and Downloads

### Step-by-Step Walkthrough

1. In the client menu, press `1` to connect to `127.0.0.1` on port `5000`.
2. Press `2` to list files. The server will initially show `(No files currently hosted on server)`.
3. Press `3` to upload a file:
   - Provide the path to any local file (e.g., `README.md` or an image).
   - Watch the live progress bar:
     ```
     [==============================] 100.0% (14.25 KB / 14.25 KB) @ 5.12 MB/s
     [+] Upload complete: 'README.md' transferred.
     ```
4. Press `2` again. You will see `README.md` listed with its size, file type, and modification timestamp.
5. Press `4` to download:
   - Enter `README.md`.
   - The file is streamed into your local `downloads/` folder.
6. Check your `downloads/` folder to confirm the file is intact.

---

## 18. LAN Setup (Connecting Computers on the Same Network)

To run the application across two physical computers on the same Wi-Fi or local area network:

### Step 1: Find Server's Local IP Address
On the **Server Computer**:
* **Windows**: Open PowerShell and run `ipconfig`. Look for the `IPv4 Address` under your active Wi-Fi or Ethernet adapter (e.g., `192.168.1.45`).
* **Linux/macOS**: Run `ip a` or `ifconfig` (e.g., `192.168.1.45`).

### Step 2: Start Server on All Interfaces
Start the server binding to `0.0.0.0`:
```bash
py server.py --host 0.0.0.0 --port 5000
```

### Step 3: Connect Client from Second Computer
On the **Client Computer**:
Launch `client.py` and choose Option 1:
```
Enter server host [127.0.0.1]: 192.168.1.45
Enter server port [5000]: 5000
```
The client will connect across the physical network.

---

## 19. Firewall and Port Troubleshooting

If the client cannot connect across machines:

1. **Allow Port in Windows Firewall (Server machine)**:
   Open PowerShell as Administrator and run:
   ```powershell
   New-NetFirewallRule -DisplayName "FileTransferServer" -Direction Inbound -LocalPort 5000 -Protocol TCP -Action Allow
   ```
2. **Ping Test**:
   From the client machine, run:
   ```bash
   ping 192.168.1.45
   ```
3. **Port In Use Error (`Address already in use`)**:
   If another application occupies port 5000, start the server on another port:
   ```bash
   py server.py --port 5050
   ```

---

## 20. Running Automated Tests

The project includes an automated test suite verifying protocol framing, socket disconnections, short reads, collision prevention, and multi-client concurrency.

To run all 20 tests:
```bash
py -m unittest discover -s tests -v
```

Expected output:
```
test_multiple_concurrent_uploads_and_downloads ... ok
test_upload_small_text_file ... ok
test_upload_binary_file_hash_verification ... ok
test_upload_zero_byte_file ... ok
test_download_file_successfully ... ok
test_download_missing_file_reports_error ... ok
test_list_files_correctness ... ok
test_prevent_accidental_file_overwrite ... ok
test_path_traversal_rejection ... ok
test_server_unavailable ... ok
test_client_disconnect_during_upload_cleans_up ... ok
test_partial_file_cleanup_on_interrupt ... ok
test_zero_byte_file_transfer ... ok
test_disconnected_peer_detection ... ok
test_encode_and_decode_message ... ok
test_malformed_json_rejection ... ok
test_oversized_control_message_rejection ... ok
test_partial_body_handling ... ok
test_partial_header_handling ... ok
test_premature_disconnect_during_payload ... ok

----------------------------------------------------------------------
Ran 20 tests in 4.8s

OK
```

---

## 21. Known Limitations

* **Plaintext Transport**: TCP stream is unencrypted. Anyone with access to the local network or a packet analyzer (e.g., Wireshark) can inspect transmitted data.
* **Authentication**: There is no user authentication or access control list (ACL). Any client on the network can list, upload, and download files.
* **Resume Interrupted Downloads**: If a connection drops halfway, the partial file is safely cleaned up, but byte-range resuming (like HTTP `Range: bytes=X-Y`) is not yet implemented.

---

## 22. Security Considerations

For this academic LAN prototype:
* **Directory Traversal Mitigation**: The server verifies that all resolved file paths lie strictly inside `server_storage/` using `is_safe_path()`. Attempts such as `../../Windows/System32` are rejected with `403 Forbidden`.
* **Filename Sanitization**: Client-provided filenames have path separators (`/`, `\`) and null bytes (`\0`) stripped.
* **Resource Exhaustion Defense**:
  - Control messages are capped at 64 KB (`MAX_CONTROL_MESSAGE_SIZE`).
  - Total file sizes are capped at 5 GB (`MAX_FILE_SIZE`).
  - Sockets implement a 30-second inactivity timeout (`SOCKET_TIMEOUT`) to prevent idle connections from consuming thread resources.

---

## 23. Future Enhancements

1. **Transport Layer Security (TLS/SSL)**: Wrap sockets using Python's `ssl` module (`ssl.wrap_socket`) for encrypted transmission.
2. **User Authentication**: Add a `LOGIN` command with hashed credentials (e.g. SHA-256 + salt).
3. **Resumable Transfers**: Include a `byte_offset` parameter in `DOWNLOAD` and `UPLOAD` to resume partial transfers.
4. **End-to-End Checksums**: Automatically transmit and verify SHA-256 digests in protocol headers.

---

## 24. Academic Demonstration Checklist

When presenting this project to an evaluator or professor:

- [ ] **Architecture Diagram**: Explain the client-server topology and per-client worker thread spawning.
- [ ] **Start the Server**: Show console startup logs with host, port, and storage path.
- [ ] **Single Client Flow**:
  1. Connect client to server.
  2. List files (demonstrate empty listing).
  3. Upload a sample file (point out live progress bar and speed in MB/s).
  4. List files again (show file name, size, and timestamp).
  5. Download the file into `downloads/` and verify content.
- [ ] **Multi-Client Concurrency Demo**:
  1. Open three separate client terminal windows.
  2. Initiate an upload on Client 1 and simultaneously run file listings and downloads on Clients 2 and 3.
  3. Point out that neither client blocks the other.
- [ ] **Fault Tolerance & Security**:
  1. Attempt to upload a duplicate file without overwrite permission (demonstrate `409 Conflict` rejection).
  2. Attempt a traversal string (demonstrate path traversal protection).
  3. Close a client abruptly mid-transfer and demonstrate that the server stays online and cleans up `.part` files.
- [ ] **Automated Tests**: Execute `py -m unittest discover -s tests -v` and present the passing 20-test report.
