"""Web Interface & HTTP-to-TCP Bridge for Multithreaded File Transfer System.

Provides a modern, glassmorphic Web UI and REST API gateway that communicates
with the underlying multithreaded TCP socket server (server.py) using pure
Python standard library (http.server, socket, threading). Supports two-way transfers
and end-to-end delivery confirmation receipts.
"""

from email import policy
from email.parser import BytesParser
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse
import json
import mimetypes
import os
import shutil
import socket
import sys
import tempfile
import threading
import time

from client import FileTransferClient
from config import (
    CLIENT_DOWNLOAD_DIR,
    DEFAULT_HOST,
    DEFAULT_PORT,
    LOGS_DIR,
    SERVER_STORAGE_DIR,
    ensure_directories_exist,
    sanitize_filename,
)

WEB_PORT = 8080
TCP_SERVER_HOST = DEFAULT_HOST
TCP_SERVER_PORT = DEFAULT_PORT


def get_formatted_bytes(size: float) -> str:
    """Format byte size into human readable string."""
    if size < 0:
        return "0 B"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"


HTML_DASHBOARD = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Multithreaded Network File Transfer System | Two-Way Transfer & Delivery ACKs</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg-base: #090d16;
      --bg-surface: #111827;
      --bg-card: rgba(23, 32, 54, 0.7);
      --bg-card-hover: rgba(30, 41, 69, 0.85);
      --border-color: rgba(99, 102, 241, 0.18);
      --border-glow: rgba(99, 102, 241, 0.4);
      --primary: #6366f1;
      --primary-light: #818cf8;
      --accent-cyan: #06b6d4;
      --accent-emerald: #10b981;
      --accent-amber: #f59e0b;
      --accent-rose: #f43f5e;
      --text-main: #f8fafc;
      --text-muted: #94a3b8;
      --text-faint: #64748b;
      --font-sans: 'Plus Jakarta Sans', system-ui, -apple-system, sans-serif;
      --font-mono: 'JetBrains Mono', monospace;
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      background: radial-gradient(circle at 15% 15%, rgba(99, 102, 241, 0.08) 0%, transparent 45%),
                  radial-gradient(circle at 85% 85%, rgba(6, 182, 212, 0.08) 0%, transparent 45%),
                  var(--bg-base);
      color: var(--text-main);
      font-family: var(--font-sans);
      min-height: 100vh;
      display: flex;
      flex-direction: column;
      overflow-x: hidden;
      line-height: 1.5;
    }

    body::before {
      content: "";
      position: fixed;
      inset: 0;
      background-image: 
        linear-gradient(to right, rgba(255, 255, 255, 0.02) 1px, transparent 1px),
        linear-gradient(to bottom, rgba(255, 255, 255, 0.02) 1px, transparent 1px);
      background-size: 32px 32px;
      pointer-events: none;
      z-index: 0;
    }

    .container {
      max-width: 1360px;
      margin: 0 auto;
      padding: 24px 20px 48px;
      position: relative;
      z-index: 1;
      width: 100%;
    }

    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding-bottom: 20px;
      margin-bottom: 20px;
      border-bottom: 1px solid var(--border-color);
      flex-wrap: wrap;
      gap: 16px;
    }

    .brand {
      display: flex;
      align-items: center;
      gap: 14px;
    }

    .brand-icon {
      width: 48px;
      height: 48px;
      border-radius: 12px;
      background: linear-gradient(135deg, var(--primary) 0%, var(--accent-cyan) 100%);
      display: flex;
      align-items: center;
      justify-content: center;
      box-shadow: 0 0 20px rgba(99, 102, 241, 0.35);
    }

    .brand-icon svg { width: 28px; height: 28px; color: white; }

    .brand-titles h1 {
      font-size: 1.4rem;
      font-weight: 800;
      letter-spacing: -0.02em;
      background: linear-gradient(to right, #fff, #cbd5e1);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
    }

    .brand-titles p {
      font-size: 0.8rem;
      color: var(--text-muted);
      font-weight: 500;
    }

    .header-user-badge {
      display: flex;
      align-items: center;
      gap: 12px;
      background: rgba(17, 24, 39, 0.8);
      border: 1px solid var(--border-color);
      padding: 8px 16px;
      border-radius: 12px;
    }

    .user-select {
      background: rgba(99, 102, 241, 0.15);
      border: 1px solid var(--primary-light);
      color: white;
      font-family: var(--font-sans);
      font-weight: 700;
      font-size: 0.85rem;
      padding: 5px 10px;
      border-radius: 8px;
      cursor: pointer;
      outline: none;
    }

    .pulse-dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: var(--accent-emerald);
      box-shadow: 0 0 10px var(--accent-emerald);
      animation: pulse 2s infinite ease-in-out;
    }

    @keyframes pulse {
      0%, 100% { opacity: 1; transform: scale(1); }
      50% { opacity: 0.4; transform: scale(0.85); }
    }

    /* Tabs Bar */
    .tabs-nav {
      display: flex;
      gap: 10px;
      margin-bottom: 22px;
      border-bottom: 1px solid var(--border-color);
      padding-bottom: 10px;
      overflow-x: auto;
    }

    .tab-btn {
      background: transparent;
      border: 1px solid transparent;
      color: var(--text-muted);
      font-family: var(--font-sans);
      font-weight: 600;
      font-size: 0.88rem;
      padding: 9px 18px;
      border-radius: 10px;
      cursor: pointer;
      transition: all 0.2s ease;
      display: inline-flex;
      align-items: center;
      gap: 8px;
    }

    .tab-btn:hover {
      color: #fff;
      background: rgba(255, 255, 255, 0.05);
    }

    .tab-btn.active {
      background: rgba(99, 102, 241, 0.15);
      border-color: var(--primary);
      color: #fff;
      box-shadow: 0 4px 12px rgba(99, 102, 241, 0.2);
    }

    .badge-count {
      background: var(--primary);
      color: white;
      font-size: 0.72rem;
      padding: 2px 7px;
      border-radius: 9999px;
      font-weight: 700;
    }

    /* Cards & Grids */
    .tab-content { display: none; }
    .tab-content.active { display: block; animation: fadeIn 0.25s ease; }

    @keyframes fadeIn {
      from { opacity: 0; transform: translateY(6px); }
      to { opacity: 1; transform: translateY(0); }
    }

    .grid-two {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 22px;
      margin-bottom: 22px;
    }

    @media (max-width: 1024px) {
      .grid-two { grid-template-columns: 1fr; }
    }

    .card {
      background: var(--bg-card);
      backdrop-filter: blur(12px);
      border: 1px solid var(--border-color);
      border-radius: 18px;
      padding: 24px;
      display: flex;
      flex-direction: column;
      box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
    }

    .card-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 18px;
    }

    .card-header h2 {
      font-size: 1.05rem;
      font-weight: 700;
      display: flex;
      align-items: center;
      gap: 10px;
    }

    .form-group {
      margin-bottom: 16px;
    }

    .form-group label {
      display: block;
      font-size: 0.8rem;
      font-weight: 600;
      color: var(--text-muted);
      margin-bottom: 6px;
    }

    .input-control {
      width: 100%;
      background: rgba(11, 17, 30, 0.8);
      border: 1px solid var(--border-color);
      border-radius: 10px;
      padding: 10px 14px;
      font-size: 0.85rem;
      color: #fff;
      font-family: var(--font-sans);
      outline: none;
      transition: border 0.2s ease;
    }

    .input-control:focus {
      border-color: var(--primary-light);
    }

    /* Upload box */
    .file-drop-area {
      border: 2px dashed rgba(99, 102, 241, 0.3);
      border-radius: 14px;
      padding: 24px 16px;
      text-align: center;
      background: rgba(17, 24, 39, 0.5);
      cursor: pointer;
      position: relative;
      transition: all 0.2s ease;
    }

    .file-drop-area:hover {
      border-color: var(--accent-cyan);
      background: rgba(6, 182, 212, 0.05);
    }

    .file-drop-area input[type="file"] {
      position: absolute;
      inset: 0;
      opacity: 0;
      cursor: pointer;
      width: 100%;
      height: 100%;
    }

    /* Buttons */
    .btn {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      padding: 10px 20px;
      border-radius: 10px;
      font-size: 0.82rem;
      font-weight: 600;
      font-family: var(--font-sans);
      cursor: pointer;
      transition: all 0.2s ease;
      border: none;
      outline: none;
    }

    .btn-primary {
      background: linear-gradient(135deg, var(--primary) 0%, #4f46e5 100%);
      color: white;
      box-shadow: 0 4px 14px rgba(79, 70, 229, 0.35);
    }

    .btn-primary:hover {
      background: linear-gradient(135deg, var(--primary-light) 0%, var(--primary) 100%);
      transform: translateY(-1px);
    }

    .btn-emerald {
      background: linear-gradient(135deg, var(--accent-emerald) 0%, #059669 100%);
      color: white;
      box-shadow: 0 4px 14px rgba(16, 185, 129, 0.35);
    }

    .btn-secondary {
      background: rgba(255, 255, 255, 0.07);
      color: var(--text-main);
      border: 1px solid rgba(255, 255, 255, 0.12);
    }

    .btn-secondary:hover {
      background: rgba(255, 255, 255, 0.12);
    }

    .btn-sm {
      padding: 6px 12px;
      font-size: 0.75rem;
      border-radius: 8px;
    }

    /* Tables */
    .table-container {
      overflow-x: auto;
      max-height: 440px;
      overflow-y: auto;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      text-align: left;
      font-size: 0.82rem;
    }

    th {
      position: sticky;
      top: 0;
      background: var(--bg-surface);
      color: var(--text-muted);
      font-weight: 600;
      text-transform: uppercase;
      font-size: 0.7rem;
      letter-spacing: 0.05em;
      padding: 12px 14px;
      border-bottom: 1px solid var(--border-color);
      z-index: 10;
    }

    td {
      padding: 12px 14px;
      border-bottom: 1px solid rgba(255, 255, 255, 0.04);
      color: var(--text-main);
    }

    tr:hover td { background: rgba(99, 102, 241, 0.04); }

    /* Delivery Status Badges */
    .status-badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 4px 10px;
      border-radius: 9999px;
      font-size: 0.74rem;
      font-weight: 700;
    }

    .status-pending {
      background: rgba(245, 158, 11, 0.12);
      border: 1px solid rgba(245, 158, 11, 0.3);
      color: #fbbf24;
    }

    .status-confirmed {
      background: rgba(16, 185, 129, 0.12);
      border: 1px solid rgba(16, 185, 129, 0.3);
      color: #34d399;
    }

    /* Terminal Console */
    .terminal-card {
      background: #070a12;
      border: 1px solid rgba(255, 255, 255, 0.08);
      border-radius: 16px;
      padding: 18px 20px;
      font-family: var(--font-mono);
      margin-top: 24px;
    }

    .terminal-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding-bottom: 12px;
      margin-bottom: 12px;
      border-bottom: 1px solid rgba(255, 255, 255, 0.06);
    }

    .terminal-dots { display: flex; gap: 6px; }
    .terminal-dots span { width: 10px; height: 10px; border-radius: 50%; }
    .dot-red { background: #ef4444; }
    .dot-yellow { background: #eab308; }
    .dot-green { background: #22c55e; }

    .terminal-body {
      background: rgba(0, 0, 0, 0.4);
      border-radius: 10px;
      padding: 14px;
      height: 190px;
      overflow-y: auto;
      font-size: 0.74rem;
      line-height: 1.6;
      color: #cbd5e1;
      white-space: pre-wrap;
      word-break: break-all;
    }

    /* Toast */
    #toast {
      position: fixed;
      bottom: 24px;
      right: 24px;
      background: var(--bg-surface);
      border: 1px solid var(--border-glow);
      box-shadow: 0 10px 25px rgba(0, 0, 0, 0.5);
      border-radius: 12px;
      padding: 14px 20px;
      font-size: 0.85rem;
      font-weight: 600;
      display: none;
      align-items: center;
      gap: 10px;
      z-index: 1000;
    }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <div class="brand">
        <div class="brand-icon">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path stroke-linecap="round" stroke-linejoin="round" d="M8 7h12m0 0l-4-4m4 4l-4 4m0 6H4m0 0l4 4m-4-4l4-4" />
          </svg>
        </div>
        <div class="brand-titles">
          <h1>Multithreaded Network File Transfer</h1>
          <p>Two-Way Peer File Exchange with Delivery Confirmation Receipts</p>
        </div>
      </div>

      <div class="header-user-badge">
        <span style="font-size:0.8rem; color:var(--text-muted);">Active User:</span>
        <select class="user-select" id="active-user-select" onchange="onUserChange()">
          <option value="Alice">Alice</option>
          <option value="Bob">Bob</option>
          <option value="Charlie">Charlie</option>
          <option value="Professor">Professor</option>
        </select>
        <span class="pulse-dot"></span>
        <span style="font-size:0.75rem; color:var(--accent-emerald); font-weight:600;">Server Connected (127.0.0.1:5000)</span>
      </div>
    </header>

    <!-- Navigation Tabs -->
    <div class="tabs-nav">
      <button class="tab-btn active" onclick="switchTab('two-way')">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M16 3h5v5M4 20L21 3M21 16v5h-5M15 15l6 6M4 4l5 5"/></svg>
        Two-Way Direct Transfer
      </button>
      <button class="tab-btn" onclick="switchTab('inbox')">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 12h-6l-2 3h-4l-2-3H2v7a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-7z"/><path d="M5.45 5.11L2 12v0h20v0l-3.45-6.89A2 2 0 0 0 16.76 4H7.24a2 2 0 0 0-1.79 1.11z"/></svg>
        My Inbox
        <span class="badge-count" id="inbox-badge">0</span>
      </button>
      <button class="tab-btn" onclick="switchTab('outbox')">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="20 6 9 17 4 12"/></svg>
        Sent Outbox & Delivery ACKs
        <span class="badge-count" id="outbox-badge">0</span>
      </button>
      <button class="tab-btn" onclick="switchTab('public')">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>
        Public Server Repository
      </button>
    </div>

    <!-- TAB 1: TWO-WAY DIRECT TRANSFER -->
    <div class="tab-content active" id="tab-two-way">
      <div class="grid-two">
        <div class="card">
          <div class="card-header">
            <h2>
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>
              Send File Directly to Another User
            </h2>
          </div>

          <form id="direct-send-form" onsubmit="handleDirectSend(event)">
            <div class="form-group">
              <label>Target Recipient Username</label>
              <input type="text" class="input-control" id="recipient-input" placeholder="e.g. Bob, Alice, Charlie..." required>
            </div>

            <div class="form-group">
              <label>Select File to Transmit</label>
              <div class="file-drop-area" id="direct-drop-zone">
                <input type="file" id="direct-file-input" required onchange="onDirectFileSelect()">
                <div style="color:var(--primary-light); margin-bottom:8px;">
                  <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>
                </div>
                <h4 id="direct-file-label">Click or Drag & Drop file to send</h4>
                <p style="font-size:0.75rem; color:var(--text-muted); margin-top:4px;">Streamed over TCP socket in 64 KB buffers directly to recipient mailbox</p>
              </div>
            </div>

            <button type="submit" class="btn btn-primary" id="btn-direct-send" style="width:100%;">
              Send File & Receive Delivery ACK
            </button>
          </form>
        </div>

        <!-- How Delivery Confirmation Works Card -->
        <div class="card">
          <div class="card-header">
            <h2>
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>
              How Two-Way Transfer & Delivery ACKs Work
            </h2>
          </div>

          <div style="font-size:0.83rem; line-height:1.7; color:var(--text-muted);">
            <p style="margin-bottom:12px;">
              <strong style="color:#fff;">1. Targeted Dispatch:</strong> When you send a file to a recipient (e.g. Alice &rarr; Bob), the file is received and locked in the server's designated mailbox.
            </p>
            <p style="margin-bottom:12px;">
              <strong style="color:#fbbf24;">2. Level 1 Receipt (Server ACK):</strong> You receive an instant confirmation that the file reached the server and is queued for the recipient. Status is marked as <code style="color:#fbbf24;">[PENDING: Waiting for recipient]</code>.
            </p>
            <p style="margin-bottom:12px;">
              <strong style="color:#34d399;">3. Level 2 Receipt (Recipient Download ACK):</strong> The moment Bob opens his Inbox and downloads the file, the server records the exact timestamp and confirms delivery.
            </p>
            <p>
              <strong style="color:var(--accent-cyan);">4. Two-Way Reply:</strong> Bob can then select Alice as the recipient and send files back to Alice, providing full two-way bidirectional file exchange!
            </p>
          </div>
        </div>
      </div>
    </div>

    <!-- TAB 2: INBOX -->
    <div class="tab-content" id="tab-inbox">
      <div class="card">
        <div class="card-header">
          <h2>
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 12h-6l-2 3h-4l-2-3H2v7a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-7z"/></svg>
            Incoming Files in Your Mailbox
          </h2>
          <button class="btn btn-secondary btn-sm" onclick="fetchInbox()">
            Refresh Inbox
          </button>
        </div>

        <div class="table-container">
          <table>
            <thead>
              <tr>
                <th>Sender</th>
                <th>Filename</th>
                <th>Size</th>
                <th>Sent At</th>
                <th>Status</th>
                <th style="text-align: right;">Action</th>
              </tr>
            </thead>
            <tbody id="inbox-tbody">
              <tr><td colspan="6" style="text-align:center; color:var(--text-faint); padding:32px;">Loading inbox...</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- TAB 3: OUTBOX & DELIVERY RECEIPTS -->
    <div class="tab-content" id="tab-outbox">
      <div class="card">
        <div class="card-header">
          <h2>
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="20 6 9 17 4 12"/></svg>
            Sent Files & Live Delivery Confirmation Tracker
          </h2>
          <button class="btn btn-secondary btn-sm" onclick="fetchOutbox()">
            Refresh Outbox
          </button>
        </div>

        <div class="table-container">
          <table>
            <thead>
              <tr>
                <th>Recipient</th>
                <th>Filename</th>
                <th>Size</th>
                <th>Sent Timestamp</th>
                <th>Delivery Confirmation Status</th>
              </tr>
            </thead>
            <tbody id="outbox-tbody">
              <tr><td colspan="5" style="text-align:center; color:var(--text-faint); padding:32px;">Loading outbox...</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- TAB 4: PUBLIC REPOSITORY -->
    <div class="tab-content" id="tab-public">
      <div class="card">
        <div class="card-header">
          <h2>
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/></svg>
            Public Server Storage (General Repository)
          </h2>
          <button class="btn btn-secondary btn-sm" onclick="fetchPublicFiles()">
            Refresh Public List
          </button>
        </div>

        <div class="table-container">
          <table>
            <thead>
              <tr>
                <th>Filename</th>
                <th>Size</th>
                <th>Type</th>
                <th>Modified</th>
                <th style="text-align: right;">Action</th>
              </tr>
            </thead>
            <tbody id="public-tbody">
              <tr><td colspan="5" style="text-align:center; color:var(--text-faint); padding:32px;">Loading public files...</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- Live Terminal Console -->
    <div class="terminal-card">
      <div class="terminal-header">
        <div class="terminal-dots">
          <span class="dot-red"></span>
          <span class="dot-yellow"></span>
          <span class="dot-green"></span>
        </div>
        <div style="font-size:0.75rem; color:var(--text-faint);">Live TCP Socket Log &amp; Delivery Receipt Stream</div>
        <div style="font-size:0.7rem; color:var(--accent-emerald);">● Streaming Active</div>
      </div>
      <div class="terminal-body" id="terminal-body">Connecting to live server log stream...</div>
    </div>
  </div>

  <div id="toast">Notification</div>

  <script>
    function formatBytes(bytes) {
      if (!bytes || bytes === 0) return '0 B';
      const k = 1024;
      const sizes = ['B', 'KB', 'MB', 'GB'];
      const i = Math.floor(Math.log(bytes) / Math.log(k));
      return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    function showToast(msg, isError = false) {
      const toast = document.getElementById('toast');
      toast.innerText = msg;
      toast.style.borderColor = isError ? 'var(--accent-rose)' : 'var(--accent-emerald)';
      toast.style.display = 'flex';
      setTimeout(() => { toast.style.display = 'none'; }, 4000);
    }

    function getActiveUser() {
      return document.getElementById('active-user-select').value;
    }

    function onUserChange() {
      const user = getActiveUser();
      showToast(`Switched active user to '${user}'`);
      fetchInbox();
      fetchOutbox();
    }

    function switchTab(tabId) {
      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));

      event.currentTarget.classList.add('active');
      document.getElementById('tab-' + tabId).classList.add('active');

      if (tabId === 'inbox') fetchInbox();
      if (tabId === 'outbox') fetchOutbox();
      if (tabId === 'public') fetchPublicFiles();
    }

    function onDirectFileSelect() {
      const fi = document.getElementById('direct-file-input');
      if (fi.files.length > 0) {
        document.getElementById('direct-file-label').innerText = `${fi.files[0].name} (${formatBytes(fi.files[0].size)})`;
      }
    }

    // Direct Two-Way File Send
    async function handleDirectSend(e) {
      e.preventDefault();
      const sender = getActiveUser();
      const recipient = document.getElementById('recipient-input').value.trim();
      const fileInput = document.getElementById('direct-file-input');
      const btn = document.getElementById('btn-direct-send');

      if (!recipient) {
        showToast('Please enter a recipient username.', true);
        return;
      }
      if (fileInput.files.length === 0) {
        showToast('Please select a file to send.', true);
        return;
      }

      const file = fileInput.files[0];
      btn.disabled = true;
      btn.innerText = `Transmitting '${file.name}' to ${recipient}...`;

      const formData = new FormData();
      formData.append('sender', sender);
      formData.append('recipient', recipient);
      formData.append('file', file);

      try {
        const res = await fetch('/api/send-to', {
          method: 'POST',
          body: formData,
        });
        const data = await res.json();
        if (res.ok && data.status === 'OK') {
          showToast(`File sent! Queued on server for '${recipient}'. Level 1 Receipt recorded!`);
          document.getElementById('direct-send-form').reset();
          document.getElementById('direct-file-label').innerText = 'Click or Drag & Drop file to send';
          fetchOutbox();
          fetchLogs();
        } else {
          showToast(`Transfer failed: ${data.message || 'Error'}`, true);
        }
      } catch (err) {
        showToast(`Send error: ${err}`, true);
      } finally {
        btn.disabled = false;
        btn.innerText = 'Send File & Receive Delivery ACK';
      }
    }

    // Fetch Inbox
    async function fetchInbox() {
      const user = getActiveUser();
      try {
        const res = await fetch(`/api/inbox?user=${encodeURIComponent(user)}`);
        const data = await res.json();
        const tbody = document.getElementById('inbox-tbody');
        tbody.innerHTML = '';

        const inbox = data.inbox || [];
        document.getElementById('inbox-badge').innerText = inbox.length;

        if (inbox.length === 0) {
          tbody.innerHTML = `<tr><td colspan="6" style="text-align:center; color:var(--text-faint); padding:32px;">No incoming files for '${user}'</td></tr>`;
          return;
        }

        inbox.forEach(item => {
          const tr = document.createElement('tr');
          const isPending = item.status === 'PENDING_RECEIPT';
          const badgeClass = isPending ? 'status-pending' : 'status-confirmed';
          const badgeText = isPending ? 'NEW / UNREAD' : 'RECEIVED';

          tr.innerHTML = `
            <td><strong style="color:#fff;">${item.sender}</strong></td>
            <td><strong>${item.filename}</strong></td>
            <td style="font-family:var(--font-mono);">${formatBytes(item.size)}</td>
            <td style="font-size:0.75rem; color:var(--text-muted);">${item.sent_at}</td>
            <td><span class="status-badge ${badgeClass}">${badgeText}</span></td>
            <td style="text-align:right;">
              <a href="/api/receive-inbox?transfer_id=${item.transfer_id}&user=${encodeURIComponent(user)}" class="btn btn-emerald btn-sm" download onclick="setTimeout(fetchInbox, 1000)">
                Receive & Confirm Receipt
              </a>
            </td>
          `;
          tbody.appendChild(tr);
        });
      } catch (err) {
        console.error('Error fetching inbox:', err);
      }
    }

    // Fetch Outbox
    async function fetchOutbox() {
      const user = getActiveUser();
      try {
        const res = await fetch(`/api/outbox?user=${encodeURIComponent(user)}`);
        const data = await res.json();
        const tbody = document.getElementById('outbox-tbody');
        tbody.innerHTML = '';

        const outbox = data.outbox || [];
        document.getElementById('outbox-badge').innerText = outbox.length;

        if (outbox.length === 0) {
          tbody.innerHTML = `<tr><td colspan="5" style="text-align:center; color:var(--text-faint); padding:32px;">No sent files from '${user}'</td></tr>`;
          return;
        }

        outbox.forEach(item => {
          const tr = document.createElement('tr');
          const isConfirmed = item.status === 'RECEIVED_BY_USER';
          const badgeClass = isConfirmed ? 'status-confirmed' : 'status-pending';
          const badgeText = isConfirmed 
            ? `DELIVERED & CONFIRMED (Received at ${item.received_at})` 
            : `SENT TO SERVER (Waiting for ${item.recipient} to download)`;

          tr.innerHTML = `
            <td><strong style="color:#fff;">${item.recipient}</strong></td>
            <td><strong>${item.filename}</strong></td>
            <td style="font-family:var(--font-mono);">${formatBytes(item.size)}</td>
            <td style="font-size:0.75rem; color:var(--text-muted);">${item.sent_at}</td>
            <td><span class="status-badge ${badgeClass}">${badgeText}</span></td>
          `;
          tbody.appendChild(tr);
        });
      } catch (err) {
        console.error('Error fetching outbox:', err);
      }
    }

    // Fetch Public files
    async function fetchPublicFiles() {
      try {
        const res = await fetch('/api/files');
        const data = await res.json();
        const tbody = document.getElementById('public-tbody');
        tbody.innerHTML = '';

        if (!data.files || data.files.length === 0) {
          tbody.innerHTML = `<tr><td colspan="5" style="text-align:center; color:var(--text-faint); padding:32px;">No files currently in public storage.</td></tr>`;
          return;
        }

        data.files.forEach(f => {
          const tr = document.createElement('tr');
          tr.innerHTML = `
            <td><strong>${f.name}</strong></td>
            <td style="font-family:var(--font-mono);">${formatBytes(f.size)}</td>
            <td>${f.type || 'file'}</td>
            <td style="font-size:0.75rem; color:var(--text-muted);">${f.modified}</td>
            <td style="text-align:right;">
              <a href="/api/download?filename=${encodeURIComponent(f.name)}" class="btn btn-secondary btn-sm" download>
                Download
              </a>
            </td>
          `;
          tbody.appendChild(tr);
        });
      } catch (err) {}
    }

    async function fetchLogs() {
      try {
        const res = await fetch('/api/logs');
        const text = await res.text();
        const term = document.getElementById('terminal-body');
        term.innerText = text;
        term.scrollTop = term.scrollHeight;
      } catch (e) {}
    }

    // Initialize
    fetchInbox();
    fetchOutbox();
    fetchPublicFiles();
    fetchLogs();
    setInterval(fetchLogs, 2000);
    setInterval(() => {
      fetchInbox();
      fetchOutbox();
    }, 3000);
  </script>
</body>
</html>
"""


class WebGatewayHandler(BaseHTTPRequestHandler):
    """HTTP Request Handler providing REST endpoints and serving the modern Web Dashboard."""

    def log_message(self, format: str, *args) -> None:
        pass

    def do_GET(self) -> None:
        """Handle GET requests."""
        parsed_url = urlparse(self.path)
        path = parsed_url.path

        if path in ("/", "/index.html"):
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            content = HTML_DASHBOARD.encode("utf-8")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
            return

        elif path == "/api/inbox":
            query = parse_qs(parsed_url.query)
            user = query.get("user", [""])[0]
            client = FileTransferClient(host=TCP_SERVER_HOST, port=TCP_SERVER_PORT)
            if not client.connect():
                self._send_json({"error": "Cannot connect to TCP Server"}, status=503)
                return
            try:
                client.username = user
                inbox = client.view_inbox()
                self._send_json({"status": "OK", "inbox": inbox or []})
            finally:
                client.disconnect()
            return

        elif path == "/api/outbox":
            query = parse_qs(parsed_url.query)
            user = query.get("user", [""])[0]
            client = FileTransferClient(host=TCP_SERVER_HOST, port=TCP_SERVER_PORT)
            if not client.connect():
                self._send_json({"error": "Cannot connect to TCP Server"}, status=503)
                return
            try:
                client.username = user
                outbox = client.view_outbox()
                self._send_json({"status": "OK", "outbox": outbox or []})
            finally:
                client.disconnect()
            return

        elif path == "/api/receive-inbox":
            query = parse_qs(parsed_url.query)
            transfer_id = query.get("transfer_id", [""])[0]
            user = query.get("user", [""])[0]

            if not transfer_id:
                self._send_json({"error": "Missing transfer_id"}, status=400)
                return

            with tempfile.TemporaryDirectory() as temp_dir:
                temp_dl_dir = Path(temp_dir)
                client = FileTransferClient(
                    host=TCP_SERVER_HOST, port=TCP_SERVER_PORT, download_dir=temp_dl_dir
                )
                if not client.connect():
                    self._send_json({"error": "Could not connect to TCP server"}, status=503)
                    return
                try:
                    client.username = user
                    success = client.download_inbox_file(transfer_id=transfer_id, show_progress=False)
                    if not success:
                        self._send_json({"error": "File transfer failed"}, status=404)
                        return

                    files = list(temp_dl_dir.iterdir())
                    if not files:
                        self._send_json({"error": "File missing"}, status=404)
                        return

                    target_file = files[0]
                    file_size = target_file.stat().st_size
                    clean_name = target_file.name

                    mime_type, _ = mimetypes.guess_type(clean_name)
                    mime_type = mime_type or "application/octet-stream"

                    self.send_response(HTTPStatus.OK)
                    self.send_header("Content-Type", mime_type)
                    self.send_header(
                        "Content-Disposition", f'attachment; filename="{clean_name}"'
                    )
                    self.send_header("Content-Length", str(file_size))
                    self.end_headers()

                    with open(target_file, "rb") as f:
                        shutil.copyfileobj(f, self.wfile)
                finally:
                    client.disconnect()
            return

        elif path == "/api/files":
            client = FileTransferClient(host=TCP_SERVER_HOST, port=TCP_SERVER_PORT)
            if not client.connect():
                self._send_json({"error": "Cannot connect to TCP Server"}, status=503)
                return
            try:
                files = client.list_files()
                self._send_json({"status": "OK", "files": files or []})
            finally:
                client.disconnect()
            return

        elif path == "/api/logs":
            log_file = LOGS_DIR / "server.log"
            log_text = ""
            if log_file.exists():
                try:
                    with open(log_file, "r", encoding="utf-8", errors="replace") as f:
                        lines = f.readlines()
                        log_text = "".join(lines[-40:])
                except OSError:
                    log_text = "(Unable to read logs)"
            else:
                log_text = "(No log entries recorded yet)"

            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            payload = log_text.encode("utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        elif path == "/api/download":
            query = parse_qs(parsed_url.query)
            filename = query.get("filename", [""])[0]
            if not filename:
                self._send_json({"error": "Missing filename parameter"}, status=400)
                return

            clean_name = sanitize_filename(filename)
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_dl_path = Path(temp_dir)
                client = FileTransferClient(
                    host=TCP_SERVER_HOST, port=TCP_SERVER_PORT, download_dir=temp_dl_path
                )
                if not client.connect():
                    self._send_json({"error": "Could not connect to TCP server"}, status=503)
                    return

                try:
                    success = client.download_file(clean_name, overwrite=True, show_progress=False)
                    if not success:
                        self._send_json({"error": "File not found or transfer failed"}, status=404)
                        return

                    target_file = temp_dl_path / clean_name
                    if not target_file.exists():
                        self._send_json({"error": "Downloaded file missing"}, status=404)
                        return

                    file_size = target_file.stat().st_size
                    mime_type, _ = mimetypes.guess_type(clean_name)
                    mime_type = mime_type or "application/octet-stream"

                    self.send_response(HTTPStatus.OK)
                    self.send_header("Content-Type", mime_type)
                    self.send_header(
                        "Content-Disposition", f'attachment; filename="{clean_name}"'
                    )
                    self.send_header("Content-Length", str(file_size))
                    self.end_headers()

                    with open(target_file, "rb") as f:
                        shutil.copyfileobj(f, self.wfile)
                finally:
                    client.disconnect()
            return

        else:
            self.send_response(HTTPStatus.NOT_FOUND)
            self.end_headers()

    def do_POST(self) -> None:
        """Handle POST requests for direct two-way transfers and uploads."""
        parsed_url = urlparse(self.path)
        path = parsed_url.path

        if path == "/api/send-to":
            content_type = self.headers.get("Content-Type", "")
            if not content_type.startswith("multipart/form-data"):
                self._send_json({"error": "Expected multipart/form-data"}, status=400)
                return

            try:
                content_length = int(self.headers.get("Content-Length", 0))
            except (TypeError, ValueError):
                content_length = 0

            body = self.rfile.read(content_length)
            raw_data = (
                b"Content-Type: "
                + content_type.encode("ascii", errors="replace")
                + b"\r\n\r\n"
                + body
            )
            msg = BytesParser(policy=policy.default).parsebytes(raw_data)

            sender = "Anonymous"
            recipient = ""
            raw_filename = None
            file_bytes = b""

            if msg.is_multipart():
                for part in msg.iter_parts():
                    name = part.get_param("name", header="content-disposition")
                    if name == "sender":
                        val = part.get_payload(decode=True)
                        if val:
                            sender = val.decode(errors="ignore").strip()
                    elif name == "recipient":
                        val = part.get_payload(decode=True)
                        if val:
                            recipient = val.decode(errors="ignore").strip()
                    elif name == "file":
                        raw_filename = part.get_filename() or "transmitted_file.bin"
                        file_bytes = part.get_payload(decode=True) or b""

            if not recipient or not raw_filename:
                self._send_json({"error": "Missing recipient or file"}, status=400)
                return

            filename = sanitize_filename(raw_filename)

            with tempfile.TemporaryDirectory() as temp_dir:
                temp_upload_file = Path(temp_dir) / filename
                with open(temp_upload_file, "wb") as out_f:
                    out_f.write(file_bytes)

                client = FileTransferClient(host=TCP_SERVER_HOST, port=TCP_SERVER_PORT)
                if not client.connect():
                    self._send_json({"error": "Cannot connect to TCP Server"}, status=503)
                    return

                try:
                    client.username = sender
                    client.register(sender)
                    success = client.send_file_to_user(
                        recipient=recipient,
                        local_path_str=str(temp_upload_file),
                        show_progress=False,
                    )
                    if success:
                        self._send_json({
                            "status": "OK",
                            "message": f"File '{filename}' successfully sent to '{recipient}'. Delivery pending.",
                            "filename": filename,
                            "recipient": recipient,
                        })
                    else:
                        self._send_json(
                            {"status": "ERROR", "message": "Failed to send file over TCP."},
                            status=500,
                        )
                finally:
                    client.disconnect()
            return

        else:
            self.send_response(HTTPStatus.NOT_FOUND)
            self.end_headers()

    def _send_json(self, data: dict, status: int = 200) -> None:
        """Helper to send JSON response."""
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        payload = json.dumps(data).encode("utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def start_web_server(port: int = WEB_PORT) -> None:
    """Run multithreaded HTTP web server."""
    ensure_directories_exist()
    server_address = ("127.0.0.1", port)
    httpd = ThreadingHTTPServer(server_address, WebGatewayHandler)
    print(f"[+] Web Interface running at: http://127.0.0.1:{port}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.server_close()
        print("\n[+] Web server stopped.")


if __name__ == "__main__":
    start_web_server()
