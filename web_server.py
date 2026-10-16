"""Web Interface & HTTP-to-TCP Bridge for Multithreaded File Transfer System.

Provides a modern, glassmorphic Web UI and REST API gateway that communicates
with the underlying multithreaded TCP socket server (server.py) using pure
Python standard library (http.server, socket, threading).
"""

from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse
from email import policy
from email.parser import BytesParser
import io
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
  <title>Multithreaded Network File Transfer System | Dashboard</title>
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

    * {
      box-sizing: border-box;
      margin: 0;
      padding: 0;
    }

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

    /* Ambient background grid */
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
      max-width: 1320px;
      margin: 0 auto;
      padding: 24px 20px 48px;
      position: relative;
      z-index: 1;
      width: 100%;
    }

    /* Header */
    header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding-bottom: 24px;
      margin-bottom: 24px;
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
      width: 46px;
      height: 46px;
      border-radius: 12px;
      background: linear-gradient(135deg, var(--primary) 0%, var(--accent-cyan) 100%);
      display: flex;
      align-items: center;
      justify-content: center;
      box-shadow: 0 0 20px rgba(99, 102, 241, 0.35);
    }

    .brand-icon svg {
      width: 26px;
      height: 26px;
      color: white;
    }

    .brand-titles h1 {
      font-size: 1.35rem;
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

    .header-badge {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      background: rgba(16, 185, 129, 0.1);
      border: 1px solid rgba(16, 185, 129, 0.3);
      padding: 6px 14px;
      border-radius: 9999px;
      font-size: 0.78rem;
      font-weight: 600;
      color: var(--accent-emerald);
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

    /* Metric stats strip */
    .metrics-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 16px;
      margin-bottom: 24px;
    }

    .metric-card {
      background: var(--bg-card);
      backdrop-filter: blur(12px);
      border: 1px solid var(--border-color);
      border-radius: 16px;
      padding: 18px 20px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      transition: all 0.25s ease;
    }

    .metric-card:hover {
      border-color: var(--border-glow);
      transform: translateY(-2px);
      box-shadow: 0 8px 20px rgba(0, 0, 0, 0.3);
    }

    .metric-info h3 {
      font-size: 0.75rem;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.05em;
      margin-bottom: 4px;
    }

    .metric-info .value {
      font-size: 1.45rem;
      font-weight: 800;
      color: #fff;
    }

    .metric-icon {
      width: 42px;
      height: 42px;
      border-radius: 10px;
      display: flex;
      align-items: center;
      justify-content: center;
      background: rgba(99, 102, 241, 0.12);
      color: var(--primary-light);
    }

    /* Main two-column layout */
    .main-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 24px;
      margin-bottom: 24px;
    }

    @media (max-width: 1024px) {
      .main-grid {
        grid-template-columns: 1fr;
      }
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
      margin-bottom: 20px;
    }

    .card-header h2 {
      font-size: 1.05rem;
      font-weight: 700;
      display: flex;
      align-items: center;
      gap: 10px;
    }

    .card-header h2 svg {
      width: 20px;
      height: 20px;
      color: var(--primary-light);
    }

    /* Upload Zone */
    .upload-zone {
      border: 2px dashed rgba(99, 102, 241, 0.3);
      border-radius: 14px;
      padding: 36px 20px;
      text-align: center;
      background: rgba(17, 24, 39, 0.5);
      cursor: pointer;
      transition: all 0.2s ease;
      position: relative;
    }

    .upload-zone:hover, .upload-zone.dragover {
      border-color: var(--accent-cyan);
      background: rgba(6, 182, 212, 0.05);
      transform: scale(0.995);
    }

    .upload-zone input[type="file"] {
      position: absolute;
      inset: 0;
      opacity: 0;
      cursor: pointer;
      width: 100%;
      height: 100%;
    }

    .upload-icon-circle {
      width: 54px;
      height: 54px;
      border-radius: 50%;
      background: rgba(99, 102, 241, 0.15);
      color: var(--primary-light);
      display: flex;
      align-items: center;
      justify-content: center;
      margin: 0 auto 12px;
    }

    .upload-zone h4 {
      font-size: 0.95rem;
      font-weight: 600;
      margin-bottom: 4px;
    }

    .upload-zone p {
      font-size: 0.8rem;
      color: var(--text-muted);
    }

    .upload-controls {
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-top: 16px;
      padding-top: 14px;
      border-top: 1px solid rgba(255, 255, 255, 0.06);
    }

    .toggle-wrap {
      display: flex;
      align-items: center;
      gap: 10px;
      font-size: 0.82rem;
      color: var(--text-muted);
      cursor: pointer;
    }

    .toggle-wrap input {
      accent-color: var(--primary);
      width: 16px;
      height: 16px;
      cursor: pointer;
    }

    /* Progress block */
    .progress-box {
      margin-top: 16px;
      background: rgba(11, 17, 30, 0.8);
      border: 1px solid var(--border-color);
      border-radius: 12px;
      padding: 14px 16px;
      display: none;
    }

    .progress-meta {
      display: flex;
      justify-content: space-between;
      font-size: 0.8rem;
      margin-bottom: 8px;
      font-family: var(--font-mono);
    }

    .progress-bar-track {
      height: 8px;
      border-radius: 9999px;
      background: rgba(255, 255, 255, 0.08);
      overflow: hidden;
      position: relative;
    }

    .progress-bar-fill {
      height: 100%;
      width: 0%;
      background: linear-gradient(90deg, var(--primary) 0%, var(--accent-cyan) 100%);
      border-radius: 9999px;
      transition: width 0.2s ease;
    }

    /* Concurrency Benchmark Demo Widget */
    .concurrency-box {
      margin-top: 18px;
      background: linear-gradient(135deg, rgba(99, 102, 241, 0.08) 0%, rgba(6, 182, 212, 0.05) 100%);
      border: 1px solid rgba(99, 102, 241, 0.25);
      border-radius: 14px;
      padding: 18px;
    }

    .concurrency-box h4 {
      font-size: 0.88rem;
      font-weight: 700;
      color: #fff;
      margin-bottom: 6px;
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .concurrency-box p {
      font-size: 0.78rem;
      color: var(--text-muted);
      margin-bottom: 12px;
    }

    .btn {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      padding: 9px 18px;
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

    .btn-sm {
      padding: 6px 12px;
      font-size: 0.75rem;
    }

    .btn-secondary {
      background: rgba(255, 255, 255, 0.07);
      color: var(--text-main);
      border: 1px solid rgba(255, 255, 255, 0.12);
    }

    .btn-secondary:hover {
      background: rgba(255, 255, 255, 0.12);
      border-color: rgba(255, 255, 255, 0.2);
    }

    /* Table styles */
    .table-container {
      overflow-x: auto;
      max-height: 480px;
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

    tr:hover td {
      background: rgba(99, 102, 241, 0.04);
    }

    .file-name-cell {
      display: flex;
      align-items: center;
      gap: 10px;
      font-weight: 600;
    }

    .file-type-pill {
      display: inline-block;
      padding: 2px 7px;
      border-radius: 6px;
      font-size: 0.68rem;
      font-weight: 700;
      text-transform: uppercase;
      background: rgba(255, 255, 255, 0.08);
      color: var(--text-muted);
      font-family: var(--font-mono);
    }

    .type-pdf { background: rgba(244, 63, 94, 0.15); color: #fb7185; }
    .type-txt, .type-md { background: rgba(6, 182, 212, 0.15); color: #38bdf8; }
    .type-zip, .type-bin { background: rgba(245, 158, 11, 0.15); color: #fbbf24; }
    .type-py { background: rgba(16, 185, 129, 0.15); color: #34d399; }

    /* Live Terminal Console */
    .terminal-card {
      background: #070a12;
      border: 1px solid rgba(255, 255, 255, 0.08);
      border-radius: 16px;
      padding: 18px 20px;
      font-family: var(--font-mono);
      display: flex;
      flex-direction: column;
      box-shadow: 0 10px 30px rgba(0, 0, 0, 0.4);
    }

    .terminal-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding-bottom: 12px;
      margin-bottom: 12px;
      border-bottom: 1px solid rgba(255, 255, 255, 0.06);
    }

    .terminal-dots {
      display: flex;
      gap: 6px;
    }

    .terminal-dots span {
      width: 10px;
      height: 10px;
      border-radius: 50%;
    }

    .dot-red { background: #ef4444; }
    .dot-yellow { background: #eab308; }
    .dot-green { background: #22c55e; }

    .terminal-title {
      font-size: 0.75rem;
      color: var(--text-faint);
    }

    .terminal-body {
      background: rgba(0, 0, 0, 0.4);
      border-radius: 10px;
      padding: 14px;
      height: 220px;
      overflow-y: auto;
      font-size: 0.74rem;
      line-height: 1.6;
      color: #cbd5e1;
      white-space: pre-wrap;
      word-break: break-all;
    }

    .log-line {
      margin-bottom: 3px;
    }

    .log-info { color: #60a5fa; }
    .log-connect { color: #34d399; font-weight: 700; }
    .log-req { color: #f472b6; }
    .log-warn { color: #fbbf24; }
    .log-error { color: #f87171; font-weight: 700; }

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
      animation: slideUp 0.3s ease;
    }

    @keyframes slideUp {
      from { transform: translateY(20px); opacity: 0; }
      to { transform: translateY(0); opacity: 1; }
    }
  </style>
</head>
<body>
  <div class="container">
    <!-- Header -->
    <header>
      <div class="brand">
        <div class="brand-icon">
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
            <path stroke-linecap="round" stroke-linejoin="round" d="M3 15a4 4 0 004 4h9a5 5 0 10-.1-9.999 5.002 5.002 0 00-9.78 2.096A4.001 4.001 0 003 15z" />
          </svg>
        </div>
        <div class="brand-titles">
          <h1>Multithreaded Network File Transfer</h1>
          <p>Academic Socket Programming & Concurrency Demonstration</p>
        </div>
      </div>
      <div class="header-badge">
        <span class="pulse-dot"></span>
        <span>TCP Server Online (127.0.0.1:5000)</span>
      </div>
    </header>

    <!-- Metrics Strip -->
    <div class="metrics-grid">
      <div class="metric-card">
        <div class="metric-info">
          <h3>Socket Backend</h3>
          <div class="value" id="val-backend">TCP / IP</div>
        </div>
        <div class="metric-icon">
          <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M5 12h14M12 5l7 7-7 7"/></svg>
        </div>
      </div>
      <div class="metric-card">
        <div class="metric-info">
          <h3>Hosted Files</h3>
          <div class="value" id="val-count">--</div>
        </div>
        <div class="metric-icon">
          <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>
        </div>
      </div>
      <div class="metric-card">
        <div class="metric-info">
          <h3>Storage Consumed</h3>
          <div class="value" id="val-size">--</div>
        </div>
        <div class="metric-icon">
          <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/><path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/></svg>
        </div>
      </div>
      <div class="metric-card">
        <div class="metric-info">
          <h3>Concurrency Model</h3>
          <div class="value" style="font-size:1.15rem; color:var(--accent-emerald);">Thread-Per-Client</div>
        </div>
        <div class="metric-icon">
          <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>
        </div>
      </div>
    </div>

    <!-- Main Two-Column View -->
    <div class="main-grid">
      <!-- Left Column: Upload & Concurrency Benchmark -->
      <div class="card">
        <div class="card-header">
          <h2>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>
            Upload File via TCP Stream
          </h2>
        </div>

        <div class="upload-zone" id="drop-zone">
          <input type="file" id="file-input">
          <div class="upload-icon-circle">
            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 14.899A7 7 0 1 1 15.71 8h1.79a4.5 4.5 0 0 1 2.5 8.242M12 12v9m-4-4 4-4 4 4"/></svg>
          </div>
          <h4>Drag & Drop file here, or click to browse</h4>
          <p>Transfers are framed and streamed in 64 KB chunks directly to server_storage/</p>
        </div>

        <div class="upload-controls">
          <label class="toggle-wrap">
            <input type="checkbox" id="overwrite-check">
            <span>Allow Overwrite if file exists</span>
          </label>
          <span id="selected-file-label" style="font-size:0.75rem; color:var(--text-muted);">No file selected</span>
        </div>

        <!-- Progress Box -->
        <div class="progress-box" id="progress-box">
          <div class="progress-meta">
            <span id="prog-filename">uploading.bin</span>
            <span id="prog-stats">0%</span>
          </div>
          <div class="progress-bar-track">
            <div class="progress-bar-fill" id="prog-bar"></div>
          </div>
        </div>

        <!-- Concurrency Demonstration -->
        <div class="concurrency-box">
          <h4>
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/></svg>
            Live Multithreaded Concurrency Demo
          </h4>
          <p>Launch 3 simulated clients simultaneously over distinct TCP threads uploading independent payloads.</p>
          <button class="btn btn-primary btn-sm" id="btn-simulate" onclick="runConcurrencySimulation()">
            Simulate 3 Concurrent Client Uploads
          </button>
        </div>
      </div>

      <!-- Right Column: Hosted Files Table -->
      <div class="card">
        <div class="card-header">
          <h2>
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M3 3v18h18"/><path d="M18.7 8l-5.1 5.2-2.8-2.7L7 14.3"/></svg>
            Hosted Files in Storage
          </h2>
          <button class="btn btn-secondary btn-sm" onclick="fetchFiles()">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M23 4v6h-6M1 20v-6h6M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></svg>
            Refresh List
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
            <tbody id="files-tbody">
              <tr>
                <td colspan="5" style="text-align: center; color: var(--text-faint); padding: 32px;">Loading hosted files...</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- Bottom: Live Server Log Terminal -->
    <div class="terminal-card">
      <div class="terminal-header">
        <div class="terminal-dots">
          <span class="dot-red"></span>
          <span class="dot-yellow"></span>
          <span class="dot-green"></span>
        </div>
        <div class="terminal-title">server.log &mdash; Live TCP Socket Events & Thread Monitor</div>
        <div style="font-size:0.7rem; color:var(--accent-emerald);">● Streaming Active</div>
      </div>
      <div class="terminal-body" id="terminal-body">Connecting to log stream...</div>
    </div>
  </div>

  <div id="toast">File uploaded successfully!</div>

  <script>
    // Format bytes helper
    function formatBytes(bytes) {
      if (bytes === 0) return '0 B';
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
      setTimeout(() => { toast.style.display = 'none'; }, 3500);
    }

    // Refresh Files List from TCP server via /api/files
    async function fetchFiles() {
      try {
        const res = await fetch('/api/files');
        const data = await res.json();
        const tbody = document.getElementById('files-tbody');
        tbody.innerHTML = '';

        if (!data.files || data.files.length === 0) {
          tbody.innerHTML = `<tr><td colspan="5" style="text-align:center; color:var(--text-faint); padding: 32px;">No files currently in server_storage/</td></tr>`;
          document.getElementById('val-count').innerText = '0';
          document.getElementById('val-size').innerText = '0 B';
          return;
        }

        let totalBytes = 0;
        data.files.forEach(file => {
          totalBytes += file.size;
          const tr = document.createElement('tr');
          const extClass = 'type-' + (file.type ? file.type.toLowerCase() : 'other');

          tr.innerHTML = `
            <td class="file-name-cell">
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="color:var(--primary-light)"><path d="M13 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V9z"/><polyline points="13 2 13 9 20 9"/></svg>
              <span>${file.name}</span>
            </td>
            <td style="font-family:var(--font-mono);">${formatBytes(file.size)}</td>
            <td><span class="file-type-pill ${extClass}">${file.type || 'file'}</span></td>
            <td style="color:var(--text-muted); font-size:0.75rem;">${file.modified}</td>
            <td style="text-align: right;">
              <a href="/api/download?filename=${encodeURIComponent(file.name)}" class="btn btn-secondary btn-sm" download>
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4M7 10l5 5 5-5M12 15V3"/></svg>
                Download
              </a>
            </td>
          `;
          tbody.appendChild(tr);
        });

        document.getElementById('val-count').innerText = data.files.length;
        document.getElementById('val-size').innerText = formatBytes(totalBytes);
      } catch (err) {
        console.error('Error fetching files:', err);
      }
    }

    // Refresh logs periodically
    async function fetchLogs() {
      try {
        const res = await fetch('/api/logs');
        const text = await res.text();
        const term = document.getElementById('terminal-body');
        term.innerText = text;
        term.scrollTop = term.scrollHeight;
      } catch (e) {}
    }

    // File Upload handling
    const fileInput = document.getElementById('file-input');
    const dropZone = document.getElementById('drop-zone');

    dropZone.addEventListener('dragover', (e) => {
      e.preventDefault();
      dropZone.classList.add('dragover');
    });

    dropZone.addEventListener('dragleave', () => {
      dropZone.classList.remove('dragover');
    });

    dropZone.addEventListener('drop', (e) => {
      e.preventDefault();
      dropZone.classList.remove('dragover');
      if (e.dataTransfer.files.length > 0) {
        uploadFile(e.dataTransfer.files[0]);
      }
    });

    fileInput.addEventListener('change', () => {
      if (fileInput.files.length > 0) {
        uploadFile(fileInput.files[0]);
      }
    });

    async function uploadFile(file) {
      document.getElementById('selected-file-label').innerText = `${file.name} (${formatBytes(file.size)})`;
      const overwrite = document.getElementById('overwrite-check').checked;
      const progBox = document.getElementById('progress-box');
      const progBar = document.getElementById('prog-bar');
      const progMeta = document.getElementById('prog-stats');
      const progFilename = document.getElementById('prog-filename');

      progFilename.innerText = file.name;
      progBar.style.width = '0%';
      progMeta.innerText = '0%';
      progBox.style.display = 'block';

      const formData = new FormData();
      formData.append('file', file);
      formData.append('overwrite', overwrite ? 'true' : 'false');

      const xhr = new XMLHttpRequest();
      xhr.open('POST', '/api/upload', true);

      xhr.upload.onprogress = (e) => {
        if (e.lengthComputable) {
          const percent = Math.round((e.loaded / e.total) * 100);
          progBar.style.width = percent + '%';
          progMeta.innerText = `${percent}% (${formatBytes(e.loaded)} / ${formatBytes(e.total)})`;
        }
      };

      xhr.onload = () => {
        if (xhr.status === 200) {
          showToast(`File '${file.name}' uploaded successfully via TCP stream!`);
          setTimeout(() => { progBox.style.display = 'none'; }, 1000);
          fetchFiles();
          fetchLogs();
        } else {
          try {
            const errData = JSON.parse(xhr.responseText);
            showToast(`Upload failed: ${errData.message || 'Error'}`, true);
          } catch(e) {
            showToast(`Upload failed with status ${xhr.status}`, true);
          }
        }
      };

      xhr.onerror = () => {
        showToast('Network error during upload', true);
      };

      xhr.send(formData);
    }

    async function runConcurrencySimulation() {
      const btn = document.getElementById('btn-simulate');
      btn.disabled = true;
      btn.innerText = 'Simulating 3 concurrent client threads...';

      try {
        const res = await fetch('/api/simulate-concurrency', { method: 'POST' });
        const data = await res.json();
        showToast('3 concurrent clients successfully transferred files simultaneously!');
        fetchFiles();
        fetchLogs();
      } catch (err) {
        showToast('Concurrency test failed: ' + err, true);
      } finally {
        btn.disabled = false;
        btn.innerText = 'Simulate 3 Concurrent Client Uploads';
      }
    }

    // Initial load and polling
    fetchFiles();
    fetchLogs();
    setInterval(fetchLogs, 1500);
  </script>
</body>
</html>
"""


class WebGatewayHandler(BaseHTTPRequestHandler):
    """HTTP Request Handler providing REST endpoints and serving the modern Web Dashboard."""

    def log_message(self, format: str, *args) -> None:
        # Suppress noisy HTTP stdout logging to keep terminal clean
        pass

    def do_GET(self) -> None:
        """Handle GET requests for static dashboard, API file listing, download, and log stream."""
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

        elif path == "/api/files":
            # Connect to underlying TCP server and request LIST
            client = FileTransferClient(host=TCP_SERVER_HOST, port=TCP_SERVER_PORT)
            connected = client.connect()
            if not connected:
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
            # Create a temporary download directory for this client request
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
        """Handle POST requests for file uploads and concurrent load simulation."""
        parsed_url = urlparse(self.path)
        path = parsed_url.path

        if path == "/api/upload":
            content_type = self.headers.get("Content-Type", "")
            if not content_type.startswith("multipart/form-data"):
                self._send_json({"error": "Expected multipart/form-data"}, status=400)
                return

            try:
                content_length = int(self.headers.get("Content-Length", 0))
            except (TypeError, ValueError):
                content_length = 0

            body = self.rfile.read(content_length)

            # Construct raw multipart byte message and parse using email policy
            raw_data = (
                b"Content-Type: "
                + content_type.encode("ascii", errors="replace")
                + b"\r\n\r\n"
                + body
            )
            msg = BytesParser(policy=policy.default).parsebytes(raw_data)

            raw_filename = None
            file_bytes = b""
            overwrite = False

            if msg.is_multipart():
                for part in msg.iter_parts():
                    name = part.get_param("name", header="content-disposition")
                    if name == "file":
                        raw_filename = part.get_filename() or "uploaded_file.bin"
                        file_bytes = part.get_payload(decode=True) or b""
                    elif name == "overwrite":
                        payload = part.get_payload(decode=True)
                        if payload:
                            overwrite = payload.decode(errors="ignore").strip().lower() == "true"

            if not raw_filename:
                self._send_json({"error": "No file uploaded"}, status=400)
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
                    success = client.upload_file(
                        str(temp_upload_file), overwrite=overwrite, show_progress=False
                    )
                    if success:
                        self._send_json({"status": "OK", "filename": filename})
                    else:
                        self._send_json(
                            {
                                "status": "ERROR",
                                "message": f"Upload rejected. File '{filename}' may already exist on server.",
                            },
                            status=409,
                        )
                finally:
                    client.disconnect()
            return

        elif path == "/api/simulate-concurrency":
            # Concurrency demonstration endpoint: launches 3 client worker threads in parallel
            results = []
            errors = []

            def sim_worker(cid: int):
                with tempfile.TemporaryDirectory() as t_dir:
                    c_path = Path(t_dir) / f"demo_client_{cid}.txt"
                    c_path.write_text(
                        f"Concurrent Client {cid} payload generated at {time.time()}\n" * 50,
                        encoding="utf-8",
                    )
                    c = FileTransferClient(host=TCP_SERVER_HOST, port=TCP_SERVER_PORT)
                    if c.connect():
                        ok = c.upload_file(str(c_path), overwrite=True, show_progress=False)
                        c.disconnect()
                        if ok:
                            results.append(cid)
                        else:
                            errors.append(f"Client {cid} failed")
                    else:
                        errors.append(f"Client {cid} could not connect")

            threads = [
                threading.Thread(target=sim_worker, args=(1,)),
                threading.Thread(target=sim_worker, args=(2,)),
                threading.Thread(target=sim_worker, args=(3,)),
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=5.0)

            self._send_json({
                "status": "OK",
                "successful_clients": len(results),
                "errors": errors,
            })
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
