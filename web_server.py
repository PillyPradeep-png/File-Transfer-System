"""Web Interface & HTTP-to-TCP Bridge for Multithreaded File Transfer System.

Features an Apple iPhone iOS Glassmorphism (Frosted Glass / Cupertino) UI,
interactive iOS widgets, live Dynamic Island activity bar, user accounts with
unique usernames and auto-generated User IDs, dual-addressing two-way transfers,
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
from user_manager import user_manager

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
  <title>Multithreaded Network File Transfer | iOS Glass & Widgets</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800&family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
  <style>
    :root {
      /* Apple iOS 18 Color Palette (Light & Frosted Glass) */
      --ios-blue: #007aff;
      --ios-blue-light: rgba(0, 122, 255, 0.12);
      --ios-purple: #5856d6;
      --ios-indigo: #4338ca;
      --ios-green: #34c759;
      --ios-green-light: rgba(52, 199, 89, 0.15);
      --ios-orange: #ff9500;
      --ios-orange-light: rgba(255, 149, 0, 0.15);
      --ios-red: #ff3b30;
      --ios-red-light: rgba(255, 59, 48, 0.12);
      --ios-teal: #30b0c7;

      /* Apple Frosted Glass Tokens */
      --glass-bg: rgba(255, 255, 255, 0.72);
      --glass-bg-hover: rgba(255, 255, 255, 0.86);
      --glass-bg-active: rgba(255, 255, 255, 0.95);
      --glass-border: rgba(255, 255, 255, 0.85);
      --glass-shadow: 0 12px 36px rgba(15, 23, 42, 0.07), 0 2px 8px rgba(15, 23, 42, 0.04);
      --glass-inset: inset 0 1px 1.5px rgba(255, 255, 255, 0.95);

      /* Typography */
      --text-primary: #0f172a;
      --text-secondary: #475569;
      --text-muted: #64748b;
      --text-faint: #94a3b8;
      --font-sans: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, 'SF Pro Display', 'Segoe UI', Roboto, sans-serif;
      --font-mono: 'JetBrains Mono', 'SF Mono', Menlo, Consolas, monospace;
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      background: 
        radial-gradient(at 0% 0%, rgba(147, 197, 253, 0.45) 0px, transparent 50%),
        radial-gradient(at 100% 0%, rgba(192, 132, 252, 0.45) 0px, transparent 50%),
        radial-gradient(at 100% 100%, rgba(244, 114, 182, 0.35) 0px, transparent 50%),
        radial-gradient(at 0% 100%, rgba(52, 211, 153, 0.35) 0px, transparent 50%),
        radial-gradient(at 50% 50%, rgba(254, 215, 170, 0.25) 0px, transparent 50%),
        #f1f5f9;
      background-attachment: fixed;
      color: var(--text-primary);
      font-family: var(--font-sans);
      min-height: 100vh;
      display: flex;
      flex-direction: column;
      overflow-x: hidden;
      line-height: 1.5;
      -webkit-font-smoothing: antialiased;
    }

    .container {
      max-width: 1360px;
      margin: 0 auto;
      padding: 20px 24px 60px;
      width: 100%;
      position: relative;
    }

    /* ========================================================
       1. iOS DYNAMIC ISLAND / LIVE ACTIVITY PILL
       ======================================================== */
    .dynamic-island-wrapper {
      display: flex;
      justify-content: center;
      margin-bottom: 20px;
    }

    .dynamic-island {
      background: rgba(15, 23, 42, 0.92);
      color: #fff;
      backdrop-filter: blur(24px);
      -webkit-backdrop-filter: blur(24px);
      border-radius: 9999px;
      padding: 8px 22px;
      display: inline-flex;
      align-items: center;
      gap: 14px;
      box-shadow: 0 10px 25px rgba(15, 23, 42, 0.2);
      border: 1px solid rgba(255, 255, 255, 0.15);
      font-size: 0.8rem;
      font-weight: 600;
      transition: all 0.3s cubic-bezier(0.16, 1, 0.3, 1);
    }

    .dynamic-island:hover {
      transform: scale(1.02);
      box-shadow: 0 12px 30px rgba(15, 23, 42, 0.28);
    }

    .island-dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: var(--ios-green);
      box-shadow: 0 0 10px var(--ios-green);
      animation: islandPulse 2s infinite ease-in-out;
    }

    @keyframes islandPulse {
      0%, 100% { opacity: 1; transform: scale(1); }
      50% { opacity: 0.5; transform: scale(0.85); }
    }

    /* ========================================================
       2. iOS FROSTED GLASS HEADER
       ======================================================== */
    header {
      background: var(--glass-bg);
      backdrop-filter: blur(28px) saturate(190%);
      -webkit-backdrop-filter: blur(28px) saturate(190%);
      border: 1px solid var(--glass-border);
      box-shadow: var(--glass-shadow), var(--glass-inset);
      border-radius: 24px;
      padding: 16px 24px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 22px;
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
      border-radius: 14px;
      background: linear-gradient(135deg, #007aff 0%, #5856d6 100%);
      display: flex;
      align-items: center;
      justify-content: center;
      box-shadow: 0 8px 20px rgba(0, 122, 255, 0.3);
    }

    .brand-icon svg { width: 28px; height: 28px; color: white; }

    .brand-titles h1 {
      font-size: 1.35rem;
      font-weight: 800;
      letter-spacing: -0.02em;
      color: var(--text-primary);
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
      background: rgba(255, 255, 255, 0.8);
      border: 1px solid rgba(0, 0, 0, 0.08);
      padding: 6px 14px;
      border-radius: 16px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.03);
    }

    .user-avatar {
      width: 36px;
      height: 36px;
      border-radius: 50%;
      background: linear-gradient(135deg, #007aff, #34c759);
      display: flex;
      align-items: center;
      justify-content: center;
      font-weight: 800;
      font-size: 0.95rem;
      color: #fff;
      box-shadow: 0 4px 10px rgba(0, 122, 255, 0.25);
    }

    .user-meta-name {
      font-weight: 700;
      font-size: 0.92rem;
      color: var(--text-primary);
    }

    .user-uid-pill {
      background: var(--ios-blue-light);
      border: 1px solid rgba(0, 122, 255, 0.25);
      color: var(--ios-blue);
      font-family: var(--font-mono);
      font-size: 0.72rem;
      font-weight: 700;
      padding: 2px 8px;
      border-radius: 8px;
      letter-spacing: 0.03em;
    }

    /* ========================================================
       3. iOS WIDGETS SECTION (HOME SCREEN WIDGETS)
       ======================================================== */
    .widgets-section-title {
      font-size: 0.85rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--text-muted);
      margin-bottom: 12px;
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .widgets-grid {
      display: grid;
      grid-template-columns: repeat(4, 1fr);
      gap: 18px;
      margin-bottom: 26px;
    }

    @media (max-width: 1200px) {
      .widgets-grid { grid-template-columns: repeat(2, 1fr); }
    }
    @media (max-width: 680px) {
      .widgets-grid { grid-template-columns: 1fr; }
    }

    .widget-card {
      background: var(--glass-bg);
      backdrop-filter: blur(28px) saturate(190%);
      -webkit-backdrop-filter: blur(28px) saturate(190%);
      border: 1px solid var(--glass-border);
      border-radius: 22px;
      padding: 18px;
      box-shadow: var(--glass-shadow), var(--glass-inset);
      transition: all 0.25s cubic-bezier(0.16, 1, 0.3, 1);
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      position: relative;
      overflow: hidden;
    }

    .widget-card:hover {
      transform: translateY(-3px);
      background: var(--glass-bg-hover);
      box-shadow: 0 16px 40px rgba(15, 23, 42, 0.09), 0 3px 10px rgba(15, 23, 42, 0.05);
    }

    .widget-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 12px;
    }

    .widget-label {
      font-size: 0.76rem;
      font-weight: 700;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.03em;
    }

    .widget-icon-pill {
      width: 32px;
      height: 32px;
      border-radius: 10px;
      display: flex;
      align-items: center;
      justify-content: center;
    }

    .widget-icon-blue { background: var(--ios-blue-light); color: var(--ios-blue); }
    .widget-icon-green { background: var(--ios-green-light); color: var(--ios-green); }
    .widget-icon-orange { background: var(--ios-orange-light); color: var(--ios-orange); }
    .widget-icon-purple { background: rgba(88, 86, 214, 0.15); color: var(--ios-purple); }

    .widget-val-lg {
      font-size: 1.55rem;
      font-weight: 800;
      letter-spacing: -0.02em;
      color: var(--text-primary);
      margin-bottom: 4px;
    }

    .widget-subtext {
      font-size: 0.75rem;
      color: var(--text-secondary);
      font-weight: 500;
    }

    /* Quick Speed-Dial Avatars in Widget */
    .widget-contacts-row {
      display: flex;
      align-items: center;
      gap: 10px;
      margin-top: 10px;
    }

    .widget-contact-chip {
      background: rgba(255, 255, 255, 0.9);
      border: 1px solid rgba(0, 0, 0, 0.06);
      padding: 4px 10px;
      border-radius: 12px;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      font-size: 0.74rem;
      font-weight: 700;
      color: var(--text-primary);
      cursor: pointer;
      transition: all 0.2s ease;
      box-shadow: 0 2px 6px rgba(0,0,0,0.03);
    }

    .widget-contact-chip:hover {
      background: var(--ios-blue);
      color: #fff;
      transform: scale(1.04);
    }

    /* ========================================================
       4. iOS SEGMENTED CONTROL TABS
       ======================================================== */
    .segmented-control-container {
      margin-bottom: 24px;
      display: flex;
      justify-content: center;
    }

    .segmented-control {
      background: rgba(255, 255, 255, 0.65);
      backdrop-filter: blur(20px);
      -webkit-backdrop-filter: blur(20px);
      border: 1px solid var(--glass-border);
      border-radius: 18px;
      padding: 5px;
      display: inline-flex;
      gap: 4px;
      box-shadow: 0 4px 16px rgba(15, 23, 42, 0.04);
      overflow-x: auto;
      max-width: 100%;
    }

    .segment-btn {
      background: transparent;
      border: none;
      color: var(--text-secondary);
      font-family: var(--font-sans);
      font-weight: 600;
      font-size: 0.85rem;
      padding: 8px 18px;
      border-radius: 14px;
      cursor: pointer;
      transition: all 0.2s cubic-bezier(0.16, 1, 0.3, 1);
      display: inline-flex;
      align-items: center;
      gap: 8px;
      white-space: nowrap;
    }

    .segment-btn:hover {
      color: var(--text-primary);
    }

    .segment-btn.active {
      background: #ffffff;
      color: var(--ios-blue);
      box-shadow: 0 3px 12px rgba(0, 0, 0, 0.08);
      font-weight: 700;
    }

    .badge-count {
      background: var(--ios-blue-light);
      color: var(--ios-blue);
      font-size: 0.72rem;
      padding: 2px 7px;
      border-radius: 9999px;
      font-weight: 800;
    }

    .segment-btn.active .badge-count {
      background: var(--ios-blue);
      color: #ffffff;
    }

    /* ========================================================
       5. CONTENT CARDS & GRIDS
       ======================================================== */
    .tab-content { display: none; }
    .tab-content.active { display: block; animation: iosFadeIn 0.28s ease; }

    @keyframes iosFadeIn {
      from { opacity: 0; transform: translateY(8px); }
      to { opacity: 1; transform: translateY(0); }
    }

    .grid-two {
      display: grid;
      grid-template-columns: 1.15fr 0.85fr;
      gap: 24px;
      margin-bottom: 24px;
    }

    @media (max-width: 1024px) {
      .grid-two { grid-template-columns: 1fr; }
    }

    .card {
      background: var(--glass-bg);
      backdrop-filter: blur(28px) saturate(190%);
      -webkit-backdrop-filter: blur(28px) saturate(190%);
      border: 1px solid var(--glass-border);
      border-radius: 24px;
      padding: 26px;
      display: flex;
      flex-direction: column;
      box-shadow: var(--glass-shadow), var(--glass-inset);
    }

    .card-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 20px;
    }

    .card-header h2 {
      font-size: 1.15rem;
      font-weight: 800;
      letter-spacing: -0.01em;
      display: flex;
      align-items: center;
      gap: 10px;
      color: var(--text-primary);
    }

    .form-group {
      margin-bottom: 18px;
    }

    .form-group label {
      display: block;
      font-size: 0.8rem;
      font-weight: 700;
      color: var(--text-secondary);
      margin-bottom: 6px;
    }

    .input-control {
      width: 100%;
      background: rgba(255, 255, 255, 0.85);
      border: 1px solid rgba(0, 0, 0, 0.1);
      border-radius: 14px;
      padding: 12px 16px;
      font-size: 0.88rem;
      color: var(--text-primary);
      font-family: var(--font-sans);
      outline: none;
      transition: all 0.2s ease;
      box-shadow: inset 0 1px 3px rgba(0, 0, 0, 0.03);
    }

    .input-control:focus {
      border-color: var(--ios-blue);
      background: #ffffff;
      box-shadow: 0 0 0 4px var(--ios-blue-light);
    }

    /* Recipient Suggestion Pills */
    .recipient-pills {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 8px;
    }

    .pill-btn {
      background: rgba(255, 255, 255, 0.8);
      border: 1px solid rgba(0, 0, 0, 0.08);
      color: var(--text-primary);
      font-size: 0.75rem;
      font-weight: 600;
      padding: 5px 12px;
      border-radius: 20px;
      cursor: pointer;
      transition: all 0.2s ease;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      box-shadow: 0 2px 5px rgba(0,0,0,0.02);
    }

    .pill-btn:hover {
      background: var(--ios-blue);
      border-color: var(--ios-blue);
      color: #fff;
      transform: translateY(-1px);
    }

    .pill-uid {
      font-family: var(--font-mono);
      font-size: 0.7rem;
      color: var(--ios-blue);
      background: var(--ios-blue-light);
      padding: 2px 6px;
      border-radius: 6px;
    }

    .pill-btn:hover .pill-uid {
      background: rgba(255, 255, 255, 0.25);
      color: #fff;
    }

    /* iOS Drag & Drop File Zone */
    .file-drop-area {
      border: 2px dashed rgba(0, 122, 255, 0.35);
      border-radius: 20px;
      padding: 28px 20px;
      text-align: center;
      background: rgba(255, 255, 255, 0.6);
      cursor: pointer;
      position: relative;
      transition: all 0.2s ease;
    }

    .file-drop-area:hover {
      border-color: var(--ios-blue);
      background: rgba(0, 122, 255, 0.04);
      transform: scale(1.005);
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
      font-family: var(--font-sans);
      font-weight: 700;
      font-size: 0.88rem;
      padding: 12px 20px;
      border-radius: 14px;
      border: none;
      cursor: pointer;
      transition: all 0.2s cubic-bezier(0.16, 1, 0.3, 1);
      text-decoration: none;
    }

    .btn:active {
      transform: scale(0.97);
    }

    .btn-primary {
      background: linear-gradient(135deg, #007aff 0%, #0051d5 100%);
      color: #fff;
      box-shadow: 0 4px 16px rgba(0, 122, 255, 0.35);
    }

    .btn-primary:hover {
      box-shadow: 0 6px 20px rgba(0, 122, 255, 0.45);
      transform: translateY(-1px);
    }

    .btn-secondary {
      background: rgba(255, 255, 255, 0.8);
      color: var(--text-primary);
      border: 1px solid rgba(0, 0, 0, 0.1);
      box-shadow: 0 2px 6px rgba(0,0,0,0.03);
    }

    .btn-secondary:hover {
      background: #ffffff;
      border-color: rgba(0, 0, 0, 0.16);
    }

    .btn-emerald {
      background: linear-gradient(135deg, #34c759 0%, #28a745 100%);
      color: white;
      box-shadow: 0 4px 14px rgba(52, 199, 89, 0.35);
    }

    .btn-emerald:hover {
      opacity: 0.95;
      transform: translateY(-1px);
    }

    .btn-sm {
      padding: 7px 14px;
      font-size: 0.78rem;
      border-radius: 10px;
    }

    /* Tables */
    .table-container {
      overflow-x: auto;
      border-radius: 16px;
      border: 1px solid rgba(0, 0, 0, 0.08);
      background: rgba(255, 255, 255, 0.7);
    }

    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.85rem;
      text-align: left;
    }

    th {
      background: rgba(248, 250, 252, 0.85);
      color: var(--text-muted);
      font-weight: 700;
      padding: 14px 18px;
      border-bottom: 1px solid rgba(0, 0, 0, 0.08);
      font-size: 0.76rem;
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }

    td {
      padding: 16px 18px;
      border-bottom: 1px solid rgba(0, 0, 0, 0.05);
      color: var(--text-primary);
    }

    tr:last-child td { border-bottom: none; }
    tr:hover td { background: rgba(0, 122, 255, 0.02); }

    /* iOS Status Badges */
    .status-badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 4px 12px;
      border-radius: 9999px;
      font-size: 0.74rem;
      font-weight: 800;
      letter-spacing: 0.02em;
    }

    .status-pending {
      background: var(--ios-orange-light);
      color: #b45309;
      border: 1px solid rgba(255, 149, 0, 0.3);
    }

    .status-confirmed {
      background: var(--ios-green-light);
      color: #15803d;
      border: 1px solid rgba(52, 199, 89, 0.3);
    }

    /* iOS Developer Log Console (Translucent Glass) */
    .terminal-card {
      background: rgba(15, 23, 42, 0.95);
      color: #e2e8f0;
      backdrop-filter: blur(28px);
      -webkit-backdrop-filter: blur(28px);
      border: 1px solid rgba(255, 255, 255, 0.15);
      border-radius: 22px;
      margin-top: 28px;
      overflow: hidden;
      box-shadow: 0 16px 40px rgba(15, 23, 42, 0.15);
    }

    .terminal-header {
      background: rgba(30, 41, 59, 0.85);
      padding: 12px 18px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      border-bottom: 1px solid rgba(255, 255, 255, 0.08);
    }

    .terminal-dots { display: flex; gap: 7px; }
    .terminal-dots span { width: 11px; height: 11px; border-radius: 50%; }
    .dot-red { background: #ff5f56; }
    .dot-yellow { background: #ffbd2e; }
    .dot-green { background: #27c93f; }

    .terminal-body {
      font-family: var(--font-mono);
      font-size: 0.8rem;
      color: #cbd5e1;
      padding: 18px;
      max-height: 180px;
      overflow-y: auto;
      white-space: pre-wrap;
      line-height: 1.6;
    }

    /* iOS Sheet Modal for Authentication */
    .modal-overlay {
      position: fixed;
      inset: 0;
      background: rgba(15, 23, 42, 0.4);
      backdrop-filter: blur(16px);
      -webkit-backdrop-filter: blur(16px);
      display: none;
      align-items: center;
      justify-content: center;
      z-index: 1000;
      padding: 20px;
    }

    .modal-overlay.active {
      display: flex;
      animation: modalSpring 0.3s cubic-bezier(0.16, 1, 0.3, 1);
    }

    @keyframes modalSpring {
      from { opacity: 0; transform: scale(0.94); }
      to { opacity: 1; transform: scale(1); }
    }

    .modal-card {
      background: rgba(255, 255, 255, 0.92);
      backdrop-filter: blur(32px) saturate(200%);
      -webkit-backdrop-filter: blur(32px) saturate(200%);
      border: 1px solid var(--glass-border);
      border-radius: 28px;
      max-width: 480px;
      width: 100%;
      padding: 32px;
      box-shadow: 0 25px 60px rgba(15, 23, 42, 0.15), var(--glass-inset);
      position: relative;
    }

    .modal-close {
      position: absolute;
      top: 20px;
      right: 20px;
      background: rgba(0, 0, 0, 0.05);
      border: none;
      color: var(--text-muted);
      cursor: pointer;
      width: 30px;
      height: 30px;
      border-radius: 50%;
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 1.1rem;
      transition: all 0.2s ease;
    }

    .modal-close:hover { background: rgba(0, 0, 0, 0.1); color: var(--text-primary); }

    .auth-tab-bar {
      display: flex;
      background: rgba(0, 0, 0, 0.05);
      padding: 4px;
      border-radius: 14px;
      margin-bottom: 22px;
      gap: 4px;
    }

    .auth-tab-btn {
      flex: 1;
      background: none;
      border: none;
      color: var(--text-secondary);
      font-weight: 700;
      font-size: 0.88rem;
      padding: 8px 12px;
      border-radius: 11px;
      cursor: pointer;
      transition: all 0.2s ease;
    }

    .auth-tab-btn.active {
      background: #ffffff;
      color: var(--ios-blue);
      box-shadow: 0 3px 8px rgba(0,0,0,0.06);
    }

    .auth-view { display: none; }
    .auth-view.active { display: block; }

    /* Toast Notification */
    #toast {
      position: fixed;
      bottom: 28px;
      right: 28px;
      background: rgba(255, 255, 255, 0.95);
      backdrop-filter: blur(20px);
      border: 1px solid rgba(0, 0, 0, 0.08);
      color: var(--text-primary);
      padding: 14px 22px;
      border-radius: 18px;
      font-size: 0.88rem;
      font-weight: 600;
      box-shadow: 0 16px 40px rgba(0,0,0,0.12);
      display: none;
      align-items: center;
      gap: 10px;
      z-index: 2000;
      animation: toastSlide 0.3s cubic-bezier(0.16, 1, 0.3, 1);
    }

    @keyframes toastSlide {
      from { opacity: 0; transform: translateY(16px); }
      to { opacity: 1; transform: translateY(0); }
    }
  </style>
</head>
<body>
  <div class="container">
    
    <!-- 1. DYNAMIC ISLAND / LIVE ACTIVITY PILL -->
    <div class="dynamic-island-wrapper">
      <div class="dynamic-island" id="dynamic-island">
        <span class="island-dot"></span>
        <span id="island-text">📡 High-Speed TCP Socket Ready (:5000) &bull; 64 KB Buffer Stream</span>
      </div>
    </div>

    <!-- 2. iOS FROSTED GLASS HEADER -->
    <header>
      <div class="brand">
        <div class="brand-icon">
          <svg fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24">
            <path stroke-linecap="round" stroke-linejoin="round" d="M8 7h12m0 0l-4-4m4 4l-4 4m0 6H4m0 0l4 4m-4-4l4-4" />
          </svg>
        </div>
        <div class="brand-titles">
          <h1>Network File Transfer</h1>
          <p>Apple iOS Frosted Glass &bull; Email Login &bull; Unique ID Addressing</p>
        </div>
      </div>

      <div class="header-user-badge">
        <div class="user-avatar" id="header-avatar">A</div>
        <div>
          <div style="font-size:0.75rem; color:var(--text-secondary); margin-bottom:1px;" id="header-email">alice@network.edu</div>
          <div style="display:flex; align-items:center; gap:8px;">
            <span class="user-meta-name" id="header-username">alice</span>
            <span class="user-uid-pill" id="header-uid" title="Click to copy Unique ID" onclick="copyUid()" style="cursor:pointer;">UID-1001 📋</span>
          </div>
        </div>
        <button class="btn btn-secondary btn-sm" onclick="openAuthModal()" style="margin-left:8px;">
          Log In / Switch
        </button>
      </div>
    </header>

    <!-- 3. iOS WIDGETS GRID -->
    <div class="widgets-section-title">
      <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><rect x="3" y="3" width="7" height="7" rx="2"/><rect x="14" y="3" width="7" height="7" rx="2"/><rect x="14" y="14" width="7" height="7" rx="2"/><rect x="3" y="14" width="7" height="7" rx="2"/></svg>
      iOS System Widgets
    </div>

    <div class="widgets-grid">
      <!-- WIDGET 1: USER IDENTITY & UNIQUE ID -->
      <div class="widget-card">
        <div class="widget-header">
          <span class="widget-label">Your Unique ID</span>
          <div class="widget-icon-pill widget-icon-blue">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><rect x="3" y="11" width="18" height="11" rx="2" ry="2"/><path d="M7 11V7a5 5 0 0 1 10 0v4"/></svg>
          </div>
        </div>
        <div>
          <div class="widget-val-lg" id="widget-uid" style="color:var(--ios-blue); font-family:var(--font-mono); letter-spacing:-0.5px;">UID-1001</div>
          <div class="widget-subtext" style="margin-top:4px;">
            <span style="font-weight:600; color:var(--text-secondary);" id="widget-email">alice@network.edu</span>
          </div>
          <div style="font-size:0.75rem; color:var(--text-muted); margin-top:2px;">
            Username: <strong id="widget-username">alice</strong>
          </div>
        </div>
        <div style="margin-top:12px; display:flex; gap:6px;">
          <button class="btn btn-primary btn-sm" style="flex:1; font-size:0.75rem;" onclick="copyUid()">📋 Copy UID</button>
          <button class="btn btn-secondary btn-sm" style="flex:1; font-size:0.75rem;" onclick="openAuthModal()">Switch</button>
        </div>
      </div>

      <!-- WIDGET 2: NETWORK & TCP SOCKET HEALTH -->
      <div class="widget-card">
        <div class="widget-header">
          <span class="widget-label">TCP Socket Health</span>
          <div class="widget-icon-pill widget-icon-green">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M22 12h-4l-3 9L9 3l-3 9H2"/></svg>
          </div>
        </div>
        <div>
          <div class="widget-val-lg" style="color:var(--ios-green);">127.0.0.1:5000</div>
          <div class="widget-subtext">Multithreaded Server Online</div>
        </div>
        <div style="font-size:0.72rem; color:var(--text-muted); margin-top:12px;">
          Buffer: <strong>64 KB Chunks</strong> &bull; Latency: <strong>&lt; 1 ms</strong>
        </div>
      </div>

      <!-- WIDGET 3: MAILBOX & DELIVERY CONFIRMATION GAUGE -->
      <div class="widget-card">
        <div class="widget-header">
          <span class="widget-label">Mailbox &amp; Receipts</span>
          <div class="widget-icon-pill widget-icon-orange">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="20 6 9 17 4 12"/></svg>
          </div>
        </div>
        <div style="display:flex; justify-content:space-between; align-items:flex-end;">
          <div>
            <div class="widget-val-lg" id="widget-inbox-count">0</div>
            <div class="widget-subtext">Incoming in Inbox</div>
          </div>
          <div style="text-align:right;">
            <div class="widget-val-lg" id="widget-outbox-count" style="color:var(--ios-blue);">0</div>
            <div class="widget-subtext">Sent Outbox Files</div>
          </div>
        </div>
        <div style="font-size:0.72rem; color:var(--text-muted); margin-top:12px;">
          Delivery Confirmations: <strong style="color:var(--ios-green);" id="widget-ack-status">Active &amp; Tracked</strong>
        </div>
      </div>

      <!-- WIDGET 4: SPEED-DIAL CONTACTS WIDGET -->
      <div class="widget-card">
        <div class="widget-header">
          <span class="widget-label">Quick Contacts (Unique IDs)</span>
          <div class="widget-icon-pill widget-icon-purple">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>
          </div>
        </div>
        <div class="widget-subtext">Click to fill recipient's Unique ID:</div>
        <div class="widget-contacts-row" id="widget-speed-dial">
          <span class="widget-contact-chip" onclick="quickSelectRecipient('UID-1002')">👤 UID-1002 (Bob)</span>
          <span class="widget-contact-chip" onclick="quickSelectRecipient('UID-1001')">👤 UID-1001 (Alice)</span>
        </div>
        <div style="font-size:0.7rem; color:var(--text-muted); margin-top:12px;">
          Direct routing powered by permanent Unique IDs
        </div>
      </div>
    </div>

    <!-- 4. iOS SEGMENTED CONTROL TABS -->
    <div class="segmented-control-container">
      <div class="segmented-control">
        <button class="segment-btn active" onclick="switchTab('two-way')">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M16 3h5v5M4 20L21 3M21 16v5h-5M15 15l6 6M4 4l5 5"/></svg>
          Direct Two-Way Transfer
        </button>
        <button class="segment-btn" onclick="switchTab('inbox')">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 12h-6l-2 3h-4l-2-3H2v7a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-7z"/></svg>
          My Inbox
          <span class="badge-count" id="inbox-badge">0</span>
        </button>
        <button class="segment-btn" onclick="switchTab('outbox')">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="20 6 9 17 4 12"/></svg>
          Sent Outbox &amp; ACKs
          <span class="badge-count" id="outbox-badge">0</span>
        </button>
        <button class="segment-btn" onclick="switchTab('public')">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/></svg>
          Public Repository
        </button>
      </div>
    </div>

    <!-- ========================================================
         TAB 1: TWO-WAY DIRECT TRANSFER
         ======================================================== -->
    <div class="tab-content active" id="tab-two-way">
      <div class="grid-two">
        <div class="card">
          <div class="card-header">
            <h2>
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg>
              Transmit File by Unique ID
            </h2>
          </div>

          <div style="background:var(--ios-blue-light); border:1px solid rgba(0, 122, 255, 0.2); border-radius:14px; padding:12px 16px; margin-bottom:18px; font-size:0.85rem;">
            Sending from: <strong style="color:var(--ios-blue);" id="sender-display-email">alice@network.edu</strong> (<span id="sender-display-name">alice</span>) &bull; Your Unique ID: <span class="user-uid-pill" id="sender-display-uid" onclick="copyUid()" title="Click to copy UID" style="cursor:pointer;">UID-1001 📋</span>
          </div>

          <form id="direct-send-form" onsubmit="handleDirectSend(event)">
            <div class="form-group">
              <label>Target Recipient Unique ID <span style="font-size:0.75rem; font-weight:400; color:var(--ios-blue);">(Required for delivery)</span></label>
              <input type="text" class="input-control" id="recipient-input" placeholder="Enter recipient Unique ID (e.g. UID-1002)" required>
              <div style="font-size:0.75rem; color:var(--text-muted); margin-top:8px;">
                Directory (Click to auto-fill recipient Unique ID):
              </div>
              <div class="recipient-pills" id="directory-pills">
                <!-- Injected dynamically -->
              </div>
            </div>

            <div class="form-group">
              <label>Select File to Transmit</label>
              <div class="file-drop-area" id="direct-drop-zone">
                <input type="file" id="direct-file-input" required onchange="onDirectFileSelect()">
                <div style="color:var(--ios-blue); margin-bottom:10px;">
                  <svg width="36" height="36" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" y1="3" x2="12" y2="15"/></svg>
                </div>
                <h4 id="direct-file-label" style="font-size:0.95rem; font-weight:700;">Click or Drag &amp; Drop file here</h4>
                <p style="font-size:0.75rem; color:var(--text-muted); margin-top:4px;">Streamed over multithreaded TCP socket in 64 KB binary buffers</p>
              </div>
            </div>

            <button type="submit" class="btn btn-primary" id="btn-direct-send" style="width:100%;">
              Send File &amp; Receive Delivery ACK
            </button>
          </form>
        </div>

        <!-- Architecture & Unique ID Transfer Guide Card -->
        <div class="card">
          <div class="card-header">
            <h2>
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>
              Email Login &amp; Unique ID Routing
            </h2>
          </div>

          <div style="font-size:0.85rem; line-height:1.75; color:var(--text-secondary);">
            <p style="margin-bottom:14px;">
              <strong style="color:var(--text-primary);">1. Email Registration &amp; Auto Unique ID:</strong> When creating an account with your email, the server immediately generates a permanent, sequential <strong style="color:var(--ios-blue);">Unique ID (e.g. UID-1002)</strong>.
            </p>
            <p style="margin-bottom:14px;">
              <strong style="color:var(--text-primary);">2. Addressing Files by Unique ID:</strong> Enter the recipient's Unique ID in the transfer box. The TCP server routes the packet directly to the recipient's secure inbox.
            </p>
            <p style="margin-bottom:14px;">
              <strong style="color:#b45309;">3. Level 1 Receipt (Server Mailbox ACK):</strong> The sender receives an immediate confirmation that the file reached the server: <span class="status-badge status-pending">[PENDING: Waiting for recipient]</span>.
            </p>
            <p style="margin-bottom:14px;">
              <strong style="color:#15803d;">4. Level 2 Receipt (Download Confirmed):</strong> When the recipient downloads the file, the server records the exact timestamp and confirms delivery to the sender: <span class="status-badge status-confirmed">[DELIVERED &amp; CONFIRMED]</span>.
            </p>
            <div style="background:rgba(255,255,255,0.7); border:1px solid rgba(0,0,0,0.06); padding:12px; border-radius:14px; margin-top:8px;">
              <span style="font-size:0.75rem; font-weight:700; color:var(--text-muted); text-transform:uppercase;">Pre-Seeded Demo Accounts:</span>
              <div style="margin-top:6px; font-size:0.8rem; display:flex; flex-direction:column; gap:4px;">
                <div>👤 <strong>alice@network.edu</strong> &bull; Unique ID: <code style="color:var(--ios-blue); font-weight:700;">UID-1001</code> &bull; Pass: <code>password123</code></div>
                <div>👤 <strong>bob@network.edu</strong> &bull; Unique ID: <code style="color:var(--ios-blue); font-weight:700;">UID-1002</code> &bull; Pass: <code>password123</code></div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- ========================================================
         TAB 2: INBOX
         ======================================================== -->
    <div class="tab-content" id="tab-inbox">
      <div class="card">
        <div class="card-header">
          <h2>
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M22 12h-6l-2 3h-4l-2-3H2v7a2 2 0 0 0 2 2h16a2 2 0 0 0 2-2v-7z"/></svg>
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
                <th>Sender Unique ID</th>
                <th>Sender (Email / Username)</th>
                <th>Filename</th>
                <th>Size</th>
                <th>Sent At</th>
                <th>Status</th>
                <th style="text-align: right;">Action</th>
              </tr>
            </thead>
            <tbody id="inbox-tbody">
              <tr><td colspan="7" style="text-align:center; color:var(--text-faint); padding:36px;">Loading inbox...</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- ========================================================
         TAB 3: OUTBOX & DELIVERY RECEIPTS
         ======================================================== -->
    <div class="tab-content" id="tab-outbox">
      <div class="card">
        <div class="card-header">
          <h2>
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="20 6 9 17 4 12"/></svg>
            Sent Files &amp; Live Delivery Confirmation Tracker
          </h2>
          <button class="btn btn-secondary btn-sm" onclick="fetchOutbox()">
            Refresh Outbox
          </button>
        </div>

        <div class="table-container">
          <table>
            <thead>
              <tr>
                <th>Recipient Unique ID</th>
                <th>Recipient (Email / Username)</th>
                <th>Filename</th>
                <th>Size</th>
                <th>Sent Timestamp</th>
                <th>Delivery Confirmation Status</th>
              </tr>
            </thead>
            <tbody id="outbox-tbody">
              <tr><td colspan="6" style="text-align:center; color:var(--text-faint); padding:36px;">Loading outbox...</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- ========================================================
         TAB 4: PUBLIC REPOSITORY
         ======================================================== -->
    <div class="tab-content" id="tab-public">
      <div class="card">
        <div class="card-header">
          <h2>
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/></svg>
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
              <tr><td colspan="5" style="text-align:center; color:var(--text-faint); padding:36px;">Loading public files...</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- ========================================================
         LIVE TCP SOCKET LOG & STREAM CONSOLE
         ======================================================== -->
    <div class="terminal-card">
      <div class="terminal-header">
        <div class="terminal-dots">
          <span class="dot-red"></span>
          <span class="dot-yellow"></span>
          <span class="dot-green"></span>
        </div>
        <div style="font-size:0.75rem; color:#94a3b8; font-weight:600;">Live TCP Socket Stream &amp; Packet Framing Console</div>
        <div style="font-size:0.72rem; color:var(--ios-green); font-weight:700;">● Active Stream</div>
      </div>
      <div class="terminal-body" id="terminal-body">Connecting to live server log stream...</div>
    </div>

  </div>

  <!-- ========================================================
       iOS FROSTED SHEET MODAL (AUTHENTICATION)
       ======================================================== -->
  <!-- ========================================================
       iOS FROSTED SHEET MODAL (AUTHENTICATION)
       ======================================================== -->
  <div class="modal-overlay" id="auth-modal">
    <div class="modal-card">
      <button class="modal-close" onclick="closeAuthModal()">&times;</button>
      
      <div class="auth-tab-bar">
        <button class="auth-tab-btn active" id="tab-auth-login" onclick="switchAuthTab('login')">Log In with Email</button>
        <button class="auth-tab-btn" id="tab-auth-register" onclick="switchAuthTab('register')">Create Account (Get UID)</button>
      </div>

      <!-- VIEW: LOGIN -->
      <div class="auth-view active" id="view-auth-login">
        <form onsubmit="handleAuthLogin(event)">
          <div class="form-group">
            <label>Email Address or Unique ID</label>
            <input type="text" class="input-control" id="login-identifier" placeholder="e.g. alice@network.edu or UID-1001" required>
          </div>
          <div class="form-group">
            <label>Password</label>
            <input type="password" class="input-control" id="login-password" placeholder="e.g. password123" required>
          </div>
          <button type="submit" class="btn btn-primary" style="width:100%; margin-top:8px;">
            Log In to Account
          </button>
        </form>

        <div style="margin-top:22px; padding-top:16px; border-top:1px solid rgba(0,0,0,0.08);">
          <div style="font-size:0.75rem; font-weight:700; color:var(--text-muted); margin-bottom:8px;">Instant Demo Logins:</div>
          <div style="display:flex; flex-direction:column; gap:8px;">
            <button class="btn btn-secondary btn-sm" style="text-align:left; justify-content:space-between; display:flex;" onclick="quickLoginDemo('alice@network.edu', 'password123'); closeAuthModal();">
              <span>👤 <strong>alice@network.edu</strong> (Alice)</span>
              <span class="user-uid-pill">UID-1001</span>
            </button>
            <button class="btn btn-secondary btn-sm" style="text-align:left; justify-content:space-between; display:flex;" onclick="quickLoginDemo('bob@network.edu', 'password123'); closeAuthModal();">
              <span>👤 <strong>bob@network.edu</strong> (Bob)</span>
              <span class="user-uid-pill">UID-1002</span>
            </button>
          </div>
        </div>
      </div>

      <!-- VIEW: REGISTER -->
      <div class="auth-view" id="view-auth-register">
        <div style="background:var(--ios-blue-light); border:1px solid rgba(0,122,255,0.2); border-radius:12px; padding:12px; margin-bottom:16px; font-size:0.8rem; line-height:1.5;">
          🔑 <strong>Automatic Unique ID Generation:</strong><br>
          Register with your email to automatically generate a permanent <strong style="color:var(--ios-blue);">Unique ID (e.g. UID-1003)</strong>. <strong>This Unique ID is used for sending and receiving files!</strong>
        </div>
        <form onsubmit="handleAuthRegister(event)">
          <div class="form-group">
            <label>Email Address <span style="color:var(--ios-red);">*</span></label>
            <input type="email" class="input-control" id="reg-email" placeholder="e.g. charlie@network.edu" required>
          </div>
          <div class="form-group">
            <label>Display Username (Optional)</label>
            <input type="text" class="input-control" id="reg-username" placeholder="e.g. charlie (defaults to email prefix)">
          </div>
          <div class="form-group">
            <label>Password <span style="color:var(--ios-red);">*</span></label>
            <input type="password" class="input-control" id="reg-password" placeholder="At least 4 characters" required>
          </div>
          <button type="submit" class="btn btn-primary" style="width:100%; margin-top:8px;">
            Create Account &amp; Generate Unique ID
          </button>
        </form>
      </div>
    </div>
  </div>

  <div id="toast">Notification</div>

  <script>
    // Current authenticated user state
    let currentUser = {
      email: 'alice@network.edu',
      username: 'alice',
      user_id: 'UID-1001'
    };

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
      toast.style.borderColor = isError ? 'var(--ios-red)' : 'var(--ios-green)';
      toast.style.color = isError ? 'var(--ios-red)' : 'var(--text-primary)';
      toast.style.display = 'flex';
      setTimeout(() => { toast.style.display = 'none'; }, 4000);
    }

    function copyUid() {
      navigator.clipboard.writeText(currentUser.user_id).then(() => {
        showToast(`Copied Unique ID: ${currentUser.user_id} to clipboard!`);
      });
    }

    function updateActiveUserUI() {
      // Header
      const displayName = currentUser.username || currentUser.email;
      document.getElementById('header-avatar').innerText = displayName.charAt(0).toUpperCase();
      document.getElementById('header-username').innerText = currentUser.username;
      document.getElementById('header-email').innerText = currentUser.email;
      document.getElementById('header-uid').innerText = currentUser.user_id + ' 📋';

      // Widgets
      document.getElementById('widget-uid').innerText = currentUser.user_id;
      document.getElementById('widget-email').innerText = currentUser.email;
      document.getElementById('widget-username').innerText = currentUser.username;

      // Transfer card
      document.getElementById('sender-display-name').innerText = currentUser.username;
      document.getElementById('sender-display-email').innerText = currentUser.email;
      document.getElementById('sender-display-uid').innerText = currentUser.user_id + ' 📋';

      // Dynamic Island text
      document.getElementById('island-text').innerText = `📡 Logged in as ${currentUser.email} • Unique ID: ${currentUser.user_id} • Ready for Transfers`;

      fetchDirectoryUsers();
      fetchInbox();
      fetchOutbox();
    }

    async function quickLoginDemo(identifier, password) {
      try {
        const res = await fetch('/api/login', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ identifier: identifier, password: password })
        });
        const data = await res.json();
        if (res.ok && data.status === 'OK') {
          currentUser = data.user;
          showToast(`Logged in as '${currentUser.email}' (Unique ID: ${currentUser.user_id})`);
          updateActiveUserUI();
        } else {
          showToast(data.message || 'Login failed', true);
        }
      } catch (err) {
        showToast(`Login error: ${err}`, true);
      }
    }

    function quickSelectRecipient(recipientUid) {
      document.getElementById('recipient-input').value = recipientUid;
      showToast(`Selected recipient Unique ID: '${recipientUid}'!`);
    }

    function openAuthModal(defaultTab = 'login') {
      document.getElementById('auth-modal').classList.add('active');
      switchAuthTab(defaultTab);
    }

    function closeAuthModal() {
      document.getElementById('auth-modal').classList.remove('active');
    }

    function switchAuthTab(tab) {
      document.getElementById('tab-auth-login').classList.toggle('active', tab === 'login');
      document.getElementById('tab-auth-register').classList.toggle('active', tab === 'register');
      document.getElementById('view-auth-login').classList.toggle('active', tab === 'login');
      document.getElementById('view-auth-register').classList.toggle('active', tab === 'register');
    }

    async function handleAuthLogin(e) {
      e.preventDefault();
      const identifier = document.getElementById('login-identifier').value.trim();
      const password = document.getElementById('login-password').value.trim();

      try {
        const res = await fetch('/api/login', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ identifier, password })
        });
        const data = await res.json();
        if (res.ok && data.status === 'OK') {
          currentUser = data.user;
          showToast(`Welcome! Logged in as ${currentUser.email} (${currentUser.user_id})!`);
          updateActiveUserUI();
          closeAuthModal();
        } else {
          showToast(data.message || 'Invalid credentials', true);
        }
      } catch (err) {
        showToast(`Login error: ${err}`, true);
      }
    }

    async function handleAuthRegister(e) {
      e.preventDefault();
      const email = document.getElementById('reg-email').value.trim();
      const username = document.getElementById('reg-username').value.trim();
      const password = document.getElementById('reg-password').value.trim();

      try {
        const res = await fetch('/api/register', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ email, username, password })
        });
        const data = await res.json();
        if (res.ok && data.status === 'OK') {
          currentUser = data.user;
          showToast(`Account created! Assigned Unique ID: ${currentUser.user_id}`);
          alert(`🎉 Account Successfully Created!\n\nEmail: ${currentUser.email}\nYour UNIQUE ID: ${currentUser.user_id}\n\nNotice: This UNIQUE ID is what other users will use to send files directly to you!`);
          updateActiveUserUI();
          closeAuthModal();
        } else {
          showToast(data.message || 'Registration failed', true);
        }
      } catch (err) {
        showToast(`Registration error: ${err}`, true);
      }
    }

    async function fetchDirectoryUsers() {
      try {
        const res = await fetch('/api/users');
        const data = await res.json();
        const pillsContainer = document.getElementById('directory-pills');
        const speedDialContainer = document.getElementById('widget-speed-dial');
        pillsContainer.innerHTML = '';
        speedDialContainer.innerHTML = '';

        if (!data.users || data.users.length === 0) {
          pillsContainer.innerHTML = '<span style="font-size:0.75rem; color:var(--text-faint);">No directory users</span>';
          return;
        }

        data.users.forEach(u => {
          if (u.user_id === currentUser.user_id || u.username.toLowerCase() === currentUser.username.toLowerCase()) return;
          
          // Form suggestion pill - shows UID as primary and user/email as details
          const btn = document.createElement('button');
          btn.type = 'button';
          btn.className = 'pill-btn';
          btn.innerHTML = `👤 <strong style="color:var(--ios-blue);">${u.user_id}</strong> <span class="pill-uid">${u.username} (${u.email})</span>`;
          btn.onclick = () => {
            document.getElementById('recipient-input').value = u.user_id;
            showToast(`Recipient set to Unique ID: '${u.user_id}' (${u.username})`);
          };
          pillsContainer.appendChild(btn);

          // Widget Speed-Dial chip
          const chip = document.createElement('span');
          chip.className = 'widget-contact-chip';
          chip.innerHTML = `👤 <strong>${u.user_id}</strong> (${u.username})`;
          chip.onclick = () => {
            document.getElementById('recipient-input').value = u.user_id;
            switchTab('two-way');
            showToast(`Target set to Unique ID: '${u.user_id}'!`);
          };
          speedDialContainer.appendChild(chip);
        });
      } catch (err) {}
    }

    function switchTab(tabId) {
      document.querySelectorAll('.segment-btn').forEach(b => b.classList.remove('active'));
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
      const sender = currentUser.user_id;
      const recipient = document.getElementById('recipient-input').value.trim();
      const fileInput = document.getElementById('direct-file-input');
      const btn = document.getElementById('btn-direct-send');

      if (!recipient) {
        showToast('Please enter a recipient Unique ID.', true);
        return;
      }
      if (fileInput.files.length === 0) {
        showToast('Please select a file to send.', true);
        return;
      }

      const file = fileInput.files[0];
      btn.disabled = true;
      btn.innerText = `Transmitting '${file.name}' to Unique ID ${recipient}...`;

      // Update island
      document.getElementById('island-text').innerText = `📡 Transmitting '${file.name}' (${formatBytes(file.size)}) to Unique ID ${recipient}...`;

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
          showToast(`File sent! Queued on server for Unique ID '${recipient}'. Level 1 Receipt recorded!`);
          document.getElementById('direct-send-form').reset();
          document.getElementById('direct-file-label').innerText = 'Click or Drag & Drop file here';
          document.getElementById('island-text').innerText = `✓ '${file.name}' Queued on Server for Unique ID ${recipient} &bull; Delivery Pending`;
          fetchOutbox();
          fetchLogs();
        } else {
          showToast(`Transfer failed: ${data.message || 'Error'}`, true);
          document.getElementById('island-text').innerText = `[-] Transfer Failed: ${data.message || 'Error'}`;
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
      const user = currentUser.user_id;
      try {
        const res = await fetch(`/api/inbox?user=${encodeURIComponent(user)}`);
        const data = await res.json();
        const tbody = document.getElementById('inbox-tbody');
        tbody.innerHTML = '';

        const inbox = data.inbox || [];
        document.getElementById('inbox-badge').innerText = inbox.length;
        document.getElementById('widget-inbox-count').innerText = inbox.length;

        if (inbox.length === 0) {
          tbody.innerHTML = `<tr><td colspan="7" style="text-align:center; color:var(--text-muted); padding:36px;">No incoming files for '${currentUser.email}' (${currentUser.user_id})</td></tr>`;
          return;
        }

        inbox.forEach(item => {
          const tr = document.createElement('tr');
          const isPending = item.status === 'PENDING_RECEIPT';
          const badgeClass = isPending ? 'status-pending' : 'status-confirmed';
          const badgeText = isPending ? 'NEW / WAITING' : 'DOWNLOADED';

          tr.innerHTML = `
            <td>
              <span class="user-uid-pill" style="font-weight:700; font-size:0.8rem;">${item.sender_uid || 'UID-????'}</span>
            </td>
            <td>
              <strong style="color:var(--text-primary); font-size:0.88rem;">${item.sender}</strong>
            </td>
            <td><strong>${item.filename}</strong></td>
            <td style="font-family:var(--font-mono); font-size:0.8rem;">${formatBytes(item.size)}</td>
            <td style="font-size:0.75rem; color:var(--text-muted);">${item.sent_at}</td>
            <td><span class="status-badge ${badgeClass}">${badgeText}</span></td>
            <td style="text-align:right;">
              <a href="/api/receive-inbox?transfer_id=${item.transfer_id}&user=${encodeURIComponent(user)}" class="btn btn-emerald btn-sm" download onclick="setTimeout(fetchInbox, 1000)">
                Receive &amp; Confirm
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
      const user = currentUser.user_id;
      try {
        const res = await fetch(`/api/outbox?user=${encodeURIComponent(user)}`);
        const data = await res.json();
        const tbody = document.getElementById('outbox-tbody');
        tbody.innerHTML = '';

        const outbox = data.outbox || [];
        document.getElementById('outbox-badge').innerText = outbox.length;
        document.getElementById('widget-outbox-count').innerText = outbox.length;

        if (outbox.length === 0) {
          tbody.innerHTML = `<tr><td colspan="6" style="text-align:center; color:var(--text-muted); padding:36px;">No sent files from '${currentUser.email}' (${currentUser.user_id})</td></tr>`;
          return;
        }

        outbox.forEach(item => {
          const tr = document.createElement('tr');
          const isConfirmed = item.status === 'RECEIVED_BY_USER';
          const badgeClass = isConfirmed ? 'status-confirmed' : 'status-pending';
          const badgeText = isConfirmed 
            ? `DELIVERED & CONFIRMED (Downloaded at ${item.received_at})` 
            : `WAITING FOR RECIPIENT (${item.recipient_uid || item.recipient})`;

          tr.innerHTML = `
            <td>
              <span class="user-uid-pill" style="font-weight:700; font-size:0.8rem;">${item.recipient_uid || 'UID-????'}</span>
            </td>
            <td>
              <strong style="color:var(--text-primary); font-size:0.88rem;">${item.recipient}</strong>
            </td>
            <td><strong>${item.filename}</strong></td>
            <td style="font-family:var(--font-mono); font-size:0.8rem;">${formatBytes(item.size)}</td>
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
          tbody.innerHTML = `<tr><td colspan="5" style="text-align:center; color:var(--text-muted); padding:36px;">No files currently in public storage.</td></tr>`;
          return;
        }

        data.files.forEach(f => {
          const tr = document.createElement('tr');
          tr.innerHTML = `
            <td><strong>${f.name}</strong></td>
            <td style="font-family:var(--font-mono); font-size:0.8rem;">${formatBytes(f.size)}</td>
            <td style="font-size:0.8rem; color:var(--text-secondary); text-transform:uppercase;">${f.type || 'file'}</td>
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
    updateActiveUserUI();
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

        elif path == "/api/users":
            users = user_manager.list_users()
            self._send_json({"status": "OK", "users": users})
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
        """Handle POST requests for authentication, direct two-way transfers, and uploads."""
        parsed_url = urlparse(self.path)
        path = parsed_url.path

        if path == "/api/login":
            try:
                content_length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(content_length).decode("utf-8")
                data = json.loads(body)
            except Exception:
                self._send_json({"status": "ERROR", "message": "Invalid JSON payload"}, status=400)
                return

            identifier = data.get("identifier", "").strip()
            password = data.get("password", "").strip()
            if not identifier or not password:
                self._send_json(
                    {"status": "ERROR", "message": "Email/Unique ID and password are required."},
                    status=400,
                )
                return

            user_rec = user_manager.authenticate(identifier, password)
            if not user_rec:
                self._send_json(
                    {"status": "ERROR", "message": "Invalid Email/Unique ID or incorrect password."},
                    status=401,
                )
                return

            self._send_json({"status": "OK", "user": user_rec})
            return

        elif path == "/api/register":
            try:
                content_length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(content_length).decode("utf-8")
                data = json.loads(body)
            except Exception:
                self._send_json({"status": "ERROR", "message": "Invalid JSON payload"}, status=400)
                return

            email = data.get("email", "").strip()
            username = data.get("username", "").strip() or None
            password = data.get("password", "").strip()

            # Fallback if email was provided in username field
            if not email and username and "@" in username:
                email = username
                username = None

            if not email or not password:
                self._send_json(
                    {"status": "ERROR", "message": "Email address and password are required."},
                    status=400,
                )
                return

            try:
                user_rec = user_manager.register(email=email, password=password, username=username)
                self._send_json({"status": "OK", "user": user_rec})
            except ValueError as err:
                self._send_json({"status": "ERROR", "message": str(err)}, status=400)
            return

        elif path == "/api/send-to":
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
                    sender_rec = user_manager.find_user(sender)
                    if sender_rec:
                        client.username = sender_rec["username"]
                        client.user_id = sender_rec["user_id"]
                    else:
                        client.username = sender

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
                            {"status": "ERROR", "message": f"Failed to send file over TCP to '{recipient}'."},
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
