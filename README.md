# MediaInfo Bot (WZGram Edition)

A Telegram bot powered by **[WZGram](https://github.com/rjriajul/wzgram)** (`WarpCrypto` + `uvloop` + `MongoStorage`) that extracts detailed media information (resolution, codec, bit depth, HDR/Dolby Vision, duration, audio codecs/channels/languages, subtitles) from videos, audio files, and documents, and rewrites captions automatically in authorized channels.

## Features

- **Powered by WZGram (`wzgram[fast,mongo]`)**:
  - Uses Rust-backed `warpcrypto` and `uvloop` for high-speed MTProto partial media streaming
  - Stores the bot's MTProto session, peers, and authorization state directly in MongoDB via `wzgram.storage.MongoStorage` (`USE_MONGO_SESSION=true`) in an isolated session database (`<DB_NAME>_session`)
- **Bot-Compatible Channel Backfill Scanner (`/scan` & Inline Inspector)**:
  - Works natively on Telegram Bot accounts using batched `channels.GetMessages` (`app.get_messages` in 200-ID chunks) instead of `messages.GetHistory` (which Telegram blocks for bots with `400 BOT_METHOD_INVALID`)
  - Auto-detects the channel's latest message ID via silent probe or zero-post exponential/binary search from `last_processed_id`
  - Supports 1-tap `🆕 Scan New Since #ID`, preset depths (`25`–`1000` messages), `🎯 Scan by Post Link / Forward / ID Range`, and running `/scan [limit]` directly inside a channel
- **Organized Telegram UI & Control Center (`/panel`, `/admin`, `/settings`, `/config`)**:
  - **Control Center Dashboard**: Clean `<blockquote>` status cards with tree-aligned metrics (`├`, `└`) and a symmetric 5-row navigation grid
  - **3-Tab Settings & MongoDB Config Hub**: Consolidated **⚡ Probing**, **⏱ Pacing**, and **🗄 System** tabs with inline steppers, 1-click JSON export, `key = value` editor, and `.env` reset
  - **Channel Directory & Per-Channel Inspector**: Paginated channel list where tapping any channel opens its dedicated Inspector view
  - **Caption Studio**: Live sample preview, active preset indicator (`• ... •`), and 1-tap switching across `Default`, `Subtitles`, `Detailed`, and `Minimal` layouts
  - **Cross-Linked Server & Live Logs**: 1-tap tab bar switching between **🖥 System Health** and **📜 Live Logs**
- **Datacenter-aware ranged reader** (`dc_stream.py`):
  - Reuses one long-lived media session per datacenter with per-DC locks
  - Strictly enforces 4 KiB-aligned power-of-2 `upload.GetFile` limits (`1048576 % limit == 0` and `offset % limit == 0`) so multi-step probes never trigger `400 LIMIT_INVALID`
- **Optimized Multi-Stage Docker Image**:
  - Compiles C extensions in a dedicated `builder` stage and copies a stripped `/opt/venv` to a clean `python:3.11-slim-bookworm` runtime running as unprivileged `appuser` (`UID 1000`)

## Commands

| Command | Who | Description |
| --- | --- | --- |
| `/start`, `/help` | everyone | 3-tab interactive menu (`Home`, `Guide`, `Formats`) |
| `/info` | everyone | Media info for a replied or attached file |
| `/panel`, `/admin`, `/settings` | admin | Open the main Control Center dashboard |
| `/config` | admin | Open the 3-tab Settings & MongoDB Config Hub |
| `/add`, `/remove`, `/chats` | admin | Manage authorized channels & open the Channel Inspector |
| `/scan [chat/link] [limit] [max_id]` | admin | Backfill older channel posts that lack a media info caption |
| `/stats` | admin | Processing analytics (all-time & current session) |
| `/queue`, `/clearqueue`, `/retrynow` | admin | Inspect and control in-memory & MongoDB retry queues |
| `/setdelay`, `/template` | admin | Caption pacing tuner and Caption Studio |
| `/logs [n]`, `/ping`, `/server` | admin | Cross-linked System Health & Live Log Viewer |
| `/broadcast`, `/restart`, `/shutdown`, `/update` | admin | Maintenance & two-step confirmed power actions |

## Run with Docker Compose

```bash
cp .env.example .env   # then edit .env
docker compose up -d --build
```