# MediaInfo Bot

A Telegram bot that extracts detailed media information (resolution, codec, bit
depth, HDR, duration, audio languages, subtitles) from videos and documents,
and rewrites their captions automatically in authorized channels.

## Features

- Streams only the first few MB of a file to probe metadata with `mediainfo` and `ffprobe`
- **Datacenter-aware ranged reader** (`dc_stream.py`): one long-lived media session per
  datacenter, byte-range reads, per-DC flood tracking and automatic recovery from
  `AUTH_BYTES_INVALID`, `FILE_REFERENCE_EXPIRED` and `FILE_MIGRATE`
- **Tail probing** for containers that store their index at the end of the file
  (fragmented MP4, some MKV muxers)
- Smart fallback that parses the release name (resolution, codec, bit depth, HDR,
  languages, ESUB) when a deep scan is impossible
- Full MediaInfo report behind an inline button on `/info`
- Dynamic per-chat authorization stored in MongoDB, plus a persistent retry queue
- `/scan` backfills captions for older messages
- aiohttp health check plus a `/status` JSON endpoint for dashboards

## How probing works

1. A small head range is fetched (32 KB) and grown step by step (1 MB, 4 MB, 10 MB).
2. After every step `mediainfo` and `ffprobe` run against the partial file; as soon
   as both dimensions and a duration are known the scan stops.
3. If the header never yields metadata, the last 1 MB of the file is read too.
4. Only when every step fails does the bot fall back to Telegram's own metadata,
   enriched with information parsed from the file name.

Ranges are 4 KiB aligned and never cross a 1 MiB boundary, as Telegram's
`upload.GetFile` requires.

## Health check

`GET /` returns `200 OK` on the port given by `PORT` (default `8080`) and is used
by the Docker `HEALTHCHECK`. `GET /status` returns a JSON summary (uptime, queue
depth, deep scans vs fallbacks, flood waits, active media sessions, whether
`mediainfo`/`ffprobe` are installed and whether MongoDB is reachable).

## Commands

| Command | Who | Description |
| --- | --- | --- |
| `/start`, `/help` | everyone | About the bot and how it works |
| `/info` | everyone | Media info for a replied or attached file |
| `/add`, `/remove`, `/chats` | admin | Manage authorized chats (id, @username, or the current chat) |
| `/scan [chat] [limit]` | admin | Queue older messages that lack a media info caption |
| `/stats` | admin | Totals plus deep-scan vs fallback counters |
| `/queue`, `/clearqueue`, `/retrynow` | admin | Inspect and control the queues |
| `/setdelay`, `/template` | admin | Caption pacing and layout |
| `/logs [n]`, `/ping`, `/server` | admin | Diagnostics |
| `/broadcast`, `/restart`, `/shutdown`, `/update` | admin | Maintenance |

## Configuration

All configuration is read from environment variables (see `config.py`). Copy
`.env.example` to `.env` and fill in your values.

| Variable | Required | Description |
| --- | --- | --- |
| `API_ID` | yes | Telegram API ID |
| `API_HASH` | yes | Telegram API hash |
| `BOT_TOKEN` | yes | Bot token from @BotFather |
| `ADMIN_ID` | yes | Your Telegram user ID |
| `ALLOWED_CHATS` | no | Comma/space separated chat IDs (merged with the database) |
| `MONGO_URI` | yes | MongoDB connection string |
| `PORT` | no | Health check port (default 8080) |
| `LOG_LEVEL` | no | Logging level (default INFO) |
| `CAPTION_TEMPLATE` | no | Caption format string |
| `PROBE_TIMEOUT` | no | Seconds per mediainfo/ffprobe run (default 15) |
| `STREAM_TIMEOUT` | no | Seconds per streaming step (default 90) |
| `STREAM_RETRIES` | no | Retries per streaming step (default 3) |
| `STREAM_CONCURRENCY` | no | Parallel media streams (default 2) |
| `CHANNEL_CONCURRENCY` | no | Channels processed in parallel (default 2) |
| `ENABLE_TAIL_PROBE` | no | Read the file tail when the header is not enough (default true) |
| `TMP_DIR` | no | Directory for temporary probe chunks (default `downloads`) |
| `SESSION_DIR` | no | Directory for the pyrogram session file (default `.`) |
| `EDIT_DELAY` | no | Seconds between caption edits per channel (default 3.5) |
| `FULL_REPORT` | no | Attach the "Full MediaInfo" button (default true) |

Caption placeholders: `{title}` `{video_line}` `{duration}` `{audio}`
`{subtitle}` `{size}` `{bitrate}` `{fps}` `{resolution}` `{codec}`
`{container}` `{filename}`.

The bot needs admin rights with **Edit messages** in every channel it captions.

## Run with Docker Compose

```bash
cp .env.example .env   # then edit .env
docker compose up -d --build
```

This starts the bot together with a MongoDB instance and wires up health checks
for both services.

## Run with Docker only

```bash
docker build -t mediainfo-bot .
docker run -d --env-file .env -p 8080:8080 mediainfo-bot
```

## Run locally

```bash
pip install -r requirements.txt
# ffmpeg and mediainfo must be installed on the host
export $(grep -v '^#' .env | xargs)
python main.py
```

## Tests

```bash
pip install -r requirements-dev.txt
pytest -q
```

The suite covers the mediainfo/ffprobe parsers, caption building, filename
heuristics and the range alignment of the datacenter reader. No network access
or Telegram credentials are needed.

