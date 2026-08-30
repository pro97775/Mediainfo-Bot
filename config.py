import os
import logging


def _get_int(name: str, default: int = 0) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _get_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _get_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on", "y")


def _get_chat_ids(name: str) -> list[int]:
    """Parse a comma/space separated list of chat IDs from an env var."""
    raw = os.environ.get(name, "")
    ids: list[int] = []
    for token in raw.replace(",", " ").split():
        try:
            ids.append(int(token))
        except ValueError:
            continue
    return ids


# --- Telegram API credentials (required) ---
API_ID = _get_int("API_ID")
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

# --- Access control ---
ADMIN_ID = _get_int("ADMIN_ID")
ALLOWED_CHATS = _get_chat_ids("ALLOWED_CHATS")

# --- Database ---
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")

# --- Logging ---
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
if not isinstance(logging.getLevelName(LOG_LEVEL), int):
    LOG_LEVEL = "INFO"
LOG_FORMAT = os.environ.get(
    "LOG_FORMAT",
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

# --- Garbage collection thresholds (gen0, gen1, gen2) ---
GC_THRESHOLD = (
    _get_int("GC_GEN0", 700),
    _get_int("GC_GEN1", 10),
    _get_int("GC_GEN2", 10),
)

# --- Caption template ---
# Available placeholders: {title} {video_line} {duration} {audio} {subtitle}
# plus: {size} {bitrate} {fps} {resolution} {codec} {container} {filename}
CAPTION_TEMPLATE = os.environ.get(
 #   "CAPTION_TEMPLATE",
  #  "<b>\U0001F3AC {title}</b>\n\n"
  #  "\U0001F39E <b>:</b> {video_line} | "
  #  "\u23f3 <b>:</b> {duration}\n"
  #  "\U0001F50A <b>:</b> {audio}\n"
#    "\U0001F4AC <b>Subtitles:</b> {subtitle}",
    "CAPTION_TEMPLATE",
    "<b>{title}</b>\n\n"
    "🎬 <b>{video_line}</b> | ⏳ <b>{duration}</b>\n"
    "🔊 <b>{audio}</b>\n"
#    "💬 <b>{subtitle}</b>\n\n"
)

# --- Probing behaviour ---
# Timeout (seconds) for each mediainfo / ffprobe subprocess call.
PROBE_TIMEOUT = _get_float("PROBE_TIMEOUT", 15.0)
# Timeout (seconds) for a single streaming step before it is abandoned.
STREAM_TIMEOUT = _get_float("STREAM_TIMEOUT", 90.0)
# How many times a streaming step is retried when the remote DC misbehaves.
STREAM_RETRIES = _get_int("STREAM_RETRIES", 3)
# Number of parallel media streams allowed (raise for faster multi-DC work).
STREAM_CONCURRENCY = max(1, _get_int("STREAM_CONCURRENCY", 2))
# Number of channels processed in parallel.
CHANNEL_CONCURRENCY = max(1, _get_int("CHANNEL_CONCURRENCY", 2))
# Also grab a chunk from the middle/end of the file when headers lack metadata
# (needed by fragmented MP4 / MKV files whose cues live at the tail).
ENABLE_TAIL_PROBE = _get_bool("ENABLE_TAIL_PROBE", True)
# Directory used for temporary probe chunks.
TMP_DIR = os.environ.get("TMP_DIR", "downloads")
# Directory holding the pyrogram session file. Keeping it on a persistent
# volume avoids re-creating the per-datacenter auth keys on every restart.
SESSION_DIR = os.environ.get("SESSION_DIR", ".")

# Default delay between caption edits in the same channel.
EDIT_DELAY = _get_float("EDIT_DELAY", 3.5)

# --- Optional features ---
# Add "MediaInfo" inline button + full text report to /info replies.
FULL_REPORT = _get_bool("FULL_REPORT", True)


def validate() -> list[str]:
    """Return a list of human readable problems with the current config."""
    problems: list[str] = []
    if not API_ID:
        problems.append("API_ID is missing or not a number")
    if not API_HASH:
        problems.append("API_HASH is missing")
    if not BOT_TOKEN:
        problems.append("BOT_TOKEN is missing")
    if not ADMIN_ID:
        problems.append("ADMIN_ID is missing or not a number")
    if not MONGO_URI:
        problems.append("MONGO_URI is missing")
    try:
        CAPTION_TEMPLATE.format(
            title="", video_line="", duration="", audio="", subtitle="",
            size="", bitrate="", fps="", resolution="", codec="",
            container="", filename="",
        )
    except (KeyError, IndexError, ValueError) as exc:
        problems.append(f"CAPTION_TEMPLATE is invalid: {exc}")
    return problems

