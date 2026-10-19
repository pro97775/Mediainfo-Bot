import logging
import os
from typing import Any, Optional


def _get_int(name: str, default: int = 0) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _get_float(name: str, default: float = 0.0) -> float:
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


# --- Telegram API credentials (required bootstrap env vars) ---
API_ID = _get_int("API_ID")
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

# --- Access control ---
ADMIN_ID = _get_int("ADMIN_ID")
ALLOWED_CHATS = _get_chat_ids("ALLOWED_CHATS")

# --- Database ---
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongo:27017")
DB_NAME = os.environ.get("DB_NAME", "MediaInfo-Bot")
# Store WZGram MTProto session directly in MongoDB via MongoStorage
USE_MONGO_SESSION = _get_bool("USE_MONGO_SESSION", True)

# --- Logging ---
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
if not isinstance(logging.getLevelName(LOG_LEVEL), int):
    LOG_LEVEL = "INFO"
LOG_FORMAT = os.environ.get(
    "LOG_FORMAT",
    "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

# --- Garbage collection thresholds (gen0, gen1, gen2) ---
GC_GEN0 = _get_int("GC_GEN0", 700)
GC_GEN1 = _get_int("GC_GEN1", 10)
GC_GEN2 = _get_int("GC_GEN2", 10)
GC_THRESHOLD = (GC_GEN0, GC_GEN1, GC_GEN2)

# --- Caption templates & presets ---
# Available placeholders: {title} {video_line} {duration} {audio} {subtitle}
# plus: {size} {bitrate} {fps} {resolution} {codec} {container} {filename}
DEFAULT_CAPTION_TEMPLATE = (
    "<b>{title}</b>\n\n"
    "\U0001F39E <b>{video_line}</b> | \u23f3 <b>{duration}</b>\n"
    "\U0001F50A <b>{audio}</b>\n"
)

TEMPLATE_PRESETS: dict[str, tuple[str, str]] = {
    "default": (
        "Default Clean",
        DEFAULT_CAPTION_TEMPLATE,
    ),
    "subs": (
        "With Subtitles",
        "<b>\U0001F3AC {title}</b>\n\n"
        "\U0001F39E <b>{video_line}</b> | \u23f3 <b>{duration}</b>\n"
        "\U0001F50A <b>{audio}</b>\n"
        "\U0001F4AC <b>Subtitles:</b> {subtitle}\n",
    ),
    "detailed": (
        "Detailed Technical",
        "<b>\U0001F3AC {title}</b>\n\n"
        "\U0001F39E <b>Video:</b> {video_line} ({fps})\n"
        "\u23f3 <b>Duration:</b> {duration} | \U0001F4BE <b>Size:</b> {size}\n"
        "\U0001F4CA <b>Bitrate:</b> {bitrate} | \U0001F4E6 <b>Format:</b> {container}\n"
        "\U0001F50A <b>Audio:</b> {audio}\n"
        "\U0001F4AC <b>Subtitles:</b> {subtitle}\n",
    ),
    "minimal": (
        "Minimal Compact",
        "<b>{title}</b>\n"
        "\U0001F39E {video_line} \u2022 \u23f3 {duration} \u2022 \U0001F50A {audio}",
    ),
}

CAPTION_TEMPLATE = os.environ.get("CAPTION_TEMPLATE") or DEFAULT_CAPTION_TEMPLATE

# --- Probing & queue behaviour ---
PROBE_TIMEOUT = max(3.0, _get_float("PROBE_TIMEOUT", 15.0))
STREAM_TIMEOUT = max(10.0, _get_float("STREAM_TIMEOUT", 90.0))
STREAM_RETRIES = max(1, _get_int("STREAM_RETRIES", 3))
STREAM_CONCURRENCY = max(1, _get_int("STREAM_CONCURRENCY", 2))
CHANNEL_CONCURRENCY = max(1, _get_int("CHANNEL_CONCURRENCY", 2))
ENABLE_TAIL_PROBE = _get_bool("ENABLE_TAIL_PROBE", True)
TAIL_SIZE_MB = max(1, _get_int("TAIL_SIZE_MB", 1))
TMP_DIR = os.environ.get("TMP_DIR", "downloads")
SESSION_DIR = os.environ.get("SESSION_DIR", ".")
EDIT_DELAY = max(1.0, _get_float("EDIT_DELAY", 3.5))
FULL_REPORT = _get_bool("FULL_REPORT", True)

# --- Error alerting & retry thresholds ---
ERROR_THRESHOLD = max(1, _get_int("ERROR_THRESHOLD", 3))
ALERT_COOLDOWN = max(60, _get_int("ALERT_COOLDOWN", 600))
MAX_RETRY_ATTEMPTS = max(1, _get_int("MAX_RETRY_ATTEMPTS", 5))


def env_defaults_dict() -> dict[str, Any]:
    """Return the complete dictionary of all live-configurable settings from env/defaults.

    Used to seed MongoDB on first run and when resetting configs from the panel.
    """
    return {
        "allowed_chats": sorted(set(ALLOWED_CHATS)),
        "caption_template": CAPTION_TEMPLATE,
        "edit_delay": EDIT_DELAY,
        "probe_timeout": PROBE_TIMEOUT,
        "stream_timeout": STREAM_TIMEOUT,
        "stream_retries": STREAM_RETRIES,
        "stream_concurrency": STREAM_CONCURRENCY,
        "channel_concurrency": CHANNEL_CONCURRENCY,
        "enable_tail_probe": ENABLE_TAIL_PROBE,
        "tail_size_mb": TAIL_SIZE_MB,
        "full_report": FULL_REPORT,
        "paused": False,
        "error_threshold": ERROR_THRESHOLD,
        "alert_cooldown": ALERT_COOLDOWN,
        "max_retry_attempts": MAX_RETRY_ATTEMPTS,
        "log_level": LOG_LEVEL,
        "gc_gen0": GC_GEN0,
        "gc_gen1": GC_GEN1,
        "gc_gen2": GC_GEN2,
        "use_mongo_session": USE_MONGO_SESSION,
    }


def validate_template(template: str) -> Optional[str]:
    """Return None if valid, or a human-readable error message."""
    if not template or not template.strip():
        return "Template cannot be empty"
    try:
        template.format(
            title="", video_line="", duration="", audio="", subtitle="",
            size="", bitrate="", fps="", resolution="", codec="",
            container="", filename="",
        )
        return None
    except (KeyError, IndexError, ValueError) as exc:
        return f"Invalid placeholder or format syntax: {exc}"


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
    tpl_err = validate_template(CAPTION_TEMPLATE)
    if tpl_err:
        problems.append(f"CAPTION_TEMPLATE is invalid: {tpl_err}")
    return problems