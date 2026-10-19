"""MediaInfo Bot - extracts media metadata from Telegram files and rewrites captions.

Powered by WZGram (high-performance Pyrogram fork with WarpCrypto & MongoStorage)
and MongoDB-backed configuration management.
"""

import asyncio
import collections
import datetime
import gc
import html
import io
import json
import logging
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import uuid
from collections import defaultdict
from functools import lru_cache
from typing import Any, Optional

try:
    import uvloop
    uvloop.install()
except ImportError:
    pass

import psutil
from aiofiles import open as aiopen
from aiofiles.os import remove as aioremove
from aiohttp import web
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from motor.motor_asyncio import AsyncIOMotorClient

try:
    from wzgram import Client, filters
    from wzgram.enums import ParseMode
    from wzgram.errors import (
        ChannelInvalid,
        ChannelPrivate,
        ChatAdminRequired,
        ChatWriteForbidden,
        FileReferenceEmpty,
        FileReferenceExpired,
        FloodWait,
        MediaCaptionTooLong,
        MessageEmpty,
        MessageIdInvalid,
        MessageNotModified,
        PeerIdInvalid,
        RPCError,
    )
    from wzgram.file_id import FileId
    from wzgram.types import InlineKeyboardButton, InlineKeyboardMarkup
    try:
        from wzgram.storage import FileStorage, MongoStorage
    except ImportError:
        FileStorage = None
        MongoStorage = None
except ImportError:  # pragma: no cover - fallback if run under pyrogram
    from pyrogram import Client, filters
    from pyrogram.enums import ParseMode
    from pyrogram.errors import (
        ChannelInvalid,
        ChannelPrivate,
        ChatAdminRequired,
        ChatWriteForbidden,
        FileReferenceEmpty,
        FileReferenceExpired,
        FloodWait,
        MediaCaptionTooLong,
        MessageEmpty,
        MessageIdInvalid,
        MessageNotModified,
        PeerIdInvalid,
        RPCError,
    )
    from pyrogram.file_id import FileId
    from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
    try:
        from pyrogram.storage import FileStorage, MongoStorage
    except ImportError:
        FileStorage = None
        MongoStorage = None

import config as cfg
import dc_stream
from config import (
    ADMIN_ID,
    ALERT_COOLDOWN as ALERT_COOLDOWN_DEFAULT,
    ALLOWED_CHATS,
    API_HASH,
    API_ID,
    BOT_TOKEN,
    CAPTION_TEMPLATE as CAPTION_TEMPLATE_ENV,
    CHANNEL_CONCURRENCY as CHANNEL_CONCURRENCY_DEFAULT,
    DB_NAME,
    EDIT_DELAY as EDIT_DELAY_DEFAULT,
    ENABLE_TAIL_PROBE as ENABLE_TAIL_PROBE_DEFAULT,
    ERROR_THRESHOLD as ERROR_THRESHOLD_DEFAULT,
    FULL_REPORT as FULL_REPORT_DEFAULT,
    GC_THRESHOLD as GC_THRESHOLD_DEFAULT,
    LOG_FORMAT,
    LOG_LEVEL as LOG_LEVEL_DEFAULT,
    MAX_RETRY_ATTEMPTS as MAX_RETRY_ATTEMPTS_DEFAULT,
    MONGO_URI,
    PROBE_TIMEOUT as PROBE_TIMEOUT_DEFAULT,
    SESSION_DIR,
    STREAM_CONCURRENCY as STREAM_CONCURRENCY_DEFAULT,
    STREAM_RETRIES as STREAM_RETRIES_DEFAULT,
    STREAM_TIMEOUT as STREAM_TIMEOUT_DEFAULT,
    TAIL_SIZE_MB as TAIL_SIZE_MB_DEFAULT,
    TEMPLATE_PRESETS,
    TMP_DIR,
    USE_MONGO_SESSION as USE_MONGO_SESSION_DEFAULT,
)

logging.basicConfig(level=LOG_LEVEL_DEFAULT, format=LOG_FORMAT, force=True)
logging.getLogger("pyrogram").setLevel(logging.ERROR)
logging.getLogger("wzgram").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

# --- IN-MEMORY LOG BUFFER (for /logs command & inline log viewer) ---
_log_buffer: collections.deque = collections.deque(maxlen=250)


class _BufferHandler(logging.Handler):
    def emit(self, record):
        try:
            _log_buffer.append(self.format(record))
        except Exception:
            pass


_buf_handler = _BufferHandler()
_buf_handler.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger().addHandler(_buf_handler)

# --- DATABASE SETUP ---
db_client = AsyncIOMotorClient(MONGO_URI, serverSelectionTimeoutMS=8000)
db = db_client[DB_NAME]
SESSION_DB_NAME = f"{DB_NAME}_session"
last_id_collection = db["last_processed_id"]
settings_collection = db["settings"]
stats_collection = db["stats"]
retry_collection = db["retry_queue"]

# Telegram hard limit for media captions.
TG_CAPTION_LIMIT = 1024
START_TIME = time.time()

# --- ALL DYNAMIC RUNTIME CONFIGS (persisted in MongoDB settings["bot_config"]) ---
authorized_chats: set[int] = set(ALLOWED_CHATS)
_db_ok = True

CAPTION_TEMPLATE: str = CAPTION_TEMPLATE_ENV
EDIT_DELAY: float = EDIT_DELAY_DEFAULT
PROBE_TIMEOUT: float = PROBE_TIMEOUT_DEFAULT
STREAM_TIMEOUT: float = STREAM_TIMEOUT_DEFAULT
STREAM_RETRIES: int = STREAM_RETRIES_DEFAULT
STREAM_CONCURRENCY: int = STREAM_CONCURRENCY_DEFAULT
CHANNEL_CONCURRENCY: int = CHANNEL_CONCURRENCY_DEFAULT
ENABLE_TAIL_PROBE: bool = ENABLE_TAIL_PROBE_DEFAULT
TAIL_SIZE_MB: int = TAIL_SIZE_MB_DEFAULT
TAIL_SIZE: int = TAIL_SIZE_MB_DEFAULT * 1024 * 1024
FULL_REPORT: bool = FULL_REPORT_DEFAULT
AUTO_CAPTION_PAUSED: bool = False
ERROR_THRESHOLD: int = ERROR_THRESHOLD_DEFAULT
_ALERT_COOLDOWN: int = ALERT_COOLDOWN_DEFAULT
MAX_RETRY_ATTEMPTS: int = MAX_RETRY_ATTEMPTS_DEFAULT
LOG_LEVEL: str = LOG_LEVEL_DEFAULT
GC_THRESHOLD: tuple[int, int, int] = GC_THRESHOLD_DEFAULT
USE_MONGO_SESSION: bool = USE_MONGO_SESSION_DEFAULT

# Cache of resolved chat titles so pagination in the inline panel is fast.
_chat_title_cache: dict[int, str] = {}

# Tracks pending interactive input expected from ADMIN_ID:
# {"mode": "add_chat" | "set_delay" | "set_template" | "broadcast" | "edit_cfg", "ts": float, "tab": str}
_admin_input_state: dict[int, dict] = {}


def _utcnow() -> datetime.datetime:
    """Timezone-aware UTC now."""
    return datetime.datetime.now(datetime.timezone.utc)


def _db_warn(action: str, exc: Exception):
    """Log a database failure once per state change instead of every call."""
    global _db_ok
    if _db_ok:
        logger.error(f"MongoDB unavailable during {action}: {exc}")
        _db_ok = False


def _db_ok_again():
    global _db_ok
    if not _db_ok:
        logger.info("MongoDB reachable again")
        _db_ok = True


def _current_config_snapshot() -> dict[str, Any]:
    """Return a serializable snapshot of all active live-configurable bot settings."""
    return {
        "allowed_chats": sorted(authorized_chats),
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
        "paused": AUTO_CAPTION_PAUSED,
        "error_threshold": ERROR_THRESHOLD,
        "alert_cooldown": _ALERT_COOLDOWN,
        "max_retry_attempts": MAX_RETRY_ATTEMPTS,
        "log_level": LOG_LEVEL,
        "gc_gen0": GC_THRESHOLD[0],
        "gc_gen1": GC_THRESHOLD[1],
        "gc_gen2": GC_THRESHOLD[2],
        "use_mongo_session": USE_MONGO_SESSION,
    }


def _apply_config_dict(doc: dict[str, Any]):
    """Apply a configuration dictionary to all runtime globals."""
    global authorized_chats, CAPTION_TEMPLATE, EDIT_DELAY
    global PROBE_TIMEOUT, STREAM_TIMEOUT, STREAM_RETRIES
    global STREAM_CONCURRENCY, CHANNEL_CONCURRENCY
    global ENABLE_TAIL_PROBE, TAIL_SIZE_MB, TAIL_SIZE, FULL_REPORT
    global AUTO_CAPTION_PAUSED, ERROR_THRESHOLD, _ALERT_COOLDOWN
    global MAX_RETRY_ATTEMPTS, LOG_LEVEL, GC_THRESHOLD, USE_MONGO_SESSION
    global stream_semaphore, channel_semaphore

    if isinstance(doc.get("allowed_chats"), list):
        authorized_chats = {
            int(x) for x in doc["allowed_chats"] if str(x).lstrip("-").isdigit()
        }
    if isinstance(doc.get("caption_template"), str):
        tpl = doc["caption_template"]
        if not cfg.validate_template(tpl):
            CAPTION_TEMPLATE = tpl
    if isinstance(doc.get("edit_delay"), (int, float)) and not isinstance(doc.get("edit_delay"), bool):
        EDIT_DELAY = max(1.0, min(60.0, float(doc["edit_delay"])))
    if isinstance(doc.get("probe_timeout"), (int, float)) and not isinstance(doc.get("probe_timeout"), bool):
        PROBE_TIMEOUT = max(3.0, min(120.0, float(doc["probe_timeout"])))
    if isinstance(doc.get("stream_timeout"), (int, float)) and not isinstance(doc.get("stream_timeout"), bool):
        STREAM_TIMEOUT = max(10.0, min(600.0, float(doc["stream_timeout"])))
    if isinstance(doc.get("stream_retries"), int) and not isinstance(doc.get("stream_retries"), bool):
        STREAM_RETRIES = max(1, min(10, int(doc["stream_retries"])))
    if isinstance(doc.get("stream_concurrency"), int) and not isinstance(doc.get("stream_concurrency"), bool):
        new_sc = max(1, min(8, int(doc["stream_concurrency"])))
        if new_sc != STREAM_CONCURRENCY:
            STREAM_CONCURRENCY = new_sc
            stream_semaphore = asyncio.Semaphore(STREAM_CONCURRENCY)
        if "app" in globals() and app is not None:
            app.max_concurrent_transmissions = max(2, STREAM_CONCURRENCY)
    if isinstance(doc.get("channel_concurrency"), int) and not isinstance(doc.get("channel_concurrency"), bool):
        new_cc = max(1, min(8, int(doc["channel_concurrency"])))
        if new_cc != CHANNEL_CONCURRENCY:
            CHANNEL_CONCURRENCY = new_cc
            channel_semaphore = asyncio.Semaphore(CHANNEL_CONCURRENCY)
    if isinstance(doc.get("enable_tail_probe"), bool):
        ENABLE_TAIL_PROBE = doc["enable_tail_probe"]
    if isinstance(doc.get("tail_size_mb"), int) and not isinstance(doc.get("tail_size_mb"), bool):
        TAIL_SIZE_MB = max(1, min(16, int(doc["tail_size_mb"])))
        TAIL_SIZE = TAIL_SIZE_MB * 1024 * 1024
    if isinstance(doc.get("full_report"), bool):
        FULL_REPORT = doc["full_report"]
    if isinstance(doc.get("paused"), bool):
        AUTO_CAPTION_PAUSED = doc["paused"]
    if isinstance(doc.get("error_threshold"), int) and not isinstance(doc.get("error_threshold"), bool):
        ERROR_THRESHOLD = max(1, min(25, int(doc["error_threshold"])))
    if isinstance(doc.get("alert_cooldown"), int) and not isinstance(doc.get("alert_cooldown"), bool):
        _ALERT_COOLDOWN = max(60, min(86400, int(doc["alert_cooldown"])))
    if isinstance(doc.get("max_retry_attempts"), int) and not isinstance(doc.get("max_retry_attempts"), bool):
        MAX_RETRY_ATTEMPTS = max(1, min(20, int(doc["max_retry_attempts"])))
    if isinstance(doc.get("log_level"), str):
        lvl = doc["log_level"].upper().strip()
        if lvl in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            LOG_LEVEL = lvl
            logging.getLogger().setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    if all(
        isinstance(doc.get(k), int) and not isinstance(doc.get(k), bool)
        for k in ("gc_gen0", "gc_gen1", "gc_gen2")
    ):
        GC_THRESHOLD = (
            max(100, int(doc["gc_gen0"])),
            max(5, int(doc["gc_gen1"])),
            max(5, int(doc["gc_gen2"])),
        )
        gc.set_threshold(*GC_THRESHOLD)
    if isinstance(doc.get("use_mongo_session"), bool):
        USE_MONGO_SESSION = doc["use_mongo_session"]


async def sync_all_configs():
    """Sync ALL bot configurations and authorized chats with MongoDB on startup.

    1. Reads both `{"_id": "allowed_chats"}` and `{"_id": "bot_config"}` from MongoDB.
    2. Seeds from `.env` defaults on first run; on subsequent runs, respects stored
       MongoDB values (so channels removed via `/remove` stay removed across restarts).
    3. Saves the complete configuration document back to MongoDB (`settings_collection`).
    """
    global authorized_chats
    defaults = cfg.env_defaults_dict()
    try:
        chats_doc = await settings_collection.find_one({"_id": "allowed_chats"})
        cfg_doc = await settings_collection.find_one({"_id": "bot_config"})
        _db_ok_again()

        has_stored_chats = False
        stored_chats: set[int] = set()
        if cfg_doc and isinstance(cfg_doc.get("allowed_chats"), list):
            has_stored_chats = True
            stored_chats |= {
                int(x) for x in cfg_doc["allowed_chats"] if str(x).lstrip("-").isdigit()
            }
        if chats_doc and isinstance(chats_doc.get("chat_ids"), list):
            has_stored_chats = True
            stored_chats |= {
                int(x) for x in chats_doc["chat_ids"] if str(x).lstrip("-").isdigit()
            }

        merged_chats = (
            sorted(stored_chats) if has_stored_chats else sorted(set(ALLOWED_CHATS))
        )

        merged_cfg: dict[str, Any] = dict(defaults)
        if cfg_doc:
            for k in defaults:
                if k in cfg_doc and cfg_doc[k] is not None:
                    merged_cfg[k] = cfg_doc[k]
        merged_cfg["allowed_chats"] = merged_chats

        _apply_config_dict(merged_cfg)

        db_payload = dict(_current_config_snapshot())
        db_payload["updated_at"] = _utcnow()

        await settings_collection.update_one(
            {"_id": "allowed_chats"},
            {"$set": {"chat_ids": merged_chats, "updated_at": _utcnow()}},
            upsert=True,
        )
        await settings_collection.update_one(
            {"_id": "bot_config"},
            {"$set": db_payload},
            upsert=True,
        )
    except Exception as e:
        _db_warn("sync_all_configs", e)
        _apply_config_dict(defaults)
    logger.info(
        f"MongoDB config synced | Authorized chats: {sorted(authorized_chats) or 'none'}"
    )


async def save_runtime_settings(**updates):
    """Persist any config changes to MongoDB (alongside the full config snapshot)."""
    try:
        snapshot = _current_config_snapshot()
        for k, v in updates.items():
            if k in snapshot:
                snapshot[k] = v
        snapshot["updated_at"] = _utcnow()
        await settings_collection.update_one(
            {"_id": "bot_config"},
            {"$set": snapshot},
            upsert=True,
        )
        if "allowed_chats" in updates:
            await settings_collection.update_one(
                {"_id": "allowed_chats"},
                {"$set": {"chat_ids": sorted(authorized_chats), "updated_at": _utcnow()}},
                upsert=True,
            )
        _db_ok_again()
    except Exception as e:
        _db_warn("save_runtime_settings", e)


async def sync_chats():
    """Backwards-compatible alias for syncing configs and chats."""
    await sync_all_configs()


async def sync_runtime_settings():
    """Backwards-compatible alias for syncing runtime settings."""
    await sync_all_configs()


async def get_last_id(chat_id: int) -> int:
    """Retrieve the last processed message ID for a specific chat."""
    try:
        data = await last_id_collection.find_one({"chat_id": chat_id})
        _db_ok_again()
        return data["last_id"] if data else 1
    except Exception as e:
        _db_warn("get_last_id", e)
        return 1


async def save_last_id(chat_id: int, last_id: int):
    """Save the current message ID as the last processed."""
    try:
        await last_id_collection.update_one(
            {"chat_id": chat_id},
            {"$max": {"last_id": last_id}},
            upsert=True,
        )
        _db_ok_again()
    except Exception as e:
        _db_warn("save_last_id", e)


# --- STATS HELPERS ---
async def increment_stat(chat_id: int, *, deep: bool = True):
    """Count a processed file, tracking deep scans vs metadata fallbacks."""
    field = "deep" if deep else "fallback"
    try:
        await stats_collection.update_one(
            {"chat_id": chat_id},
            {
                "$inc": {"count": 1, field: 1},
                "$set": {"last_seen": _utcnow()},
            },
            upsert=True,
        )
        _db_ok_again()
    except Exception as e:
        _db_warn("increment_stat", e)


async def get_all_stats() -> list[dict]:
    """Return all per-chat stats sorted by count descending."""
    try:
        stats = await stats_collection.find({}).sort("count", -1).to_list(length=None)
        _db_ok_again()
        return stats
    except Exception as e:
        _db_warn("get_all_stats", e)
        return []


async def get_chat_stat(chat_id: int) -> dict:
    """Return stats document for a single chat."""
    try:
        doc = await stats_collection.find_one({"chat_id": chat_id})
        _db_ok_again()
        return doc or {}
    except Exception:
        return {}


# --- ERROR COUNTER (for admin DM alerts) ---
_error_counts: dict[int, int] = defaultdict(int)
_last_alert: dict[int, float] = {}


async def _report_error(chat_id: int, reason: str):
    """Track consecutive failures per chat; DM admin when threshold is hit."""
    _error_counts[chat_id] += 1
    if _error_counts[chat_id] < ERROR_THRESHOLD:
        return

    _error_counts[chat_id] = 0
    now = time.monotonic()
    if now - _last_alert.get(chat_id, 0) < _ALERT_COOLDOWN:
        return
    _last_alert[chat_id] = now

    if not ADMIN_ID:
        return
    try:
        markup = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("\U0001F4E5 Queues", callback_data="adm:queue"),
                InlineKeyboardButton("\U0001F4DC Logs", callback_data="adm:logs:20"),
            ],
            [InlineKeyboardButton("\U0001F39B Dashboard", callback_data="adm:home")],
        ])
        await app.send_message(
            ADMIN_ID,
            "<b>\u26a0\ufe0f Channel Error Alert</b>\n"
            "<blockquote>"
            f"\u251c <b>Chat ID:</b> <code>{chat_id}</code>\n"
            f"\u251c <b>Failures:</b> <code>{ERROR_THRESHOLD}x consecutive</code>\n"
            f"\u2514 <b>Reason:</b> <code>{html.escape(str(reason))[:280]}</code>"
            "</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=markup,
        )
    except Exception:
        pass


def _reset_error_count(chat_id: int):
    _error_counts[chat_id] = 0


# --- RETRY QUEUE HELPERS ---
async def enqueue_retry(chat_id: int, message_id: int, caption: str):
    """Persist a failed caption edit for later retry."""
    try:
        await retry_collection.update_one(
            {"chat_id": chat_id, "message_id": message_id},
            {
                "$set": {
                    "caption": caption,
                    "next_retry": _utcnow() + datetime.timedelta(seconds=60),
                },
                "$setOnInsert": {"attempts": 0},
            },
            upsert=True,
        )
        _db_ok_again()
    except Exception as e:
        _db_warn("enqueue_retry", e)


async def process_retry_queue():
    """Scheduled job: retry failed caption edits."""
    try:
        cursor = retry_collection.find({
            "next_retry": {"$lte": _utcnow()},
            "attempts": {"$lt": MAX_RETRY_ATTEMPTS},
        })
        docs = await cursor.to_list(length=50)
        _db_ok_again()
    except Exception as e:
        _db_warn("process_retry_queue", e)
        return

    for doc in docs:
        chat_id = doc["chat_id"]
        message_id = doc["message_id"]
        caption = doc["caption"]
        attempts = doc.get("attempts", 0)
        try:
            await app.edit_message_caption(
                chat_id, message_id, caption, parse_mode=ParseMode.HTML
            )
            await retry_collection.delete_one({"_id": doc["_id"]})
            logger.info(f"Retry success: chat={chat_id} msg={message_id}")
        except FloodWait as e:
            await _retry_backoff(doc, e.value + 5)
        except (MessageIdInvalid, MessageEmpty, MessageNotModified):
            await retry_collection.delete_one({"_id": doc["_id"]})
            logger.info(f"Retry dropped (gone/unchanged): chat={chat_id} msg={message_id}")
        except (ChatWriteForbidden, ChatAdminRequired, ChannelPrivate,
                ChannelInvalid, PeerIdInvalid) as e:
            await retry_collection.delete_one({"_id": doc["_id"]})
            logger.warning(f"Retry dropped (no access to {chat_id}): {e}")
        except MediaCaptionTooLong:
            trimmed = _trim_caption(caption)
            try:
                await app.edit_message_caption(
                    chat_id, message_id, trimmed, parse_mode=ParseMode.HTML
                )
                await retry_collection.delete_one({"_id": doc["_id"]})
                logger.info(f"Retry success after trim: chat={chat_id} msg={message_id}")
            except Exception as err:
                logger.warning(f"Retry trim failed: {err}")
                await _retry_backoff(doc, 60 * (2 ** attempts))
        except Exception as e:
            if attempts + 1 >= MAX_RETRY_ATTEMPTS:
                await retry_collection.delete_one({"_id": doc["_id"]})
                logger.warning(
                    f"Retry abandoned after {MAX_RETRY_ATTEMPTS} attempts: {e}"
                )
            else:
                await _retry_backoff(doc, 60 * (2 ** attempts))


async def _retry_backoff(doc: dict, seconds: float):
    """Push a retry document further into the future and bump its attempts."""
    try:
        await retry_collection.update_one(
            {"_id": doc["_id"]},
            {
                "$set": {"next_retry": _utcnow() + datetime.timedelta(seconds=seconds)},
                "$inc": {"attempts": 1},
            },
        )
    except Exception as e:
        _db_warn("_retry_backoff", e)


try:
    os.makedirs(SESSION_DIR, exist_ok=True)
except OSError as _e:  # pragma: no cover
    logger.warning(f"Could not create SESSION_DIR {SESSION_DIR!r}: {_e}")


def _configure_client_storage(client: Client) -> None:
    """Apply the active USE_MONGO_SESSION setting to `client.storage` before start()."""
    if USE_MONGO_SESSION and MongoStorage is not None:
        try:
            client.storage = MongoStorage(
                "MediaInfo-Bot_Session",
                connection=db_client,
                database=SESSION_DB_NAME,
            )
            logger.info(
                f"WZGram MongoStorage active (database={SESSION_DB_NAME!r})"
            )
            return
        except Exception as e:
            logger.warning(f"Could not initialize MongoStorage, using file session: {e}")
    if FileStorage is not None and not isinstance(getattr(client, "storage", None), FileStorage):
        try:
            from pathlib import Path
            client.storage = FileStorage("MediaInfo-Bot", Path(SESSION_DIR))
        except Exception:
            pass


def _build_client() -> Client:
    """Create the WZGram Client, using MongoStorage when enabled and available."""
    client_kwargs: dict[str, Any] = {
        "api_id": API_ID,
        "api_hash": API_HASH,
        "bot_token": BOT_TOKEN,
        "workers": 6,
        "sleep_threshold": 60,
        "workdir": SESSION_DIR,
        "max_concurrent_transmissions": max(2, STREAM_CONCURRENCY),
    }
    if USE_MONGO_SESSION and MongoStorage is not None:
        try:
            client_kwargs["storage_engine"] = MongoStorage(
                "MediaInfo-Bot_Session",
                connection=db_client,
                database=SESSION_DB_NAME,
            )
        except Exception as e:
            logger.warning(f"Could not initialize MongoStorage, using file session: {e}")
    return Client("MediaInfo-Bot", **client_kwargs)


app = _build_client()

stream_semaphore = asyncio.Semaphore(STREAM_CONCURRENCY)
channel_semaphore = asyncio.Semaphore(CHANNEL_CONCURRENCY)
active_users: set = set()

_last_edit: dict[tuple[int, int], float] = {}
channel_queues: dict[int, list] = defaultdict(list)
channel_locks: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
channel_workers: dict[int, bool] = defaultdict(bool)
last_edit_time: dict[int, float] = {}

# Runtime counters exposed through /stats, /server, and the control panel.
_runtime = {
    "deep": 0,        # captions produced from a real probe
    "fallback": 0,    # captions produced from Telegram metadata only
    "flood": 0,       # FloodWait hits while streaming
    "probe_fail": 0,  # streaming/probing errors
}

scheduler = AsyncIOScheduler()


_LANGUAGE_MAP: dict[str, str] = {
    'en': 'English', 'eng': 'English',
    'hi': 'Hindi', 'hin': 'Hindi',
    'ta': 'Tamil', 'tam': 'Tamil',
    'te': 'Telugu', 'tel': 'Telugu',
    'ml': 'Malayalam', 'mal': 'Malayalam',
    'kn': 'Kannada', 'kan': 'Kannada',
    'bn': 'Bengali', 'ben': 'Bengali',
    'mr': 'Marathi', 'mar': 'Marathi',
    'gu': 'Gujarati', 'guj': 'Gujarati',
    'pa': 'Punjabi', 'pun': 'Punjabi',
    'bho': 'Bhojpuri',
    'zh': 'Chinese', 'chi': 'Chinese', 'cmn': 'Chinese', 'zho': 'Chinese',
    'ko': 'Korean', 'kor': 'Korean',
    'pt': 'Portuguese', 'por': 'Portuguese',
    'th': 'Thai', 'tha': 'Thai',
    'tl': 'Tagalog', 'tgl': 'Tagalog', 'fil': 'Tagalog',
    'ja': 'Japanese', 'jpn': 'Japanese',
    'es': 'Spanish', 'spa': 'Spanish',
    'sv': 'Swedish', 'swe': 'Swedish',
    'fr': 'French', 'fra': 'French', 'fre': 'French',
    'de': 'German', 'deu': 'German', 'ger': 'German',
    'it': 'Italian', 'ita': 'Italian',
    'ru': 'Russian', 'rus': 'Russian',
    'ar': 'Arabic', 'ara': 'Arabic',
    'tr': 'Turkish', 'tur': 'Turkish',
    'nl': 'Dutch', 'nld': 'Dutch', 'dut': 'Dutch',
    'pl': 'Polish', 'pol': 'Polish',
    'vi': 'Vietnamese', 'vie': 'Vietnamese',
    'id': 'Indonesian', 'ind': 'Indonesian',
    'ms': 'Malay', 'msa': 'Malay', 'may': 'Malay',
    'fa': 'Persian', 'fas': 'Persian', 'per': 'Persian',
    'ur': 'Urdu', 'urd': 'Urdu',
    'he': 'Hebrew', 'heb': 'Hebrew',
    'el': 'Greek', 'ell': 'Greek', 'gre': 'Greek',
    'hu': 'Hungarian', 'hun': 'Hungarian',
    'cs': 'Czech', 'ces': 'Czech', 'cze': 'Czech',
    'ro': 'Romanian', 'ron': 'Romanian', 'rum': 'Romanian',
    'da': 'Danish', 'dan': 'Danish',
    'fi': 'Finnish', 'fin': 'Finnish',
    'no': 'Norwegian', 'nor': 'Norwegian', 'nob': 'Norwegian',
    'uk': 'Ukrainian', 'ukr': 'Ukrainian',
    'ca': 'Catalan', 'cat': 'Catalan',
    'hr': 'Croatian', 'hrv': 'Croatian',
    'sk': 'Slovak', 'slk': 'Slovak', 'slo': 'Slovak',
    'sr': 'Serbian', 'srp': 'Serbian',
    'bg': 'Bulgarian', 'bul': 'Bulgarian',
    'unknown': 'Original Audio',
    'und': 'Original Audio',
    'mul': 'Multi Audio',
    'zxx': 'Original Audio',
    'mis': 'Original Audio',
}

_KNOWN_LANG_NAMES = sorted(set(_LANGUAGE_MAP.values()))

_NON_LANGUAGE_WORDS = {
    'stereo', 'mono', 'surround', 'commentary', 'default', 'forced',
    'original', 'audio', 'track', 'main', 'clean', 'dubbed', 'dub',
    'aac', 'flac', 'opus', 'ac3', 'eac3', 'dts', 'truehd', 'atmos',
    'vorbis', 'mp3', 'pcm', 'lpcm', 'he-aac', 'ddp', 'dolby', 'none',
    'null', 'sdh', 'full', 'signs', 'songs',
}


@lru_cache(maxsize=256)
def get_full_language_name(code: str) -> str:
    """Map an ISO-639 code (or an already spelled out name) to a display name."""
    if not code:
        return 'Original Audio'

    cleaned = str(code).split('(')[0].strip()
    cleaned = re.split(r'[;,/]', cleaned)[0].strip()
    if not cleaned:
        return 'Original Audio'

    base = re.split(r'[-_]', cleaned)[0].strip().lower()
    low_clean = cleaned.lower()

    if low_clean in _NON_LANGUAGE_WORDS or base in _NON_LANGUAGE_WORDS:
        return 'Original Audio'

    if base in _LANGUAGE_MAP:
        return _LANGUAGE_MAP[base]
    if low_clean in _LANGUAGE_MAP:
        return _LANGUAGE_MAP[low_clean]

    for name in _KNOWN_LANG_NAMES:
        if low_clean == name.lower():
            return name

    if cleaned.isalpha() and len(cleaned) > 3:
        return cleaned.title()

    return 'Original Audio'


def _resolve_audio_track_lang(raw_lang: str, raw_title: str = '') -> str:
    """Resolve an audio track's language from its language tag, falling back to title hints."""
    if raw_lang:
        mapped = get_full_language_name(raw_lang)
        if mapped != 'Original Audio':
            return mapped
    if raw_title:
        title_langs = _langs_from_text(raw_title)
        if title_langs:
            return title_langs[0]
        mapped_title = get_full_language_name(raw_title)
        if mapped_title != 'Original Audio':
            return mapped_title
    return 'Original Audio'


def _normalize_sub_lang(raw_lang: str, raw_title: str = '') -> Optional[str]:
    """Return a clean subtitle language name, or None for untagged/non-language tags."""
    for candidate in (raw_lang, raw_title):
        if not candidate:
            continue
        low = str(candidate).strip().lower()
        if low in ('und', 'unknown', 'zxx', 'mis', 'default', 'forced', 'sdh', 'cc') or low in _NON_LANGUAGE_WORDS:
            continue
        mapped = get_full_language_name(candidate)
        if mapped not in ('Original Audio', 'Multi Audio'):
            return mapped
        title_langs = _langs_from_text(candidate)
        if title_langs and title_langs[0] not in ('Original Audio', 'Multi Audio', 'Dual Audio'):
            return title_langs[0]
    return None


@lru_cache(maxsize=64)
def get_standard_resolution(height: int) -> Optional[str]:
    if not height:
        return None
    if height <= 240:  return "240p"
    if height <= 360:  return "360p"
    if height <= 480:  return "480p"
    if height <= 576:  return "576p"
    if height <= 720:  return "720p"
    if height <= 1080: return "1080p"
    if height <= 1440: return "1440p"
    if height <= 2160: return "2160p"
    if height <= 4320: return "4320p"
    return "4320p+"


@lru_cache(maxsize=128)
def get_video_format(codec: str, transfer: str = '', hdr: str = '', bit_depth: str = '') -> Optional[str]:
    if not codec:
        return None
    codec = codec.lower()
    parts: list[str] = []

    if any(x in codec for x in ('hevc', 'h.265', 'h265')):          parts.append('HEVC')
    elif 'av1' in codec:                                             parts.append('AV1')
    elif any(x in codec for x in ('avc', 'avc1', 'h.264', 'h264')): parts.append('x264')
    elif 'vp9' in codec:                                             parts.append('VP9')
    elif 'vp8' in codec:                                             parts.append('VP8')
    elif any(x in codec for x in ('mpeg4', 'xvid', 'divx')):         parts.append('MPEG4')
    elif any(x in codec for x in ('mpeg-2', 'mpeg2')):               parts.append('MPEG2')
    elif any(x in codec for x in ('vc-1', 'vc1')):                   parts.append('VC1')
    elif 'prores' in codec:                                          parts.append('ProRes')
    elif 'theora' in codec:                                          parts.append('Theora')
    elif any(x in codec for x in ('wmv', 'vc-9')):                   parts.append('WMV')
    else:
        return None

    try:
        depth = int(re.findall(r'\d+', str(bit_depth))[0]) if bit_depth else 0
        if depth > 8:
            parts.append(f"{depth}bit")
    except (ValueError, TypeError, IndexError):
        pass

    t = (transfer or '').lower()
    h = (hdr or '').lower()
    if 'dolby vision' in h or 'dvhe' in h or 'dolby' in h:
        parts.append('DV')
    elif any(x in t for x in ('pq', 'hlg', 'smpte2084', 'smpte 2084', '2084', 'arib')) or 'hdr' in h:
        parts.append('HDR')

    return ' '.join(parts)


_VIDEO_CODEC_HINTS = (
    'avc', 'hevc', 'h.264', 'h264', 'h.265', 'h265', 'av1', 'vp8', 'vp9',
    'mpeg-4 visual', 'mpeg4', 'mpeg-2 video', 'xvid', 'divx', 'vc-1',
    'prores', 'theora', 'wmv',
)


def _is_video_track(track: dict) -> bool:
    """Decide whether a mediainfo track describes the video stream."""
    t = (track.get('@type', '') or '').strip().lower()
    if t == 'video':
        return True
    if t in ('audio', 'text', 'menu', 'general', 'image', 'other', 'subtitle'):
        return False

    fmt = (track.get('Format', '') or '').lower()
    cid = (track.get('CodecID', '') or '').lower()

    if any(x in fmt for x in _VIDEO_CODEC_HINTS):
        return True
    if any(x in cid for x in ('avc', 'h264', 'hevc', 'h265', 'av01', 'av1',
                              'vp08', 'vp09', 'mp4v', 'xvid', 'divx', 'vc-1')):
        return True
    return bool(track.get('Width') or track.get('Height'))


_SUB_FORMATS = (
    'pgs', 'subrip', 'srt', 'ass', 'ssa', 'timed text', 'ttml', 'webvtt',
    'vtt', 'dvb subtitle', 'dvd subtitle', 'teletext', 'mov_text', 'utf-8',
)
_SUB_CODEC_IDS = (
    's_text', 's_hdmv', 's_vobsub', 's_dvbsub', 'subp', 'pgs', 'tx3g',
    'c608', 'c708', 'text',
)


def _has_subtitles(tracks: list) -> bool:
    """True when at least one track in the mediainfo output is a subtitle."""
    for track in tracks:
        if not isinstance(track, dict):
            continue
        t = (track.get('@type', '') or '').strip().lower()
        if t in ('text', 'subtitle'):
            return True
        if t in ('video', 'audio', 'general', 'menu', 'image'):
            continue

        fmt = (track.get('Format', '') or '').lower()
        cid = (track.get('CodecID', '') or '').lower()
        if any(x in fmt for x in _SUB_FORMATS) or any(x in cid for x in _SUB_CODEC_IDS):
            return True
    return False


def _parse_int(value) -> int:
    try:
        return int(re.findall(r"\d+", str(value))[0])
    except Exception:
        return 0


def _parse_duration(value) -> float:
    """Parse a duration expressed in seconds, ms, us or HH:MM:SS[.mmm]."""
    try:
        if value is None or value == '':
            return 0.0
        v = str(value).strip().replace('\u00a0', ' ')

        if ':' in v:
            parts = [float(p) for p in v.split(':')]
            if len(parts) == 3:
                return parts[0] * 3600 + parts[1] * 60 + parts[2]
            if len(parts) == 2:
                return parts[0] * 60 + parts[1]
            return 0.0

        if re.search(r'[a-zA-Z]', v):
            total = 0.0
            matched = False
            for amount, unit in re.findall(r'(\d+(?:\.\d+)?)\s*([a-zA-Z]+)', v):
                unit = unit.lower()
                amount = float(amount)
                if unit.startswith('h'):
                    total += amount * 3600
                elif unit.startswith('min') or unit == 'm':
                    total += amount * 60
                elif unit == 'ms':
                    total += amount / 1000
                elif unit == 'us':
                    total += amount / 1_000_000
                elif unit.startswith('s'):
                    total += amount
                else:
                    continue
                matched = True
            return total if matched else 0.0

        f = float(v)
        if f < 0:
            return 0.0
        if f > 86_400_000:      # > 1 day in ms -> microseconds
            return f / 1_000_000
        if f > 86_400:          # > 1 day in s  -> milliseconds
            return f / 1_000
        return f
    except Exception:
        return 0.0


def _fmt_duration(s: float) -> str:
    s = max(0, int(s))
    return f"{s//3600:02}:{(s%3600)//60:02}:{s%60:02}"


def _fmt_size(num_bytes) -> str:
    """Human readable file size (Telegram reports bytes)."""
    try:
        size = float(num_bytes or 0)
    except (TypeError, ValueError):
        return 'Unknown'
    if size <= 0:
        return 'Unknown'
    for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
        if size < 1024 or unit == 'TB':
            return f"{size:.2f} {unit}"
        size /= 1024
    return 'Unknown'


def _fmt_bitrate(bps) -> str:
    try:
        v = float(bps or 0)
    except (TypeError, ValueError):
        return 'Unknown'
    if v <= 0:
        return 'Unknown'
    if v >= 1_000_000:
        return f"{v / 1_000_000:.2f} Mb/s"
    return f"{v / 1000:.0f} kb/s"


def _fmt_fps(fps) -> str:
    try:
        v = float(fps or 0)
    except (TypeError, ValueError):
        return ''
    if v <= 0:
        return ''
    return f"{v:.3f}".rstrip('0').rstrip('.') + ' fps'


def _which(binary: str) -> bool:
    return shutil.which(binary) is not None


async def _run_cmd(args: list[str], timeout: float) -> Optional[bytes]:
    """Run a probe binary, returning stdout or None on any failure."""
    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        if proc.returncode not in (0, None) and not stdout:
            msg = (stderr or b'').decode(errors='replace').strip()[:200]
            logger.debug(f"{args[0]} exited {proc.returncode}: {msg}")
        return stdout
    except asyncio.TimeoutError:
        logger.warning(f"{args[0]} timed out after {timeout}s")
        if proc:
            try:
                proc.kill()
                await proc.wait()
            except ProcessLookupError:
                pass
        return None
    except FileNotFoundError:
        logger.error(f"{args[0]} is not installed - metadata will be incomplete")
        return None
    except Exception as e:
        logger.warning(f"{args[0]} failed: {e}")
        return None


async def _run_cmd_full(args: list[str], timeout: float) -> tuple[int, str, str]:
    """Run a command and return ``(returncode, stdout_text, stderr_text)``."""
    proc = None
    try:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        rc = proc.returncode if proc.returncode is not None else 0
        return (
            rc,
            (stdout or b"").decode(errors="replace").strip(),
            (stderr or b"").decode(errors="replace").strip(),
        )
    except asyncio.TimeoutError:
        if proc:
            try:
                proc.kill()
                await proc.wait()
            except ProcessLookupError:
                pass
        return (-1, "", f"{args[0]} timed out after {timeout}s")
    except Exception as e:
        return (-1, "", str(e))


async def _run_mediainfo(path: str, parse_speed: str = "0.5") -> dict:
    """Run mediainfo on a (possibly truncated) file."""
    out = await _run_cmd(
        [
            'mediainfo',
            f'--ParseSpeed={parse_speed}',
            '--Language=raw',
            '--Full',
            '--Output=JSON',
            path,
        ],
        PROBE_TIMEOUT,
    )
    if not out:
        return {}
    try:
        return json.loads(out.decode(errors='replace') or '{}')
    except json.JSONDecodeError as e:
        logger.warning(f"mediainfo returned invalid JSON: {e}")
        return {}


async def _run_ffprobe_full(path: str) -> dict:
    """Run ffprobe with error tolerance so truncated files still report data."""
    out = await _run_cmd(
        [
            'ffprobe',
            '-v', 'error',
            '-err_detect', 'ignore_err',
            '-analyzeduration', '20000000',
            '-probesize', '20000000',
            '-show_streams', '-show_format',
            '-of', 'json',
            path,
        ],
        PROBE_TIMEOUT,
    )
    if not out:
        return {}
    try:
        return json.loads(out.decode(errors='replace') or '{}')
    except json.JSONDecodeError as e:
        logger.warning(f"ffprobe returned invalid JSON: {e}")
        return {}


def _fps_from_rate(rate: str) -> float:
    """Convert ffprobe's ``num/den`` frame rate into a float."""
    try:
        if not rate or rate in ('0/0', 'N/A'):
            return 0.0
        if '/' in str(rate):
            num, den = str(rate).split('/', 1)
            den = float(den)
            return float(num) / den if den else 0.0
        return float(rate)
    except (ValueError, ZeroDivisionError):
        return 0.0


def _blank_info() -> dict:
    """Shape of the metadata dictionary passed around the caption builders."""
    return {
        'duration': 0.0,
        'width': None,
        'height': None,
        'codec': '',
        'codec_name': '',
        'bit_depth': '',
        'hdr': '',
        'transfer': '',
        'fps': 0.0,
        'bitrate': 0,
        'container': '',
        'audio': '',
        'audio_codec': '',
        'audio_channels': '',
        'subtitle': '',
        'audio_langs': [],
        'sub_langs': [],
        'has_sub': False,
        'has_video': False,
        'source': '',
    }


def _parse_ffprobe(data: dict) -> dict:
    """Convert ffprobe JSON into the shared metadata dictionary."""
    info = _blank_info()
    info['source'] = 'ffprobe'

    streams = data.get('streams') or []
    fmt = data.get('format') or {}

    info['duration'] = _parse_duration(fmt.get('duration'))
    info['bitrate'] = _parse_int(fmt.get('bit_rate'))
    info['container'] = (fmt.get('format_name') or '').split(',')[0]

    audio_langs: list[str] = []
    sub_langs: list[str] = []

    for s in streams:
        if not isinstance(s, dict):
            continue
        ctype = (s.get('codec_type') or '').lower()
        tags = s.get('tags') or {}
        raw_lang_tag = (
            tags.get('language') or tags.get('LANGUAGE') or tags.get('lang') or ''
        )
        raw_title_tag = tags.get('title') or tags.get('TITLE') or ''

        if ctype == 'video':
            if (s.get('codec_name') or '').lower() in ('mjpeg', 'png', 'bmp', 'gif', 'webp'):
                continue
            info['has_video'] = True
            if not info['width']:
                info['width'] = s.get('width') or s.get('coded_width')
            if not info['height']:
                info['height'] = s.get('height') or s.get('coded_height')

            codec_raw = (s.get('codec_name') or '').lower()
            info['codec_name'] = codec_raw
            if 'hevc' in codec_raw or 'h265' in codec_raw:   info['codec'] = 'hevc'
            elif 'h264' in codec_raw or 'avc' in codec_raw:  info['codec'] = 'avc'
            elif 'av1' in codec_raw:                          info['codec'] = 'av1'
            elif 'vp9' in codec_raw:                          info['codec'] = 'vp9'
            elif 'vp8' in codec_raw:                          info['codec'] = 'vp8'
            elif 'mpeg4' in codec_raw or 'xvid' in codec_raw: info['codec'] = 'mpeg4'
            elif 'mpeg2' in codec_raw:                        info['codec'] = 'mpeg-2'
            elif codec_raw:                                   info['codec'] = codec_raw

            pix = (s.get('pix_fmt') or '').lower()
            depth = ''
            if 'p10' in pix or '10le' in pix or '10be' in pix:
                depth = '10'
            elif 'p12' in pix or '12le' in pix or '12be' in pix:
                depth = '12'
            else:
                bps = str(s.get('bits_per_raw_sample')
                          or s.get('bits_per_coded_sample') or '')
                if bps.isdigit() and bps != '0':
                    depth = bps
            if depth:
                info['bit_depth'] = depth

            ct = (s.get('color_transfer') or '').lower()
            cs = (s.get('color_space') or s.get('color_primaries') or '').lower()
            info['transfer'] = ct
            for sd in (s.get('side_data_list') or []):
                stype = str(sd.get('side_data_type', '')).lower()
                if 'dovi' in stype or 'dolby vision' in stype:
                    info['hdr'] = 'Dolby Vision'
                    break
            if not info['hdr']:
                if any(x in ct for x in ('smpte2084', 'arib-std-b67', 'smpte428')):
                    info['hdr'] = 'HDR'
                elif 'bt2020' in cs:
                    info['hdr'] = 'HDR'

            if not info['fps']:
                info['fps'] = _fps_from_rate(
                    s.get('avg_frame_rate') or s.get('r_frame_rate') or ''
                )
            if not info['duration']:
                info['duration'] = _parse_duration(
                    s.get('duration') or tags.get('DURATION') or tags.get('duration')
                )
            if not info['bitrate']:
                info['bitrate'] = _parse_int(s.get('bit_rate'))

        elif ctype == 'audio':
            audio_langs.append(_resolve_audio_track_lang(raw_lang_tag, raw_title_tag))
            if not info['audio_codec']:
                info['audio_codec'] = (s.get('codec_name') or '').upper()
                ch = s.get('channels')
                if ch:
                    info['audio_channels'] = {1: 'Mono', 2: 'Stereo',
                                              6: '5.1', 8: '7.1'}.get(ch, f"{ch}ch")
            if not info['duration']:
                info['duration'] = _parse_duration(s.get('duration'))
            if not info['bitrate']:
                info['bitrate'] = _parse_int(s.get('bit_rate'))

        elif ctype == 'subtitle':
            info['has_sub'] = True
            sub_lang = _normalize_sub_lang(raw_lang_tag, raw_title_tag)
            if sub_lang:
                sub_langs.append(sub_lang)

    info['audio_langs'] = _dedupe(audio_langs)
    info['sub_langs'] = _dedupe(sub_langs)
    info['audio'] = ', '.join(info['audio_langs']) if info['audio_langs'] else ''
    info['subtitle'] = ', '.join(info['sub_langs']) if info['sub_langs'] else (
        'ESUB' if info['has_sub'] else ''
    )
    return info


def _dedupe(items: list[str]) -> list[str]:
    """Order-preserving de-duplication (keeps the original track order)."""
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        if item and item not in seen:
            seen.add(item)
            out.append(item)
    return out


def _to_float(value) -> float:
    """Best-effort float conversion for mediainfo string fields."""
    try:
        if value is None or value == '':
            return 0.0
        m = re.search(r'-?\d+(?:\.\d+)?', str(value).replace(',', '.'))
        return float(m.group()) if m else 0.0
    except (TypeError, ValueError):
        return 0.0


def _mi_num(track: dict, *fields: str) -> int:
    """Read the first positive integer out of the given mediainfo fields."""
    for field in fields:
        raw = str(track.get(field, '') or '')
        raw = re.split(r'[A-Za-z]', raw, maxsplit=1)[0]
        raw = re.sub(r'[\s\u00a0\u202f,]', '', raw).split('.')[0]
        if raw.isdigit() and int(raw) > 0:
            return int(raw)
    return 0


def _mi_tracks(mi_data: dict) -> list:
    """Normalise mediainfo's ``media.track`` into a list of dicts."""
    media = mi_data.get('media')
    if not isinstance(media, dict):
        return []
    tracks = media.get('track')
    if isinstance(tracks, dict):
        return [tracks]
    if isinstance(tracks, list):
        return [t for t in tracks if isinstance(t, dict)]
    return []


def _parse_tracks(tracks: list) -> dict:
    """Convert mediainfo tracks into the shared metadata dictionary."""
    info = _blank_info()
    info['source'] = 'mediainfo'

    audio_langs: list[str] = []
    sub_langs: list[str] = []
    video_seen = False

    for track in tracks:
        if not isinstance(track, dict):
            continue
        t = (track.get('@type', '') or '').strip().lower()

        if t == 'general':
            if not info['duration']:
                info['duration'] = _parse_duration(track.get('Duration'))
            if not info['bitrate']:
                info['bitrate'] = _parse_int(
                    track.get('OverallBitRate') or track.get('BitRate')
                )
            if not info['container']:
                info['container'] = (track.get('Format', '') or '')
            if _parse_int(track.get('TextCount')) > 0:
                info['has_sub'] = True
            if _parse_int(track.get('VideoCount')) > 0:
                info['has_video'] = True

        elif t == 'audio':
            raw_lang = (
                track.get('Language')
                or track.get('Language_String')
                or track.get('Language_String3')
                or ''
            )
            raw_title = track.get('Title') or ''
            audio_langs.append(_resolve_audio_track_lang(str(raw_lang), str(raw_title)))
            if not info['audio_codec']:
                info['audio_codec'] = (track.get('Format', '') or '').upper()
                ch = _parse_int(track.get('Channels'))
                if ch:
                    info['audio_channels'] = {1: 'Mono', 2: 'Stereo',
                                              6: '5.1', 8: '7.1'}.get(ch, f"{ch}ch")
            if not info['duration']:
                info['duration'] = _parse_duration(track.get('Duration'))
            if not info['bitrate']:
                info['bitrate'] = _parse_int(track.get('BitRate'))

        elif t in ('text', 'subtitle'):
            info['has_sub'] = True
            sub_lang = _normalize_sub_lang(
                str(track.get('Language') or track.get('Language_String') or ''),
                str(track.get('Title') or ''),
            )
            if sub_lang:
                sub_langs.append(sub_lang)

        elif t == 'menu':
            continue

        elif _is_video_track(track):
            info['has_video'] = True
            if video_seen:
                continue
            video_seen = True

            info['height'] = _mi_num(track, 'Height', 'Encoded_Height', 'Sampled_Height') or None
            info['width'] = _mi_num(track, 'Width', 'Encoded_Width', 'Sampled_Width') or None

            info['codec'] = (track.get('Format', '') or '').lower()
            info['codec_name'] = track.get('Format', '') or ''
            info['bit_depth'] = str(track.get('BitDepth', '') or '')
            info['transfer'] = (track.get('transfer_characteristics', '') or
                                track.get('TransferCharacteristics', '') or '').lower()
            info['hdr'] = (track.get('HDR_Format', '') or
                           track.get('HDR_Format_Commercial', '') or
                           track.get('HDR_Format_Compatibility', '') or '')
            info['fps'] = _to_float(track.get('FrameRate'))

            if not info['duration']:
                info['duration'] = _parse_duration(track.get('Duration'))
            if not info['bitrate']:
                info['bitrate'] = _parse_int(track.get('BitRate'))

            if not info['hdr']:
                blob = ' '.join(
                    str(track.get(k, '')) for k in (
                        'HDR_Format', 'HDR_Format_Commercial', 'HDR_Format_Profile',
                        'Format_Profile', 'colour_primaries', 'MasteringDisplay_Luminance',
                    )
                ).lower()
                if 'dolby vision' in blob or 'dvhe' in blob or 'dvh1' in blob:
                    info['hdr'] = 'Dolby Vision'
                elif 'hdr' in blob or 'bt.2020' in blob or 'bt2020' in blob:
                    info['hdr'] = 'HDR'

    if not info['has_sub'] and _has_subtitles(tracks):
        info['has_sub'] = True

    info['audio_langs'] = _dedupe(audio_langs)
    info['sub_langs'] = _dedupe(sub_langs)
    info['audio'] = ', '.join(info['audio_langs']) if info['audio_langs'] else ''
    info['subtitle'] = ', '.join(info['sub_langs']) if info['sub_langs'] else (
        'ESUB' if info['has_sub'] else ''
    )
    return info


def _merge_info(primary: dict, secondary: dict) -> dict:
    """Merge two metadata dicts, preferring whichever value is actually set."""
    out = _blank_info()
    for key in out:
        a, b = primary.get(key), secondary.get(key)
        out[key] = a if a not in (None, '', 0, 0.0, [], False) else b
    for key in ('audio_langs', 'sub_langs'):
        if len(secondary.get(key) or []) > len(primary.get(key) or []):
            out[key] = secondary[key]
    out['audio'] = ', '.join(out['audio_langs']) if out['audio_langs'] else (
        primary.get('audio') or secondary.get('audio') or ''
    )
    if out['sub_langs']:
        out['subtitle'] = ', '.join(out['sub_langs'])
    elif out['has_sub']:
        out['subtitle'] = 'ESUB'
    else:
        out['subtitle'] = ''
    out['source'] = '+'.join(
        s for s in (primary.get('source'), secondary.get('source')) if s
    )
    return out


_AUDIO_EXTS = {
    '.mp3', '.flac', '.wav', '.ogg', '.opus', '.m4a', '.aac', '.wma', '.alac', '.aiff', '.ac3', '.dts', '.eac3'
}


def _is_audio_media(media) -> bool:
    """True when the Telegram media object is an audio-only file."""
    if media is None:
        return False
    mime = (getattr(media, 'mime_type', '') or '').lower()
    if mime.startswith('audio/'):
        return True
    fname = (getattr(media, 'file_name', '') or '').lower()
    if any(fname.endswith(ext) for ext in _AUDIO_EXTS):
        return True
    return False


def _info_is_useful(info: dict, media=None) -> bool:
    """A probe is useful when it found picture dimensions, or audio codec+duration for audio-only files."""
    if info.get('width') or info.get('height'):
        return True
    if not info.get('has_video') and info.get('audio_codec') and info.get('duration'):
        return True
    if _is_audio_media(media) and (info.get('audio_codec') or info.get('duration')):
        return True
    return False


async def _probe(path: str, *, deep: bool = False, media=None) -> dict:
    """Probe a local (possibly partial) file with mediainfo and ffprobe."""
    mi_data = await _run_mediainfo(path, "1.0" if deep else "0.5")
    mi = _parse_tracks(_mi_tracks(mi_data))

    fp = _blank_info()
    if not _info_is_useful(mi, media) or not mi.get('duration') or not mi.get('audio_langs'):
        fp_data = await _run_ffprobe_full(path)
        if fp_data:
            fp = _parse_ffprobe(fp_data)

    if _info_is_useful(mi, media) and not _info_is_useful(fp, media):
        merged = mi
    elif _info_is_useful(fp, media) and not _info_is_useful(mi, media):
        merged = fp
    else:
        merged = _merge_info(mi, fp)
    return merged


def _safe_title(message, media) -> str:
    """Return an HTML-escaped title safe for caption rendering."""
    title = message.caption or getattr(media, 'file_name', None) or 'Video'
    return html.escape(str(title))


_HTML_TAG_RE = re.compile(r'<(/?)([a-zA-Z0-9]+)(?:\s[^>]*)?>')
_VOID_TAGS = {'br', 'hr', 'img'}


def _trim_caption(caption: str, limit: int = TG_CAPTION_LIMIT) -> str:
    """Shorten a caption to Telegram's limit while keeping HTML entities and tags valid."""
    if len(caption) <= limit:
        return caption

    budget = max(0, limit - 48)
    cut = caption[:budget]

    lt_idx = cut.rfind('<')
    if lt_idx != -1 and '>' not in cut[lt_idx:]:
        cut = cut[:lt_idx]

    amp_idx = cut.rfind('&')
    if amp_idx != -1 and amp_idx > len(cut) - 12 and ';' not in cut[amp_idx:]:
        cut = cut[:amp_idx]

    cut = cut.rstrip()

    open_tags: list[str] = []
    for match in _HTML_TAG_RE.finditer(cut):
        is_close, tag_name = match.group(1), match.group(2).lower()
        if tag_name in _VOID_TAGS:
            continue
        if is_close:
            if tag_name in open_tags:
                while open_tags:
                    popped = open_tags.pop()
                    if popped == tag_name:
                        break
        else:
            open_tags.append(tag_name)

    closing = ''.join(f"</{tag}>" for tag in reversed(open_tags))
    if len(cut) + len(closing) > limit:
        cut = cut[: max(0, limit - len(closing))].rstrip()
    return cut + closing


def _render_caption(**values) -> str:
    """Apply CAPTION_TEMPLATE, tolerating unknown placeholders."""
    defaults = {
        'title': '', 'video_line': 'Unknown', 'duration': 'Unknown',
        'audio': 'Original Audio', 'subtitle': 'No Esubs',
        'size': 'Unknown', 'bitrate': 'Unknown', 'fps': '',
        'resolution': '', 'codec': '', 'container': '', 'filename': '',
    }
    defaults.update({k: v for k, v in values.items() if v is not None})
    try:
        caption = CAPTION_TEMPLATE.format(**defaults)
    except (KeyError, IndexError, ValueError) as e:
        logger.error(f"CAPTION_TEMPLATE is invalid ({e}); using the built-in layout")
        caption = (
            f"<b>{defaults['title']}</b>\n\n"
            f"\U0001F39E <b>{defaults['video_line']}</b> | "
            f"\u23f3 <b>{defaults['duration']}</b>\n"
            f"\U0001F50A <b>{defaults['audio']}</b>\n"
        )
    return _trim_caption(caption)


def _video_line(info: dict) -> str:
    """Build the '1080p HEVC 10bit HDR' summary (or audio summary for audio-only files)."""
    res_val = info.get('height') or info.get('width') or 0
    quality = get_standard_resolution(res_val)
    fmt = get_video_format(
        info.get('codec', ''),
        info.get('transfer', ''),
        info.get('hdr', ''),
        info.get('bit_depth', ''),
    )
    line = ' '.join(filter(None, [quality, fmt]))
    if line:
        return line
    if info.get('audio_codec'):
        parts = [info['audio_codec']]
        if info.get('audio_channels'):
            parts.append(info['audio_channels'])
        if info.get('bitrate'):
            parts.append(_fmt_bitrate(info['bitrate']))
        return ' '.join(parts)
    return 'Unknown'


def _build_caption(message, media, info: dict) -> str:
    """Caption built from real probe results."""
    size = getattr(media, 'file_size', None)
    width, height = info.get('width'), info.get('height')
    file_name = getattr(media, 'file_name', '') or ''
    return _render_caption(
        title=_safe_title(message, media),
        video_line=html.escape(_video_line(info)),
        duration=_fmt_duration(info['duration']) if info.get('duration') else 'Unknown',
        audio=html.escape(info.get('audio') or 'Original Audio'),
        subtitle=html.escape(info.get('subtitle') or 'No Esubs'),
        size=_fmt_size(size),
        bitrate=_fmt_bitrate(info.get('bitrate')),
        fps=_fmt_fps(info.get('fps')),
        resolution=f"{width}x{height}" if width and height else '',
        codec=html.escape(info.get('codec_name') or info.get('codec') or ''),
        container=html.escape(info.get('container') or ''),
        filename=html.escape(file_name),
    )


def _clean_tokens_text(text: str) -> str:
    """Replace filename separators (dots, underscores, brackets, hyphens) with spaces."""
    return re.sub(r'[._\-\[\]()]+', ' ', text or '')


_RES_TOKEN_RE = re.compile(
    r'(?<![a-zA-Z0-9])(240|360|480|540|576|720|1080|1440|2160|4320)\s*[pi](?![a-zA-Z0-9])',
    re.IGNORECASE,
)
_DIM_TOKEN_RE = re.compile(
    r'(?<![a-zA-Z0-9])(\d{3,4})\s*[xX\u00d7]\s*(\d{3,4})(?![a-zA-Z0-9])'
)


def _extract_res_from_text(text: str) -> Optional[str]:
    """Parse a resolution out of a file name or caption."""
    if not text:
        return None
    norm = _clean_tokens_text(text)
    match = _RES_TOKEN_RE.search(norm)
    if match:
        return f"{match.group(1)}p"
    match = _DIM_TOKEN_RE.search(norm)
    if match:
        return get_standard_resolution(int(match.group(2)))
    low = f" {norm.lower()} "
    if ' 8k ' in low:
        return "4320p"
    if ' 4k ' in low or ' uhd ' in low or ' 2160 ' in low:
        return "2160p"
    if ' fhd ' in low or ' full hd ' in low:
        return "1080p"
    if ' hd rip ' in low or ' hdrip ' in low or ' hd ' in low:
        return "720p"
    return None


_CODEC_TOKENS = (
    ('HEVC', ('hevc', 'x265', 'h265', 'h.265')),
    ('AV1', ('av1',)),
    ('x264', ('x264', 'h264', 'h.264', 'avc')),
    ('VP9', ('vp9',)),
    ('MPEG4', ('xvid', 'divx')),
)


def _extract_codec_from_text(text: str) -> str:
    low = (text or '').lower()
    for label, tokens in _CODEC_TOKENS:
        if any(tok in low for tok in tokens):
            return label
    return ''


def _extract_extras_from_text(text: str) -> list[str]:
    """Pull bit depth / HDR hints out of a release name."""
    norm = _clean_tokens_text(text).lower()
    extras: list[str] = []
    if '10bit' in norm or '10 bit' in norm or 'hi10' in norm:
        extras.append('10bit')
    elif '12bit' in norm or '12 bit' in norm:
        extras.append('12bit')
    if 'dolby vision' in norm or re.search(r'(?<![a-z0-9])dv(?![a-z0-9])', norm):
        extras.append('DV')
    elif 'hdr10+' in (text or '').lower() or 'hdr10plus' in norm:
        extras.append('HDR10+')
    elif re.search(r'(?<![a-z0-9])hdr(?:\d+)?(?![a-z0-9])', norm):
        extras.append('HDR')
    return extras


def _langs_from_text(text: str) -> list[str]:
    """Detect audio languages named in a release title in order of appearance."""
    norm = _clean_tokens_text(text).lower()
    matches: list[tuple[int, str]] = []
    for name in _KNOWN_LANG_NAMES:
        if name in ('Original Audio', 'Multi Audio'):
            continue
        m = re.search(rf'(?<![a-z0-9]){re.escape(name.lower())}(?![a-z0-9])', norm)
        if m:
            matches.append((m.start(), name))
    matches.sort(key=lambda item: item[0])
    found = [name for _, name in matches]
    if re.search(r'(?<![a-z0-9])dual\s*audio(?![a-z0-9])', norm) and len(found) < 2:
        found.append('Dual Audio')
    elif re.search(r'(?<![a-z0-9])multi\s*audio(?![a-z0-9])', norm) and not found:
        found.append('Multi Audio')
    return _dedupe(found)


def _build_caption_from_tg(message, media) -> str:
    """Fallback caption built from Telegram metadata plus filename parsing."""
    raw_title = message.caption or getattr(media, 'file_name', None) or 'Video'
    file_name = getattr(media, 'file_name', '') or ''
    text_pool = f"{raw_title} {file_name}"
    norm_pool = _clean_tokens_text(text_pool)

    quality = _extract_res_from_text(text_pool)
    if not quality:
        height = getattr(media, 'height', None)
        width = getattr(media, 'width', None)
        quality = get_standard_resolution(height or width or 0)

    parts = [quality] if quality else []
    codec = _extract_codec_from_text(text_pool)
    if codec:
        parts.append(codec)
    parts.extend(_extract_extras_from_text(text_pool))
    video_line = ' '.join(parts) or 'Unknown'

    tg_dur = getattr(media, 'duration', None)
    langs = _langs_from_text(text_pool)
    has_sub = bool(
        re.search(
            r'(?<![a-zA-Z0-9])(esub|esubs|msub|msubs|subbed|sub)(?![a-zA-Z0-9])',
            norm_pool,
            re.IGNORECASE,
        )
    )

    return _render_caption(
        title=html.escape(str(raw_title)),
        video_line=html.escape(video_line),
        duration=_fmt_duration(tg_dur) if tg_dur else 'Unknown',
        audio=html.escape(', '.join(langs) if langs else 'Original Audio'),
        subtitle='ESUB' if has_sub else 'No Esubs',
        size=_fmt_size(getattr(media, 'file_size', None)),
        bitrate='Unknown',
        fps='',
        resolution=(
            f"{getattr(media, 'width', 0)}x{getattr(media, 'height', 0)}"
            if getattr(media, 'width', None) and getattr(media, 'height', None) else ''
        ),
        codec=html.escape(codec),
        container=html.escape(os.path.splitext(file_name)[1].lstrip('.').upper() or ''),
        filename=html.escape(file_name),
    )


def caption_has_media_info(caption: str) -> bool:
    """Detect a caption this bot (or a similar one) has already written."""
    if not caption:
        return False
    hits = (
        '\U0001F3AC' in caption or '\U0001F39E' in caption or '汐' in caption,
        bool(re.search(r'(\u23f3|竢ｳ)\s*<?b?>?\s*(\d{1,2}:\d{2}|Unknown)', caption)),
        '\U0001F50A' in caption or '矧' in caption,
        '\U0001F4AC' in caption or '町' in caption,
        bool(
            re.search(
                r'\b\d{3,4}p\b.*\b(HEVC|x264|AV1|VP9|VP8|MPEG4|MPEG2|VC1|ProRes|WMV)\b',
                caption,
                re.DOTALL | re.IGNORECASE,
            )
        ),
        bool(re.search(r'\b\d{2}:\d{2}:\d{2}\b', caption)) and (
            '\n' in caption
            and any(
                tok in caption
                for tok in ('Original Audio', 'Multi Audio', 'No Esubs', 'ESUB', 'Audio:', 'Subtitles:')
            )
        ),
    )
    return sum(hits) >= 2


_STREAM_STEPS = [
    ("32 KB", 32 * 1024),
    ("1 MB", 1 * 1024 * 1024),
    ("4 MB", 4 * 1024 * 1024),
    ("10 MB", 10 * 1024 * 1024),
]


def _tmp_path(name: str) -> str:
    """Absolute path inside TMP_DIR for a temporary probe file."""
    try:
        os.makedirs(TMP_DIR, exist_ok=True)
    except OSError:
        return name
    return os.path.join(TMP_DIR, name)


async def _cleanup(path: Optional[str]):
    if path and os.path.exists(path):
        try:
            await aioremove(path)
        except OSError as e:
            logger.debug(f"could not remove {path}: {e}")


def _media_dc(media) -> Optional[int]:
    """Datacenter that hosts a media object (None when undecodable)."""
    try:
        return FileId.decode(media.file_id).dc_id
    except Exception:
        return None


async def _refresh_media(message):
    """Re-fetch a message to obtain a fresh file_reference."""
    try:
        fresh = await app.get_messages(message.chat.id, message.id)
        if fresh:
            return _media_of(fresh)
    except Exception as e:
        logger.warning(f"could not refresh file reference: {e}")
    return None


async def _write_range(media, offset: int, length: int, path: str, mode: str) -> int:
    """Write one byte range of a media file to disk. Returns bytes written."""
    written = 0
    file_id = FileId.decode(media.file_id)
    async with stream_semaphore:
        async with aiopen(path, mode) as f:
            async for chunk in dc_stream.iter_range(app, file_id, offset, length):
                if not chunk:
                    break
                piece = chunk[: length - written] if written + len(chunk) > length else chunk
                await f.write(piece)
                written += len(piece)
                if written >= length:
                    break
    return written


async def _write_range_fallback(media, length: int, path: str) -> int:
    """Header-only download through client.stream_media (used for CDN-hosted files)."""
    written = 0
    async with stream_semaphore:
        async with aiopen(path, 'wb') as f:
            async for chunk in app.stream_media(media):
                if not chunk:
                    break
                piece = chunk[: length - written]
                await f.write(piece)
                written += len(piece)
                if written >= length:
                    break
    return written


async def _fetch_range(
    message, media, offset: int, length: int, path: str, mode: str = 'wb'
) -> tuple[int, object]:
    """Fetch a byte range with retries, flood handling and reference refresh."""
    dc_id = _media_dc(media)
    attempts = max(1, STREAM_RETRIES)
    base_size = os.path.getsize(path) if mode == 'ab' and os.path.exists(path) else 0

    def _rollback():
        """Drop a half-written range so a retry cannot duplicate bytes."""
        try:
            if os.path.exists(path) and os.path.getsize(path) != base_size:
                with open(path, 'r+b') as fh:
                    fh.truncate(base_size)
        except OSError as e:
            logger.debug(f"could not roll back {path}: {e}")

    for attempt in range(attempts):
        left = dc_stream.flood_left(dc_id)
        if left:
            logger.warning(f"DC{dc_id}: flood wait active for {left}s more, skipping range")
            _rollback()
            return 0, media
        try:
            written = await asyncio.wait_for(
                _write_range(media, offset, length, path, mode),
                timeout=STREAM_TIMEOUT,
            )
            if written:
                return written, media
            logger.warning(
                f"DC{dc_id}: empty response for range {offset}+{length} "
                f"(attempt {attempt + 1}/{attempts})"
            )
            _rollback()
        except FloodWait as e:
            _runtime["flood"] += 1
            logger.warning(f"DC{dc_id}: FloodWait {e.value}s while streaming")
            _rollback()
            return 0, media
        except dc_stream.CdnRedirect:
            if offset:
                logger.warning("CDN-hosted file: cannot read a tail range, skipping")
                _rollback()
                return 0, media
            logger.info("File is CDN hosted, using client.stream_media downloader")
            try:
                written = await asyncio.wait_for(
                    _write_range_fallback(media, length, path),
                    timeout=STREAM_TIMEOUT,
                )
                return written, media
            except Exception as e:
                logger.warning(f"CDN fallback failed: {e}")
                _rollback()
                return 0, media
        except (FileReferenceExpired, FileReferenceEmpty):
            logger.info("File reference expired, refreshing the message")
            _rollback()
            fresh = await _refresh_media(message)
            if fresh is None:
                return 0, media
            media = fresh
            dc_id = _media_dc(media)
        except asyncio.TimeoutError:
            logger.warning(
                f"DC{dc_id}: range {offset}+{length} timed out after {STREAM_TIMEOUT}s"
            )
            _rollback()
        except RPCError as e:
            logger.warning(f"DC{dc_id}: RPC error while streaming: {e}")
            _rollback()
            await asyncio.sleep(1 + attempt)
        except Exception as e:
            logger.warning(f"DC{dc_id}: stream failed: {e}")
            _rollback()
            await asyncio.sleep(1 + attempt)

    _runtime["probe_fail"] += 1
    return 0, media


def _media_of(message):
    """The media object we can probe on a message, if any."""
    return (
        getattr(message, 'video', None)
        or getattr(message, 'document', None)
        or getattr(message, 'animation', None)
        or getattr(message, 'audio', None)
        or getattr(message, 'video_note', None)
        or getattr(message, 'voice', None)
    )


def _get_forwarded_chat(message):
    """Extract forwarded channel/chat from either classic or Bot API 7+ WZGram fields."""
    fwd = getattr(message, "forward_from_chat", None)
    if fwd is not None:
        return fwd
    origin = getattr(message, "forward_origin", None)
    if origin is not None:
        return getattr(origin, "chat", None)
    return None


async def probe_media(message, media, progress=None) -> tuple[dict, bool]:
    """Probe a Telegram media object without downloading the whole file."""
    file_size = getattr(media, 'file_size', 0) or 0
    dc_id = _media_dc(media)
    home_hint = f"DC{dc_id}" if dc_id else "unknown DC"
    tmp = _tmp_path(f"probe_{message.chat.id}_{message.id}_{uuid.uuid4().hex[:8]}.bin")
    best: dict = _blank_info()

    try:
        total = 0
        for index, (label, size) in enumerate(_STREAM_STEPS):
            target = min(size, file_size) if file_size else size
            if total >= target:
                continue

            if progress:
                await progress(f"\U0001F4E6 Streaming {label} \u2022 {home_hint}\u2026")

            written, media = await _fetch_range(
                message, media, total, target - total, tmp,
                'wb' if total == 0 else 'ab',
            )
            if not written:
                if dc_stream.flood_left(_media_dc(media)):
                    break
                if total == 0:
                    continue
                break

            total += written
            info = await _probe(tmp, deep=index >= len(_STREAM_STEPS) - 1, media=media)
            if _info_is_useful(info, media):
                best = info
                if info.get('duration'):
                    logger.info(
                        f"probe ok via {info.get('source')} after {_fmt_size(total)} "
                        f"({home_hint}, msg {message.id})"
                    )
                    return info, True
            if file_size and total >= file_size:
                break
            if total % dc_stream.ALIGN:
                break

        # Header data alone was not enough: check the 4 KiB-aligned tail through EOF.
        if ENABLE_TAIL_PROBE and file_size > TAIL_SIZE * 2 and not (
            _info_is_useful(best, media) and best.get('duration')
        ):
            if not dc_stream.flood_left(_media_dc(media)):
                if progress:
                    await progress("\U0001F50E Inspecting container tail index\u2026")
                tail = _tmp_path(f"tail_{message.id}_{uuid.uuid4().hex[:8]}.bin")
                try:
                    raw_tail_offset = max(0, file_size - TAIL_SIZE)
                    tail_offset = raw_tail_offset - (raw_tail_offset % dc_stream.ALIGN)
                    tail_length = file_size - tail_offset
                    written, media = await _fetch_range(
                        message, media, tail_offset,
                        tail_length, tail, 'wb',
                    )
                    if written:
                        tail_info = await _probe(tail, deep=True, media=media)
                        if _info_is_useful(tail_info, media):
                            best = (
                                _merge_info(best, tail_info)
                                if _info_is_useful(best, media) else tail_info
                            )
                        elif tail_info.get('duration') and _info_is_useful(best, media):
                            best['duration'] = best['duration'] or tail_info['duration']
                finally:
                    await _cleanup(tail)

        if _info_is_useful(best, media):
            logger.info(
                f"probe partial via {best.get('source')} ({home_hint}, msg {message.id})"
            )
            return best, True

        logger.warning(
            f"probe failed after {_fmt_size(total)} from {home_hint} (msg {message.id})"
        )
        return best, False
    finally:
        await _cleanup(tmp)


async def process_message(message, progress_msg=None) -> tuple[str, bool]:
    """Build the caption for a message. Returns ``(caption, deep_scan)``."""
    media = _media_of(message)
    if media is None:
        return _render_caption(title='Video'), False

    async def _update(text: str):
        if progress_msg:
            await _safe_edit(progress_msg, text, throttle=True)

    await _update("\u26a1 Starting media scan\u2026")

    try:
        info, deep = await probe_media(message, media, _update)
    except Exception as e:
        logger.error(f"probe crashed for msg {message.id}: {e}")
        info, deep = _blank_info(), False

    if deep:
        _runtime["deep"] += 1
        return _build_caption(message, media, info), True

    _runtime["fallback"] += 1
    await _update("\u26a0\ufe0f Deep scan unavailable \u2014 using Telegram metadata.")
    logger.warning(f"Falling back to Telegram metadata for msg {message.id}")
    return _build_caption_from_tg(message, media), False


async def _safe_edit(msg, text: str, parse_mode=None, reply_markup=None, *, throttle: bool = False):
    """Edit a message safely. Intermediate progress updates pass ``throttle=True``."""
    if not msg:
        return
    chat_id = getattr(getattr(msg, "chat", None), "id", 0) or 0
    key = (chat_id, msg.id)
    now = asyncio.get_event_loop().time()
    if throttle and key in _last_edit and now - _last_edit[key] < 1.7:
        return
    try:
        await msg.edit_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
        _last_edit[key] = now
    except MessageNotModified:
        pass
    except (MessageIdInvalid, MessageEmpty):
        pass
    except FloodWait as e:
        logger.warning(f"_safe_edit FloodWait: {e.value}s")
        _last_edit[key] = now + e.value
    except Exception as e:
        logger.warning(f"_safe_edit failed: {e}")


def _prune_caches():
    """Drop stale bookkeeping so long-running instances do not leak memory."""
    try:
        now = asyncio.get_event_loop().time()
    except RuntimeError:
        return

    for key, stamp in list(_last_edit.items()):
        if now - stamp > 300:
            _last_edit.pop(key, None)

    for cid, queue in list(channel_queues.items()):
        if queue or channel_workers.get(cid):
            continue
        lock = channel_locks.get(cid)
        if lock is not None and lock.locked():
            continue
        channel_queues.pop(cid, None)
        channel_workers.pop(cid, None)
        channel_locks.pop(cid, None)

    for cid, stamp in list(last_edit_time.items()):
        if now - stamp > 3600 and cid not in channel_queues:
            last_edit_time.pop(cid, None)

    now_wall = time.time()
    for uid, st in list(_admin_input_state.items()):
        if now_wall - st.get("ts", 0) > 600:
            _admin_input_state.pop(uid, None)


async def _edit_caption(message, caption: str) -> bool:
    """Apply a caption, trimming it if Telegram rejects the length."""
    try:
        await message.edit_caption(caption, parse_mode=ParseMode.HTML)
        return True
    except MediaCaptionTooLong:
        await message.edit_caption(_trim_caption(caption), parse_mode=ParseMode.HTML)
        return True


def _enqueue_channel_message(channel_id: int, message) -> bool:
    """Add a message to a channel's queue if not already present, and start worker if needed."""
    q = channel_queues[channel_id]
    if any(getattr(m, "id", None) == message.id for m in q):
        return False
    q.append(message)
    if not AUTO_CAPTION_PAUSED and not channel_workers[channel_id]:
        channel_workers[channel_id] = True
        asyncio.create_task(_channel_worker(channel_id))
    return True


def _resume_all_channel_workers():
    """Spawn workers for any channels with pending messages after unpausing."""
    if AUTO_CAPTION_PAUSED:
        return
    for cid, q in list(channel_queues.items()):
        if q and not channel_workers[cid]:
            channel_workers[cid] = True
            asyncio.create_task(_channel_worker(cid))


async def _process_channel_queue(channel_id: int):
    global EDIT_DELAY
    async with channel_semaphore:
        async with channel_locks[channel_id]:
            while channel_queues[channel_id] and not AUTO_CAPTION_PAUSED:
                if channel_id not in authorized_chats:
                    channel_queues[channel_id].clear()
                    break
                message = channel_queues[channel_id].pop(0)

                try:
                    caption, deep = await process_message(message)
                except Exception as e:
                    logger.error(f"Caption build failed for msg {message.id}: {e}")
                    await _report_error(channel_id, str(e))
                    continue

                now = asyncio.get_event_loop().time()
                last = last_edit_time.get(channel_id, 0)
                if now - last < EDIT_DELAY:
                    await asyncio.sleep(EDIT_DELAY - (now - last))
                try:
                    await _edit_caption(message, caption)
                    last_edit_time[channel_id] = asyncio.get_event_loop().time()
                    await save_last_id(channel_id, message.id)
                    await increment_stat(channel_id, deep=deep)
                    _reset_error_count(channel_id)
                except FloodWait as e:
                    EDIT_DELAY = round(min(60.0, max(EDIT_DELAY, e.value / 10 + 1)), 1)
                    await save_runtime_settings(edit_delay=EDIT_DELAY)
                    logger.warning(
                        f"Edit FloodWait {e.value}s on {channel_id}; "
                        f"delay is now {EDIT_DELAY:.1f}s (saved to MongoDB)"
                    )
                    await asyncio.sleep(e.value + 1)
                    try:
                        await _edit_caption(message, caption)
                        last_edit_time[channel_id] = asyncio.get_event_loop().time()
                        await save_last_id(channel_id, message.id)
                        await increment_stat(channel_id, deep=deep)
                        _reset_error_count(channel_id)
                    except (MessageIdInvalid, MessageEmpty):
                        logger.warning("Edit skipped: message deleted or invalid.")
                    except MessageNotModified:
                        pass
                    except Exception as err:
                        logger.error(f"Retry edit failed: {err}")
                        await enqueue_retry(channel_id, message.id, caption)
                        await _report_error(channel_id, str(err))
                except (MessageIdInvalid, MessageEmpty):
                    logger.warning("Edit skipped: message deleted or invalid.")
                except MessageNotModified:
                    logger.debug("Edit skipped: caption already up to date.")
                except (ChatWriteForbidden, ChatAdminRequired) as e:
                    logger.error(
                        f"Cannot edit in {channel_id} ({e}); "
                        f"grant the bot admin rights with 'Edit messages'."
                    )
                    await _report_error(channel_id, "missing edit permission")
                    channel_queues[channel_id].clear()
                    break
                except Exception as e:
                    logger.error(f"Edit failed: {e}")
                    await enqueue_retry(channel_id, message.id, caption)
                    await _report_error(channel_id, str(e))


_CHANNEL_MEDIA_FILTER = (
    filters.video | filters.document | filters.animation | filters.audio | filters.voice
)
_MEDIA_FILTER = _CHANNEL_MEDIA_FILTER | filters.video_note


@app.on_message(filters.channel & _CHANNEL_MEDIA_FILTER)
async def channel_handler(_, message):
    if message.chat.id not in authorized_chats:
        return
    if caption_has_media_info(message.caption or ''):
        return
    if _media_of(message) is None:
        return
    _enqueue_channel_message(message.chat.id, message)


async def _channel_worker(channel_id: int):
    try:
        await _process_channel_queue(channel_id)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.exception(f"Channel worker for {channel_id} crashed: {e}")
        await _report_error(channel_id, str(e))
    finally:
        channel_workers[channel_id] = False
        if channel_queues[channel_id] and not AUTO_CAPTION_PAUSED:
            channel_workers[channel_id] = True
            asyncio.create_task(_channel_worker(channel_id))


@app.on_message(filters.private & _MEDIA_FILTER)
async def private_handler(_, message):
    if not message.from_user:
        return
    user_id = message.from_user.id

    if user_id == ADMIN_ID and user_id in _admin_input_state:
        st = _admin_input_state.get(user_id, {})
        fwd_chat = _get_forwarded_chat(message)
        if st.get("mode") == "add_chat" and fwd_chat:
            _admin_input_state.pop(user_id, None)
            await _authorize_chat_and_reply(message, fwd_chat.id)
            return

    if user_id in active_users:
        await message.reply_text("\u26a0\ufe0f Please wait until your current file is processed.")
        return
    active_users.add(user_id)
    asyncio.create_task(_handle_private(message))


async def _handle_private(message):
    progress_msg = None
    user_id = message.from_user.id
    try:
        progress_msg = await message.reply_text("\u23f3 <b>Probing media\u2026</b>", parse_mode=ParseMode.HTML)
        media = _media_of(message)
        if media is None:
            await _safe_edit(progress_msg, "\u26a0\ufe0f Unsupported media type.")
            return

        info, deep = await probe_media(
            message, media, lambda t: _safe_edit(progress_msg, t, throttle=True)
        )
        caption = (
            _build_caption(message, media, info) if deep
            else _build_caption_from_tg(message, media)
        )
        if deep:
            _runtime["deep"] += 1
        else:
            caption += "\n<i>\u26a0\ufe0f Telegram metadata only (deep scan unavailable).</i>"
            _runtime["fallback"] += 1

        extra_rows: list[list[InlineKeyboardButton]] = []
        fwd_chat = _get_forwarded_chat(message)
        if user_id == ADMIN_ID and fwd_chat and fwd_chat.id not in authorized_chats:
            extra_rows.append([
                InlineKeyboardButton(
                    f"\u2795 Authorize {(fwd_chat.title or str(fwd_chat.id))[:24]}",
                    callback_data=f"adm:chat_add_id:{fwd_chat.id}",
                )
            ])

        markup = _report_markup(media, info, extra_rows=extra_rows) if deep else (
            InlineKeyboardMarkup(extra_rows) if extra_rows else None
        )
        try:
            await message.reply_text(
                caption, parse_mode=ParseMode.HTML, reply_markup=markup
            )
            await _delete_quietly(progress_msg)
        except Exception:
            await _safe_edit(progress_msg, caption, parse_mode=ParseMode.HTML, reply_markup=markup)
    except Exception as e:
        logger.error(f"Private handler error: {e}")
        if progress_msg:
            await _safe_edit(progress_msg, f"\u274c Failed: {html.escape(str(e))[:200]}")
    finally:
        active_users.discard(user_id)


async def _delete_quietly(msg):
    if not msg:
        return
    try:
        await msg.delete()
    except Exception:
        pass


# --- FULL MEDIAINFO REPORT ---
_report_cache: "collections.OrderedDict[str, str]" = collections.OrderedDict()
_REPORT_CACHE_MAX = 128


def _cache_report(text: str) -> str:
    token = uuid.uuid4().hex[:12]
    _report_cache[token] = text
    while len(_report_cache) > _REPORT_CACHE_MAX:
        _report_cache.popitem(last=False)
    return token


def _full_report(media, info: dict) -> str:
    """Structured, categorized report used by the Full MediaInfo button."""
    width, height = info.get('width'), info.get('height')
    dc_id = _media_dc(media)
    fname = html.escape(str(getattr(media, 'file_name', None) or 'Unnamed Media'))
    size_str = html.escape(_fmt_size(getattr(media, 'file_size', None)))
    cont_str = html.escape(str(info.get('container') or '-'))
    dur_str = html.escape(_fmt_duration(info['duration']) if info.get('duration') else '-')
    bit_str = html.escape(_fmt_bitrate(info.get('bitrate')))

    res_str = f"{width}x{height}" if width and height else '-'
    qual_str = get_standard_resolution(height or width or 0) or '-'
    vcodec_str = html.escape(str(info.get('codec_name') or info.get('codec') or '-'))
    depth_str = f"{info['bit_depth']}-bit" if info.get('bit_depth') else '-'
    hdr_str = html.escape(str(info.get('hdr') or 'SDR'))
    fps_str = html.escape(_fmt_fps(info.get('fps')) or '-')

    acodec_str = html.escape(str(info.get('audio_codec') or '-'))
    achan_str = html.escape(str(info.get('audio_channels') or '-'))
    alang_str = html.escape(', '.join(info.get('audio_langs') or []) or 'Original Audio')
    sub_str = html.escape(str(info.get('subtitle') or 'None'))
    src_str = html.escape(str(info.get('source') or '-'))
    dc_str = f"DC{dc_id}" if dc_id else '-'

    return (
        "<b>\U0001F4CB Technical MediaInfo Report</b>\n\n"
        "<b>\U0001F4C1 Container &amp; File</b>\n"
        "<blockquote>"
        f"\u251c <b>File:</b> <code>{fname}</code>\n"
        f"\u251c <b>Size:</b> <code>{size_str}</code> \u2022 <b>Format:</b> <code>{cont_str}</code>\n"
        f"\u251c <b>Duration:</b> <code>{dur_str}</code> \u2022 <b>Bitrate:</b> <code>{bit_str}</code>\n"
        f"\u2514 <b>Source:</b> <code>{src_str}</code> (<code>{dc_str}</code>)"
        "</blockquote>\n"
        "<b>\U0001F39E Video Stream</b>\n"
        "<blockquote>"
        f"\u251c <b>Resolution:</b> <code>{res_str}</code> (<code>{qual_str}</code>)\n"
        f"\u251c <b>Codec:</b> <code>{vcodec_str}</code> \u2022 <b>Depth:</b> <code>{depth_str}</code>\n"
        f"\u2514 <b>Dynamic Range:</b> <code>{hdr_str}</code> \u2022 <b>FPS:</b> <code>{fps_str}</code>"
        "</blockquote>\n"
        "<b>\U0001F50A Audio &amp; Subtitles</b>\n"
        "<blockquote>"
        f"\u251c <b>Audio Codec:</b> <code>{acodec_str}</code> (<code>{achan_str}</code>)\n"
        f"\u251c <b>Languages:</b> <code>{alang_str}</code>\n"
        f"\u2514 <b>Subtitles:</b> <code>{sub_str}</code>"
        "</blockquote>"
    )


def _report_markup(
    media,
    info: dict,
    extra_rows: Optional[list[list[InlineKeyboardButton]]] = None,
) -> Optional[InlineKeyboardMarkup]:
    rows: list[list[InlineKeyboardButton]] = []
    if FULL_REPORT:
        try:
            token = _cache_report(_full_report(media, info))
            rows.append([
                InlineKeyboardButton("\U0001F4CB Technical MediaInfo", callback_data=f"rep:{token}")
            ])
        except Exception as e:
            logger.warning(f"could not build report: {e}")
    if extra_rows:
        rows.extend(extra_rows)
    return InlineKeyboardMarkup(rows) if rows else None


@app.on_callback_query(filters.regex(r"^rep:"))
async def report_callback(_, query):
    token = query.data.split(":", 1)[1]
    if token == "close":
        await _delete_quietly(query.message)
        return await query.answer()
    report = _report_cache.get(token)
    if not report:
        return await query.answer("Report expired, send the file again.", show_alert=True)
    try:
        close_kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("\u2716\ufe0f Dismiss Report", callback_data="rep:close")]
        ])
        await query.message.reply_text(
            report, parse_mode=ParseMode.HTML, reply_markup=close_kb
        )
        await query.answer()
    except Exception as e:
        await query.answer(f"Failed: {e}"[:180], show_alert=True)


async def _info_reply(message, target, media):
    """Shared implementation of the /info command."""
    status = await message.reply_text("\u23f3 <b>Scanning media\u2026</b>", parse_mode=ParseMode.HTML)
    try:
        info, deep = await probe_media(
            target, media, lambda t: _safe_edit(status, t, throttle=True)
        )
        caption = (
            _build_caption(target, media, info) if deep
            else _build_caption_from_tg(target, media)
        )
        if not deep:
            caption += "\n<i>\u26a0\ufe0f Telegram metadata only.</i>"
        await message.reply_text(
            caption,
            parse_mode=ParseMode.HTML,
            reply_markup=_report_markup(media, info) if deep else None,
        )
        await _delete_quietly(status)
    except Exception as e:
        logger.error(f"/info failed: {e}")
        await _safe_edit(
            status,
            f"\u274c Failed\n\n<code>{html.escape(str(e))[:300]}</code>",
            parse_mode=ParseMode.HTML,
        )


@app.on_message(filters.command("info") & filters.reply)
async def info_command(_, message):
    reply = message.reply_to_message
    media = _media_of(reply) if reply else None
    if media is None:
        return await message.reply_text("\u26a0\ufe0f Reply to a video, audio or document.")
    await _info_reply(message, reply, media)


@app.on_message(filters.command("info") & ~filters.reply & _MEDIA_FILTER)
async def info_command_forward(_, message):
    """Handle /info sent together with a media file (no reply needed)."""
    media = _media_of(message)
    if media is None:
        return await message.reply_text(
            "\u26a0\ufe0f Send a file with the /info command, or reply to one."
        )
    await _info_reply(message, message, media)


@app.on_message(filters.command("info") & ~filters.reply & ~_MEDIA_FILTER)
async def info_command_help(_, message):
    await message.reply_text(
        "\u26a0\ufe0f Reply to a video/document with <code>/info</code>, "
        "or send the file together with the command.",
        parse_mode=ParseMode.HTML,
    )


# =============================================================================
# ORGANIZED INLINE KEYBOARD UI & MONGODB CONFIGURATION HUB
# =============================================================================

def _fmt_uptime(seconds: float) -> str:
    seconds = int(seconds)
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    if days:
        return f"{days}d {hours}h {minutes}m"
    if hours:
        return f"{hours}h {minutes}m {secs}s"
    return f"{minutes}m {secs}s"


def _tab_label(label: str, active: bool) -> str:
    """Highlight the currently selected tab in inline navigation bars."""
    return f"\u2022 {label} \u2022" if active else label


async def _get_chat_label(cid: int) -> str:
    """Resolve and cache a human-readable title for a chat ID."""
    if cid in _chat_title_cache:
        return _chat_title_cache[cid]
    try:
        chat = await app.get_chat(cid)
        title = chat.title or chat.first_name or str(cid)
        _chat_title_cache[cid] = title
        return title
    except Exception:
        return "Unreachable / Private"


async def _build_admin_home() -> tuple[str, InlineKeyboardMarkup]:
    """Build the clean, categorized Admin Control Center dashboard."""
    pending = sum(len(q) for q in channel_queues.values())
    try:
        retry_count = await retry_collection.count_documents({})
    except Exception:
        retry_count = -1

    dcs = dc_stream.active_dcs()
    status_badge = "\u23f8 Paused" if AUTO_CAPTION_PAUSED else "\U0001F7E2 Active"
    tail_badge = f"ON ({TAIL_SIZE_MB} MB)" if ENABLE_TAIL_PROBE else "OFF"
    rep_badge = "ON" if FULL_REPORT else "OFF"
    db_badge = "\U0001F7E2 Synced" if _db_ok else "\U0001F534 Offline"
    sess_badge = (
        "MongoStorage"
        if (USE_MONGO_SESSION and MongoStorage is not None)
        else "SQLite File"
    )
    dc_badge = ", ".join(f"DC{d}" for d in dcs) if dcs else "Idle"

    text = (
        "<b>\U0001F39B MediaInfo Bot \u2022 Control Center</b>\n\n"
        "<b>\U0001F4CC Pipeline Status</b>\n"
        "<blockquote>"
        f"\u251c <b>Auto-Caption:</b> {status_badge} \u2022 <b>Delay:</b> <code>{EDIT_DELAY:.1f}s</code>\n"
        f"\u251c <b>Channels:</b> <code>{len(authorized_chats)}</code> \u2022 "
        f"<b>Queue:</b> <code>{pending}</code> (Retries: <code>{retry_count if retry_count >= 0 else '?'}</code>)\n"
        f"\u2514 <b>Session Scans:</b> <code>{_runtime['deep']}</code> deep \u2022 "
        f"<code>{_runtime['fallback']}</code> fallback"
        "</blockquote>\n"
        "<b>\u26a1 Engine &amp; Storage</b>\n"
        "<blockquote>"
        f"\u251c <b>Probing:</b> Tail <code>{tail_badge}</code> \u2022 Report <code>{rep_badge}</code>\n"
        f"\u251c <b>Workers:</b> <code>{STREAM_CONCURRENCY}</code> streams \u2022 "
        f"<code>{CHANNEL_CONCURRENCY}</code> channels\n"
        f"\u251c <b>MongoDB:</b> {db_badge} \u2022 <b>Session:</b> <code>{sess_badge}</code>\n"
        f"\u2514 <b>Media DCs:</b> <code>{dc_badge}</code> \u2022 "
        f"<b>Uptime:</b> <code>{_fmt_uptime(time.time() - START_TIME)}</code>"
        "</blockquote>"
    )

    pause_label = "\u25b6\ufe0f Resume Bot" if AUTO_CAPTION_PAUSED else "\u23f8 Pause Bot"
    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                f"\U0001F4E1 Channels ({len(authorized_chats)})",
                callback_data="adm:chats:0",
            ),
            InlineKeyboardButton(
                f"\U0001F4E5 Queues ({pending})",
                callback_data="adm:queue",
            ),
        ],
        [
            InlineKeyboardButton("\u2699\ufe0f Settings & Config", callback_data="adm:cfg_tab:probing"),
            InlineKeyboardButton("\U0001F3A8 Caption Studio", callback_data="adm:tpl"),
        ],
        [
            InlineKeyboardButton("\U0001F4CA Analytics", callback_data="adm:stats"),
            InlineKeyboardButton("\U0001F5A5 Server & Logs", callback_data="adm:server"),
        ],
        [
            InlineKeyboardButton("\U0001F4E2 Broadcast", callback_data="adm:bcast_prompt"),
            InlineKeyboardButton(pause_label, callback_data="adm:toggle_pause:home"),
        ],
        [
            InlineKeyboardButton("\U0001F6E0 Power", callback_data="adm:power"),
            InlineKeyboardButton("\U0001F504 Refresh", callback_data="adm:home"),
            InlineKeyboardButton("\u2716\ufe0f Close", callback_data="adm:close"),
        ],
    ])
    return text, keyboard


def _build_settings_hub_view(tab: str = "probing") -> tuple[str, InlineKeyboardMarkup]:
    """Unified, tabbed Settings & MongoDB Configuration Hub.

    Tabs:
      - ``probing``: Tail probe, tail size, probe/stream timeouts, retries, stream concurrency, DC reset
      - ``pacing``: Auto-caption state, edit delay, channel concurrency, retry limit, error alert threshold
      - ``system``: Full report button, log level, WZGram session backend, key=value editor, JSON export, .env reset
    """
    if tab not in ("probing", "pacing", "system"):
        tab = "probing"

    tab_bar = [
        InlineKeyboardButton(
            _tab_label("\u26a1 Probing", tab == "probing"),
            callback_data="adm:cfg_tab:probing",
        ),
        InlineKeyboardButton(
            _tab_label("\u23f1 Pacing", tab == "pacing"),
            callback_data="adm:cfg_tab:pacing",
        ),
        InlineKeyboardButton(
            _tab_label("\U0001F5C4 System", tab == "system"),
            callback_data="adm:cfg_tab:system",
        ),
    ]

    dcs = dc_stream.active_dcs()
    flood = dc_stream.flood_left()

    if tab == "probing":
        tail_state = "\u2705 Enabled" if ENABLE_TAIL_PROBE else "\u274c Disabled"
        text = (
            "<b>\u2699\ufe0f Settings &amp; MongoDB Config \u2022 Probing &amp; Streams</b>\n"
            f"<i>All changes save immediately to</i> <code>{html.escape(DB_NAME)}.settings[\"bot_config\"]</code>\n\n"
            "<blockquote>"
            f"\u251c <b>Tail Index Probe:</b> {tail_state} (<code>{TAIL_SIZE_MB} MB</code> window)\n"
            f"\u251c <b>Probe Timeout:</b> <code>{PROBE_TIMEOUT:.0f}s</code> (mediainfo / ffprobe)\n"
            f"\u251c <b>Stream Timeout:</b> <code>{STREAM_TIMEOUT:.0f}s</code> per range\n"
            f"\u251c <b>Stream Retries:</b> <code>{STREAM_RETRIES}</code> attempts\n"
            f"\u251c <b>Stream Workers:</b> <code>{STREAM_CONCURRENCY}</code> concurrent downloads\n"
            f"\u2514 <b>Cached DCs:</b> <code>{', '.join(f'DC{d}' for d in dcs) or 'none'}</code>"
            + (f" (FloodWait: <code>{flood}s</code>)" if flood else "")
            + "</blockquote>"
        )
        rows = [
            tab_bar,
            [
                InlineKeyboardButton(
                    "Tail Probe: " + ("\u2705 ON" if ENABLE_TAIL_PROBE else "\u274c OFF"),
                    callback_data="adm:tog:tail_probe",
                ),
                InlineKeyboardButton("\u2796 Tail", callback_data="adm:cfg_step:tail_size_mb:-1:probing"),
                InlineKeyboardButton(f"{TAIL_SIZE_MB} MB", callback_data="adm:noop"),
                InlineKeyboardButton("\u2795 Tail", callback_data="adm:cfg_step:tail_size_mb:1:probing"),
            ],
            [
                InlineKeyboardButton("\u2796 Probe", callback_data="adm:cfg_step:probe_timeout:-5:probing"),
                InlineKeyboardButton(f"Probe: {PROBE_TIMEOUT:.0f}s", callback_data="adm:noop"),
                InlineKeyboardButton("\u2795 Probe", callback_data="adm:cfg_step:probe_timeout:5:probing"),
            ],
            [
                InlineKeyboardButton("\u2796 Stream", callback_data="adm:cfg_step:stream_timeout:-15:probing"),
                InlineKeyboardButton(f"Stream: {STREAM_TIMEOUT:.0f}s", callback_data="adm:noop"),
                InlineKeyboardButton("\u2795 Stream", callback_data="adm:cfg_step:stream_timeout:15:probing"),
            ],
            [
                InlineKeyboardButton("\u2796 Retry", callback_data="adm:cfg_step:stream_retries:-1:probing"),
                InlineKeyboardButton(f"Retries: {STREAM_RETRIES}", callback_data="adm:noop"),
                InlineKeyboardButton("\u2795 Retry", callback_data="adm:cfg_step:stream_retries:1:probing"),
                InlineKeyboardButton("\u2796", callback_data="adm:conc:stream:-1:probing"),
                InlineKeyboardButton(f"Work: {STREAM_CONCURRENCY}", callback_data="adm:noop"),
                InlineKeyboardButton("\u2795", callback_data="adm:conc:stream:1:probing"),
            ],
            [
                InlineKeyboardButton("\U0001F50C Reset DC Sessions", callback_data="adm:dc_reset:probing"),
                InlineKeyboardButton("\U0001F39B Dashboard", callback_data="adm:home"),
            ],
        ]
        return text, InlineKeyboardMarkup(rows)

    if tab == "pacing":
        pause_state = "\u23f8 Paused" if AUTO_CAPTION_PAUSED else "\U0001F7E2 Active"
        text = (
            "<b>\u2699\ufe0f Settings &amp; MongoDB Config \u2022 Pacing &amp; Retries</b>\n"
            f"<i>All changes save immediately to</i> <code>{html.escape(DB_NAME)}.settings[\"bot_config\"]</code>\n\n"
            "<blockquote>"
            f"\u251c <b>Auto-Captioning:</b> {pause_state}\n"
            f"\u251c <b>Edit Delay:</b> <code>{EDIT_DELAY:.1f}s</code> between edits per channel\n"
            f"\u251c <b>Channel Workers:</b> <code>{CHANNEL_CONCURRENCY}</code> parallel channels\n"
            f"\u251c <b>Max Retry Attempts:</b> <code>{MAX_RETRY_ATTEMPTS}</code> before dropping\n"
            f"\u251c <b>Error Alert Threshold:</b> <code>{ERROR_THRESHOLD}x</code> consecutive failures\n"
            f"\u2514 <b>Alert Cooldown:</b> <code>{_ALERT_COOLDOWN}s</code>"
            "</blockquote>"
        )
        rows = [
            tab_bar,
            [
                InlineKeyboardButton(
                    "Auto-Caption: " + ("\u23f8 Paused" if AUTO_CAPTION_PAUSED else "\U0001F7E2 Active"),
                    callback_data="adm:tog:pause:pacing",
                ),
                InlineKeyboardButton("\u23f1 Delay Presets", callback_data="adm:delay"),
            ],
            [
                InlineKeyboardButton("\u2796 0.5s", callback_data="adm:delay_adj:-0.5:pacing"),
                InlineKeyboardButton(f"Delay: {EDIT_DELAY:.1f}s", callback_data="adm:delay"),
                InlineKeyboardButton("\u2795 0.5s", callback_data="adm:delay_adj:0.5:pacing"),
            ],
            [
                InlineKeyboardButton("\u2796 Chan", callback_data="adm:conc:chan:-1:pacing"),
                InlineKeyboardButton(f"Chan Workers: {CHANNEL_CONCURRENCY}", callback_data="adm:noop"),
                InlineKeyboardButton("\u2795 Chan", callback_data="adm:conc:chan:1:pacing"),
            ],
            [
                InlineKeyboardButton("\u2796 MaxRetry", callback_data="adm:cfg_step:max_retry_attempts:-1:pacing"),
                InlineKeyboardButton(f"Max Retry: {MAX_RETRY_ATTEMPTS}", callback_data="adm:noop"),
                InlineKeyboardButton("\u2795 MaxRetry", callback_data="adm:cfg_step:max_retry_attempts:1:pacing"),
            ],
            [
                InlineKeyboardButton("\u2796 Alert", callback_data="adm:cfg_step:error_threshold:-1:pacing"),
                InlineKeyboardButton(f"Alert At: {ERROR_THRESHOLD}x", callback_data="adm:noop"),
                InlineKeyboardButton("\u2795 Alert", callback_data="adm:cfg_step:error_threshold:1:pacing"),
            ],
            [InlineKeyboardButton("\U0001F39B Dashboard", callback_data="adm:home")],
        ]
        return text, InlineKeyboardMarkup(rows)

    # tab == "system"
    sess_mode = f"MongoStorage ({SESSION_DB_NAME})" if USE_MONGO_SESSION else "Local File"
    rep_state = "\u2705 Enabled" if FULL_REPORT else "\u274c Disabled"
    text = (
        "<b>\u2699\ufe0f Settings &amp; MongoDB Config \u2022 System &amp; Storage</b>\n"
        f"<i>Database:</i> <code>{html.escape(DB_NAME)}</code> \u2022 "
        "<i>Doc:</i> <code>settings[\"bot_config\"]</code>\n\n"
        "<blockquote>"
        f"\u251c <b>Technical Report Button:</b> {rep_state}\n"
        f"\u251c <b>Log Verbosity:</b> <code>{LOG_LEVEL}</code>\n"
        f"\u251c <b>WZGram Session Store:</b> <code>{html.escape(sess_mode)}</code>\n"
        f"\u251c <b>GC Thresholds:</b> <code>{GC_THRESHOLD[0]}, {GC_THRESHOLD[1]}, {GC_THRESHOLD[2]}</code>\n"
        f"\u2514 <b>Authorized Channels:</b> <code>{len(authorized_chats)} saved</code>"
        "</blockquote>"
    )
    rows = [
        tab_bar,
        [
            InlineKeyboardButton(
                "Report Btn: " + ("\u2705 ON" if FULL_REPORT else "\u274c OFF"),
                callback_data="adm:tog:full_report:system",
            ),
            InlineKeyboardButton(
                f"\U0001F4DD Log: {LOG_LEVEL}",
                callback_data="adm:cfg_cycle_log",
            ),
        ],
        [
            InlineKeyboardButton(
                "Session Store: " + ("MongoDB" if USE_MONGO_SESSION else "File"),
                callback_data="adm:tog:mongo_session",
            ),
        ],
        [
            InlineKeyboardButton("\u270f\ufe0f Edit Key=Value", callback_data="adm:cfg_edit_prompt"),
            InlineKeyboardButton("\U0001F4E4 Export JSON", callback_data="adm:cfg_export"),
        ],
        [
            InlineKeyboardButton("\U0001F504 Reset All to .env", callback_data="adm:cfg_reset_confirm"),
            InlineKeyboardButton("\U0001F39B Dashboard", callback_data="adm:home"),
        ],
    ]
    return text, InlineKeyboardMarkup(rows)


def _build_db_config_view() -> tuple[str, InlineKeyboardMarkup]:
    """Alias for the Settings & MongoDB Config Hub."""
    return _build_settings_hub_view("probing")


def _build_toggles_view() -> tuple[str, InlineKeyboardMarkup]:
    """Alias for the Probing tab in the Settings Hub."""
    return _build_settings_hub_view("probing")


CHATS_PER_PAGE = 5


async def _build_chats_view(page: int = 0) -> tuple[str, InlineKeyboardMarkup]:
    """Build a clean, paginated Channel Directory where each channel opens a dedicated inspector."""
    sorted_cids = sorted(authorized_chats)
    total = len(sorted_cids)
    max_page = max(0, (total - 1) // CHATS_PER_PAGE) if total else 0
    page = max(0, min(page, max_page))
    slice_cids = sorted_cids[page * CHATS_PER_PAGE : (page + 1) * CHATS_PER_PAGE]

    lines = [f"<b>\U0001F4E1 Channel Directory ({total})</b>"]
    rows: list[list[InlineKeyboardButton]] = []

    if not sorted_cids:
        lines.append(
            "\n<blockquote>No channels are authorized yet.\n"
            "Tap <b>\u2795 Add Channel</b> below to authorize a channel by ID, "
            "<code>@username</code>, or by forwarding a post.</blockquote>"
        )
    else:
        lines.append(
            f"<i>Page {page + 1}/{max_page + 1} \u2014 select a channel below to inspect, scan, or remove:</i>\n"
            "<blockquote>"
        )
        for idx, cid in enumerate(slice_cids):
            title = await _get_chat_label(cid)
            q_len = len(channel_queues.get(cid, []))
            branch = "\u2514" if idx == len(slice_cids) - 1 else "\u251c"
            q_tag = f" \u2022 <code>{q_len} queued</code>" if q_len else ""
            lines.append(f"{branch} <b>{html.escape(title[:32])}</b> (<code>{cid}</code>){q_tag}")

            btn_q = f" \u2022 \U0001F4E5 {q_len}" if q_len else ""
            rows.append([
                InlineKeyboardButton(
                    f"\U0001F4E1 {title[:24]}{btn_q}",
                    callback_data=f"adm:chat_view:{cid}:{page}",
                ),
            ])
        lines[-1] += "</blockquote>"

    nav_row: list[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("\u2b05\ufe0f Prev", callback_data=f"adm:chats:{page - 1}"))
    if total > CHATS_PER_PAGE:
        nav_row.append(InlineKeyboardButton(f"{page + 1}/{max_page + 1}", callback_data="adm:noop"))
    if page < max_page:
        nav_row.append(InlineKeyboardButton("Next \u27a1\ufe0f", callback_data=f"adm:chats:{page + 1}"))
    if nav_row:
        rows.append(nav_row)

    rows.append([
        InlineKeyboardButton("\u2795 Add Channel", callback_data="adm:chat_add_prompt"),
        InlineKeyboardButton("\U0001F50E Backfill Scanner", callback_data="adm:scan_menu"),
    ])
    rows.append([
        InlineKeyboardButton("\U0001F504 Refresh", callback_data=f"adm:chats:{page}"),
        InlineKeyboardButton("\U0001F39B Dashboard", callback_data="adm:home"),
    ])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


async def _build_chat_detail_view(cid: int, page: int = 0) -> tuple[str, InlineKeyboardMarkup]:
    """Dedicated Channel Inspector view with stats, queue state, scan options, and removal."""
    title = await _get_chat_label(cid)
    last_id = await get_last_id(cid)
    stat_doc = await get_chat_stat(cid)
    q_len = len(channel_queues.get(cid, []))
    worker_state = (
        "Running"
        if channel_workers.get(cid)
        else ("Paused" if AUTO_CAPTION_PAUSED else "Idle")
    )
    total_cnt = stat_doc.get("count", 0)
    deep_cnt = stat_doc.get("deep", 0)
    fb_cnt = stat_doc.get("fallback", 0)

    text = (
        f"<b>\U0001F4E1 Channel Inspector</b>\n\n"
        "<blockquote>"
        f"\u251c <b>Title:</b> <b>{html.escape(title)}</b>\n"
        f"\u251c <b>Chat ID:</b> <code>{cid}</code>\n"
        f"\u251c <b>Worker:</b> <code>{worker_state}</code> \u2022 <b>Queued:</b> <code>{q_len}</code>\n"
        f"\u251c <b>Processed:</b> <code>{total_cnt}</code> "
        f"(deep: <code>{deep_cnt}</code>, fallback: <code>{fb_cnt}</code>)\n"
        f"\u2514 <b>Last Msg ID:</b> <code>#{last_id if last_id > 1 else 'None'}</code>"
        "</blockquote>\n"
        "<i>Use the controls below to backfill uncaptioned posts or manage this channel:</i>"
    )
    rows: list[list[InlineKeyboardButton]] = []
    if last_id > 1:
        rows.append([
            InlineKeyboardButton(
                f"\U0001F195 Scan New Since #{last_id}",
                callback_data=f"adm:scan_since:{cid}:500",
            )
        ])
    rows.append([
        InlineKeyboardButton("\U0001F50E Scan 50", callback_data=f"adm:scan_run:{cid}:50"),
        InlineKeyboardButton("\U0001F50E Scan 200", callback_data=f"adm:scan_run:{cid}:200"),
        InlineKeyboardButton("\U0001F50E More\u2026", callback_data=f"adm:scan_pick:{cid}"),
    ])
    action_row = []
    if q_len > 0:
        action_row.append(
            InlineKeyboardButton(f"\U0001F9F9 Clear Queue ({q_len})", callback_data=f"adm:chat_clear_q:{cid}:{page}")
        )
    action_row.append(
        InlineKeyboardButton("\U0001F5D1 Remove Channel", callback_data=f"adm:chat_rm_confirm:{cid}:{page}")
    )
    rows.append(action_row)
    rows.append([
        InlineKeyboardButton("\u2b05\ufe0f Channels", callback_data=f"adm:chats:{page}"),
        InlineKeyboardButton("\U0001F504 Refresh", callback_data=f"adm:chat_view:{cid}:{page}"),
        InlineKeyboardButton("\U0001F39B Dashboard", callback_data="adm:home"),
    ])
    return text, InlineKeyboardMarkup(rows)


async def _build_queue_view() -> tuple[str, InlineKeyboardMarkup]:
    """Build the Queue & Retry Manager view."""
    total = sum(len(q) for q in channel_queues.values())
    try:
        retry_count = await retry_collection.count_documents({})
    except Exception:
        retry_count = -1

    state_str = "\u23f8 Paused" if AUTO_CAPTION_PAUSED else "\U0001F7E2 Active"
    flood = dc_stream.flood_left()

    lines = [
        "<b>\U0001F4E5 Queue &amp; Retry Manager</b>\n",
        "<blockquote>"
        f"\u251c <b>Pipeline State:</b> {state_str}\n"
        f"\u251c <b>In-Memory Queue:</b> <code>{total} pending</code>\n"
        f"\u251c <b>MongoDB Retry Queue:</b> <code>{retry_count if retry_count >= 0 else 'unreachable'}</code>\n"
        f"\u2514 <b>DC FloodWait:</b> <code>{f'{flood}s remaining' if flood else 'Clear'}</code>"
        "</blockquote>",
    ]

    active_qs = [(cid, q) for cid, q in channel_queues.items() if q]
    if active_qs:
        lines.append("\n<b>Active Channel Queues</b>\n<blockquote>")
        for idx, (cid, q) in enumerate(active_qs[:10]):
            worker = "running" if channel_workers.get(cid) else ("paused" if AUTO_CAPTION_PAUSED else "idle")
            branch = "\u2514" if idx == min(len(active_qs), 10) - 1 else "\u251c"
            lines.append(f"{branch} <code>{cid}</code> \u2192 <b>{len(q)}</b> (<i>{worker}</i>)")
        lines[-1] += "</blockquote>"

    pause_btn = (
        InlineKeyboardButton("\u25b6\ufe0f Resume Queues", callback_data="adm:toggle_pause:queue")
        if AUTO_CAPTION_PAUSED
        else InlineKeyboardButton("\u23f8 Pause Queues", callback_data="adm:toggle_pause:queue")
    )
    rows = [
        [
            pause_btn,
            InlineKeyboardButton("\U0001F501 Run Retries Now", callback_data="adm:queue_retry"),
        ],
        [
            InlineKeyboardButton("\U0001F9F9 Clear Pending", callback_data="adm:queue_clear_confirm"),
            InlineKeyboardButton("\U0001F5D1 Clear Retry DB", callback_data="adm:retry_clear_confirm"),
        ],
        [
            InlineKeyboardButton("\U0001F504 Refresh", callback_data="adm:queue"),
            InlineKeyboardButton("\U0001F39B Dashboard", callback_data="adm:home"),
        ],
    ]
    return "\n".join(lines), InlineKeyboardMarkup(rows)


async def _build_stats_view() -> tuple[str, InlineKeyboardMarkup]:
    """Build the Processing Analytics view."""
    all_stats = await get_all_stats()
    total = sum(s.get("count", 0) for s in all_stats)
    total_deep = sum(s.get("deep", 0) for s in all_stats)
    total_fb = sum(s.get("fallback", 0) for s in all_stats)
    dcs = dc_stream.active_dcs()

    lines = [
        "<b>\U0001F4CA Processing Analytics</b>\n",
        "<b>All-Time &amp; Session Totals</b>\n"
        "<blockquote>"
        f"\u251c <b>All-Time Total:</b> <code>{total}</code> "
        f"(deep: <code>{total_deep}</code>, fallback: <code>{total_fb}</code>)\n"
        f"\u251c <b>Session Total:</b> <code>{_runtime['deep'] + _runtime['fallback']}</code> "
        f"(deep: <code>{_runtime['deep']}</code>, fallback: <code>{_runtime['fallback']}</code>)\n"
        f"\u251c <b>FloodWaits / Errors:</b> <code>{_runtime['flood']}</code> / <code>{_runtime['probe_fail']}</code>\n"
        f"\u2514 <b>Cached Media DCs:</b> <code>{', '.join(f'DC{d}' for d in dcs) or 'none'}</code>"
        "</blockquote>",
    ]

    if all_stats:
        lines.append("\n<b>Top Channels by Volume</b>\n<blockquote>")
        top_slice = all_stats[:12]
        for idx, s in enumerate(top_slice):
            deep = s.get("deep", 0)
            fb = s.get("fallback", 0)
            branch = "\u2514" if idx == len(top_slice) - 1 else "\u251c"
            detail = f" (deep {deep}, fb {fb})" if deep or fb else ""
            lines.append(f"{branch} <code>{s['chat_id']}</code> \u2192 <b>{s.get('count', 0)}</b>{detail}")
        lines[-1] += "</blockquote>"

    rows = [
        [
            InlineKeyboardButton("\u267b\ufe0f Reset Session", callback_data="adm:stats_reset_session"),
            InlineKeyboardButton("\U0001F5D1 Wipe DB Stats", callback_data="adm:stats_reset_db_confirm"),
        ],
        [
            InlineKeyboardButton("\U0001F504 Refresh", callback_data="adm:stats"),
            InlineKeyboardButton("\U0001F39B Dashboard", callback_data="adm:home"),
        ],
    ]
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def _build_delay_view() -> tuple[str, InlineKeyboardMarkup]:
    """Build the interactive Edit Delay tuner."""
    text = (
        "<b>\u23f1 Caption Edit Pacing</b>\n\n"
        "<blockquote>"
        f"\u251c <b>Active Delay:</b> <code>{EDIT_DELAY:.1f}s</code> per channel\n"
        "\u251c <b>Valid Range:</b> <code>1.0s \u2013 60.0s</code>\n"
        "\u2514 <b>Auto-Backoff:</b> Raises automatically on Telegram FloodWait"
        "</blockquote>\n"
        "<i>Select a preset or fine-tune with the steppers below:</i>"
    )
    rows = [
        [
            InlineKeyboardButton("\u2796 1.0s", callback_data="adm:delay_adj:-1.0:delay"),
            InlineKeyboardButton("\u2796 0.5s", callback_data="adm:delay_adj:-0.5:delay"),
            InlineKeyboardButton("\u2795 0.5s", callback_data="adm:delay_adj:0.5:delay"),
            InlineKeyboardButton("\u2795 1.0s", callback_data="adm:delay_adj:1.0:delay"),
        ],
        [
            InlineKeyboardButton(_tab_label("1.5s", EDIT_DELAY == 1.5), callback_data="adm:delay_set:1.5"),
            InlineKeyboardButton(_tab_label("2.5s", EDIT_DELAY == 2.5), callback_data="adm:delay_set:2.5"),
            InlineKeyboardButton(_tab_label("3.5s", EDIT_DELAY == 3.5), callback_data="adm:delay_set:3.5"),
            InlineKeyboardButton(_tab_label("5.0s", EDIT_DELAY == 5.0), callback_data="adm:delay_set:5.0"),
        ],
        [
            InlineKeyboardButton(_tab_label("7.5s", EDIT_DELAY == 7.5), callback_data="adm:delay_set:7.5"),
            InlineKeyboardButton(_tab_label("10.0s", EDIT_DELAY == 10.0), callback_data="adm:delay_set:10.0"),
            InlineKeyboardButton(_tab_label("15.0s", EDIT_DELAY == 15.0), callback_data="adm:delay_set:15.0"),
            InlineKeyboardButton(_tab_label("30.0s", EDIT_DELAY == 30.0), callback_data="adm:delay_set:30.0"),
        ],
        [
            InlineKeyboardButton("\u270f\ufe0f Custom Value", callback_data="adm:delay_custom"),
            InlineKeyboardButton("\u2b05\ufe0f Pacing Settings", callback_data="adm:cfg_tab:pacing"),
            InlineKeyboardButton("\U0001F39B Dashboard", callback_data="adm:home"),
        ],
    ]
    return text, InlineKeyboardMarkup(rows)


def _build_template_view() -> tuple[str, InlineKeyboardMarkup]:
    """Build the interactive Caption Studio with active preset highlight and live preview."""
    sample_preview = _render_caption(
        title="Sample.Movie.2026.1080p.BluRay.x265.mkv",
        video_line="1080p HEVC 10bit HDR",
        duration="02:14:35",
        audio="English, Japanese",
        subtitle="English, Spanish",
        size="2.45 GB",
        bitrate="2.61 Mb/s",
        fps="23.976 fps",
        resolution="1920x1080",
        codec="HEVC",
        container="Matroska",
        filename="Sample.Movie.2026.1080p.BluRay.x265.mkv",
    )
    active_preset = "Custom"
    for key, (label, tpl_str) in TEMPLATE_PRESETS.items():
        if CAPTION_TEMPLATE == tpl_str:
            active_preset = label
            break

    placeholders = (
        "{title} {video_line} {duration} {audio} {subtitle} "
        "{size} {bitrate} {fps} {resolution} {codec} {container} {filename}"
    )
    text = (
        f"<b>\U0001F3A8 Caption Studio \u2022 {html.escape(active_preset)}</b>\n\n"
        "<b>Live Sample Output</b>\n"
        f"<blockquote>{sample_preview}</blockquote>\n"
        "<b>Active Template Source</b>\n"
        f"<pre>{html.escape(CAPTION_TEMPLATE)}</pre>\n"
        "<b>Placeholders:</b>\n"
        f"<code>{html.escape(placeholders)}</code>"
    )
    rows = [
        [
            InlineKeyboardButton(
                _tab_label("\U0001F3AC Default", CAPTION_TEMPLATE == TEMPLATE_PRESETS["default"][1]),
                callback_data="adm:tpl_preset:default",
            ),
            InlineKeyboardButton(
                _tab_label("\U0001F4AC Subtitles", CAPTION_TEMPLATE == TEMPLATE_PRESETS["subs"][1]),
                callback_data="adm:tpl_preset:subs",
            ),
        ],
        [
            InlineKeyboardButton(
                _tab_label("\U0001F4CA Detailed", CAPTION_TEMPLATE == TEMPLATE_PRESETS["detailed"][1]),
                callback_data="adm:tpl_preset:detailed",
            ),
            InlineKeyboardButton(
                _tab_label("\u2728 Minimal", CAPTION_TEMPLATE == TEMPLATE_PRESETS["minimal"][1]),
                callback_data="adm:tpl_preset:minimal",
            ),
        ],
        [
            InlineKeyboardButton("\u270f\ufe0f Custom Template", callback_data="adm:tpl_custom"),
            InlineKeyboardButton("\U0001F504 Reset Default", callback_data="adm:tpl_preset:env"),
        ],
        [InlineKeyboardButton("\U0001F39B Dashboard", callback_data="adm:home")],
    ]
    return text, InlineKeyboardMarkup(rows)


async def _build_scan_menu_view() -> tuple[str, InlineKeyboardMarkup]:
    """Build interactive channel picker for backfilling old messages."""
    sorted_cids = sorted(authorized_chats)
    lines = [
        "<b>\U0001F50E Channel History Scanner</b>\n",
        "Select an authorized channel below to backfill uncaptioned media posts:",
    ]
    rows: list[list[InlineKeyboardButton]] = []
    if not sorted_cids:
        lines.append("\n<blockquote>No authorized channels configured yet.</blockquote>")
        rows.append([InlineKeyboardButton("\u2795 Add Channel", callback_data="adm:chat_add_prompt")])
    else:
        for cid in sorted_cids[:12]:
            title = await _get_chat_label(cid)
            rows.append([
                InlineKeyboardButton(
                    f"\U0001F4E1 {title[:26]} ({cid})",
                    callback_data=f"adm:scan_pick:{cid}",
                )
            ])
    rows.append([
        InlineKeyboardButton("\u2b05\ufe0f Channels", callback_data="adm:chats:0"),
        InlineKeyboardButton("\U0001F39B Dashboard", callback_data="adm:home"),
    ])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


async def _build_scan_pick_view(cid: int) -> tuple[str, InlineKeyboardMarkup]:
    """Build message count selector for scanning a specific channel."""
    title = await _get_chat_label(cid)
    last_id = await get_last_id(cid)
    text = (
        f"<b>\U0001F50E Backfill Scanner \u2022 {html.escape(title)}</b>\n\n"
        "<blockquote>"
        f"\u251c <b>Channel ID:</b> <code>{cid}</code>\n"
        f"\u251c <b>Last Processed ID:</b> <code>#{last_id if last_id > 1 else 'None'}</code>\n"
        "\u2514 <b>Filter:</b> Skips posts that already have MediaInfo captions"
        "</blockquote>\n"
        "<i>Select how many recent messages to scan:</i>"
    )
    rows: list[list[InlineKeyboardButton]] = []
    if last_id > 1:
        rows.append([
            InlineKeyboardButton(
                f"\U0001F195 Scan New Since #{last_id} (up to 500)",
                callback_data=f"adm:scan_since:{cid}:500",
            )
        ])
    rows.extend([
        [
            InlineKeyboardButton("25 msgs", callback_data=f"adm:scan_run:{cid}:25"),
            InlineKeyboardButton("50 msgs", callback_data=f"adm:scan_run:{cid}:50"),
            InlineKeyboardButton("100 msgs", callback_data=f"adm:scan_run:{cid}:100"),
        ],
        [
            InlineKeyboardButton("250 msgs", callback_data=f"adm:scan_run:{cid}:250"),
            InlineKeyboardButton("500 msgs", callback_data=f"adm:scan_run:{cid}:500"),
            InlineKeyboardButton("1000 msgs", callback_data=f"adm:scan_run:{cid}:1000"),
        ],
        [
            InlineKeyboardButton("\u2b05\ufe0f Inspector", callback_data=f"adm:chat_view:{cid}:0"),
            InlineKeyboardButton("\U0001F4E1 Channels", callback_data="adm:scan_menu"),
            InlineKeyboardButton("\U0001F39B Dashboard", callback_data="adm:home"),
        ],
    ])
    return text, InlineKeyboardMarkup(rows)


def _build_logs_view(n: int = 20, errors_only: bool = False) -> tuple[str, InlineKeyboardMarkup]:
    """Build the live log viewer and inline controls (cross-linked with Server Health)."""
    n = max(5, min(n, 200))
    all_lines = list(_log_buffer)
    if errors_only:
        filtered = [
            ln for ln in all_lines
            if any(lvl in ln for lvl in ("| WARNING |", "| ERROR |", "| CRITICAL |"))
        ]
    else:
        filtered = all_lines

    tail_lines = filtered[-n:]
    mode_label = "Warnings & Errors" if errors_only else f"Last {len(tail_lines)} lines"
    if not tail_lines:
        body = "<blockquote><i>No matching log entries in memory buffer.</i></blockquote>"
    else:
        log_text = html.escape("\n".join(tail_lines))
        if len(log_text) > 3500:
            log_text = "\u2026" + log_text[-3500:]
        body = f"<pre>{log_text}</pre>"

    text = f"<b>\U0001F4DC Live Log Viewer \u2022 {mode_label}</b>\n\n{body}"
    cb_prefix = "adm:logs_err" if errors_only else "adm:logs"
    rows = [
        [
            InlineKeyboardButton("\U0001F5A5 System Health", callback_data="adm:server"),
            InlineKeyboardButton(_tab_label("\U0001F4DC All Logs", not errors_only), callback_data=f"adm:logs:{n}"),
            InlineKeyboardButton(_tab_label("\u26a0\ufe0f Errors", errors_only), callback_data=f"adm:logs_err:{n}"),
        ],
        [
            InlineKeyboardButton(_tab_label("20", n == 20), callback_data=f"{cb_prefix}:20"),
            InlineKeyboardButton(_tab_label("50", n == 50), callback_data=f"{cb_prefix}:50"),
            InlineKeyboardButton(_tab_label("100", n == 100), callback_data=f"{cb_prefix}:100"),
        ],
        [
            InlineKeyboardButton("\U0001F4E5 Download .log", callback_data="adm:logs_dl"),
            InlineKeyboardButton("\U0001F9F9 Clear Buffer", callback_data="adm:logs_clear"),
        ],
        [
            InlineKeyboardButton("\U0001F504 Refresh", callback_data=f"{cb_prefix}:{n}"),
            InlineKeyboardButton("\U0001F39B Dashboard", callback_data="adm:home"),
        ],
    ]
    return text, InlineKeyboardMarkup(rows)


async def _build_server_view(ping_ms: Optional[float] = None) -> tuple[str, InlineKeyboardMarkup]:
    """Build Server & Diagnostics view cross-linked with Live Logs."""
    proc = psutil.Process()
    mem = proc.memory_info().rss
    disk_path = os.path.abspath(TMP_DIR) if os.path.isdir(TMP_DIR) else os.path.abspath('.')
    try:
        disk = psutil.disk_usage(disk_path).percent
    except Exception:
        disk = -1

    try:
        db_start = time.monotonic()
        await db.command("ping")
        db_ms = f"{(time.monotonic() - db_start) * 1000:.0f} ms"
        _db_ok_again()
    except Exception:
        db_ms = "unreachable"

    dcs = dc_stream.active_dcs()
    ping_str = f"{ping_ms:.0f} ms" if ping_ms is not None else "Tap Ping Test"
    sess_backend = "MongoStorage" if (USE_MONGO_SESSION and MongoStorage is not None) else "SQLite"
    mi_ok = "\u2705 Ready" if _which("mediainfo") else "\u274c Missing"
    fp_ok = "\u2705 Ready" if _which("ffprobe") else "\u274c Missing"

    text = (
        "<b>\U0001F5A5 Server &amp; WZGram Diagnostics</b>\n\n"
        "<b>Resources &amp; Uptime</b>\n"
        "<blockquote>"
        f"\u251c <b>CPU Load:</b> <code>{psutil.cpu_percent()}%</code>\n"
        f"\u251c <b>Memory:</b> <code>{psutil.virtual_memory().percent}%</code> "
        f"(Bot RSS: <code>{_fmt_size(mem)}</code>)\n"
        f"\u251c <b>Disk Usage:</b> <code>{disk if disk >= 0 else '?'}%</code>\n"
        f"\u2514 <b>Uptime:</b> <code>{_fmt_uptime(time.time() - START_TIME)}</code>"
        "</blockquote>\n"
        "<b>Connectivity &amp; Binaries</b>\n"
        "<blockquote>"
        f"\u251c <b>Telegram RTT:</b> <code>{ping_str}</code> \u2022 <b>MongoDB:</b> <code>{db_ms}</code>\n"
        f"\u251c <b>WZGram Session:</b> <code>{sess_backend}</code>\n"
        f"\u251c <b>Media DCs:</b> <code>{', '.join(f'DC{d}' for d in dcs) or 'none'}</code>\n"
        f"\u2514 <b>Binaries:</b> mediainfo {mi_ok} \u2022 ffprobe {fp_ok}"
        "</blockquote>"
    )
    rows = [
        [
            InlineKeyboardButton(_tab_label("\U0001F5A5 System Health", True), callback_data="adm:server"),
            InlineKeyboardButton("\U0001F4DC Live Logs", callback_data="adm:logs:20"),
        ],
        [
            InlineKeyboardButton("\U0001F3D3 Ping Test", callback_data="adm:ping"),
            InlineKeyboardButton("\U0001F9F9 GC & Clean Temp", callback_data="adm:gc_clean"),
        ],
        [
            InlineKeyboardButton("\U0001F50C Reset DC Sessions", callback_data="adm:dc_reset:server"),
            InlineKeyboardButton("\U0001F504 Refresh", callback_data="adm:server"),
        ],
        [InlineKeyboardButton("\U0001F39B Dashboard", callback_data="adm:home")],
    ]
    return text, InlineKeyboardMarkup(rows)


def _build_power_view() -> tuple[str, InlineKeyboardMarkup]:
    """Build Power & Maintenance view with two-step confirmation buttons."""
    text = (
        "<b>\U0001F6E0 Power &amp; Maintenance</b>\n\n"
        "<blockquote>"
        "\u251c <b>Git Update:</b> Pulls latest commits &amp; installs updated requirements\n"
        "\u251c <b>Restart:</b> Cleanly closes DC sessions &amp; re-executes the process\n"
        "\u2514 <b>Shutdown:</b> Gracefully stops workers, scheduler, and health server"
        "</blockquote>\n"
        "<i>Every action below asks for confirmation before executing:</i>"
    )
    rows = [
        [InlineKeyboardButton("\U0001F504 Git Pull & Update Bot", callback_data="adm:confirm:update")],
        [
            InlineKeyboardButton("\u267b\ufe0f Restart Bot", callback_data="adm:confirm:restart"),
            InlineKeyboardButton("\U0001F6D1 Shutdown Bot", callback_data="adm:confirm:shutdown"),
        ],
        [InlineKeyboardButton("\U0001F39B Dashboard", callback_data="adm:home")],
    ]
    return text, InlineKeyboardMarkup(rows)


def _build_user_nav_kb(user_id: Optional[int], active_tab: str) -> InlineKeyboardMarkup:
    """Shared 3-tab navigation bar for user-facing /start, /help, and formats views."""
    rows = [
        [
            InlineKeyboardButton(
                _tab_label("\U0001F3E0 Home", active_tab == "start"),
                callback_data="usr:start",
            ),
            InlineKeyboardButton(
                _tab_label("\U0001F4D8 Guide", active_tab == "help"),
                callback_data="usr:help",
            ),
            InlineKeyboardButton(
                _tab_label("\U0001F39E Formats", active_tab == "formats"),
                callback_data="usr:formats",
            ),
        ],
    ]
    if user_id and user_id == ADMIN_ID:
        rows.append([
            InlineKeyboardButton("\U0001F39B Open Admin Control Center", callback_data="adm:home"),
        ])
    return InlineKeyboardMarkup(rows)


def _build_user_start_view(user_id: Optional[int]) -> tuple[str, InlineKeyboardMarkup]:
    """Build /start message and inline buttons."""
    text = (
        "<b>\U0001F3AC MediaInfo Bot</b>\n"
        "<i>Instant, zero-full-download stream inspection for Telegram media</i>\n\n"
        "<blockquote>"
        "\u251c \U0001F39E <b>Video:</b> Resolution, codec, bit depth &amp; HDR / Dolby Vision\n"
        "\u251c \u23f3 <b>Timing:</b> Exact duration, bitrate &amp; frame rate\n"
        "\u251c \U0001F50A <b>Audio:</b> Codecs, channel layout &amp; all track languages\n"
        "\u2514 \U0001F4AC <b>Subtitles:</b> Embedded subtitle tracks &amp; language tags"
        "</blockquote>\n"
        "Send or forward any video, audio, or document here, or reply to a file with <code>/info</code>."
    )
    return text, _build_user_nav_kb(user_id, "start")


def _build_user_help_view(user_id: Optional[int]) -> tuple[str, InlineKeyboardMarkup]:
    """Build /help message and inline buttons."""
    text = (
        "<b>\U0001F4D8 How It Works &amp; Commands</b>\n\n"
        "<blockquote>"
        "Instead of downloading gigabytes, the bot streams only small 4 KiB-aligned "
        "header/tail chunks over persistent per-datacenter WZGram MTProto sessions "
        "and inspects them with <code>mediainfo</code> and <code>ffprobe</code>."
        "</blockquote>\n"
        "<b>User Commands</b>\n"
        "\u2022 <code>/start</code> \u2014 Home screen\n"
        "\u2022 <code>/info</code> \u2014 Reply to any media file for a full report\n"
    )
    if user_id and user_id == ADMIN_ID:
        text += (
            "\n<b>Admin Shortcuts</b>\n"
            "<blockquote>"
            "\u251c <code>/panel</code>, <code>/config</code> \u2014 Control Center &amp; MongoDB Settings\n"
            "\u251c <code>/chats</code>, <code>/add</code>, <code>/remove</code> \u2014 Channel directory\n"
            "\u251c <code>/scan [chat] [limit]</code> \u2014 Backfill channel history\n"
            "\u251c <code>/queue</code>, <code>/stats</code>, <code>/server</code>, <code>/logs</code>\n"
            "\u2514 <code>/template</code>, <code>/setdelay</code>, <code>/broadcast</code>, <code>/update</code>"
            "</blockquote>"
        )
    return text, _build_user_nav_kb(user_id, "help")


def _build_user_formats_view(user_id: Optional[int]) -> tuple[str, InlineKeyboardMarkup]:
    """Build Supported Formats overview."""
    text = (
        "<b>\U0001F39E Supported Media &amp; Codecs</b>\n\n"
        "<blockquote>"
        "\u251c <b>Containers:</b> MKV, MP4, AVI, MOV, WebM, TS, M2TS, FLV, OGG, MP3, FLAC, WAV, M4A\n"
        "\u251c <b>Video Codecs:</b> HEVC (x265), AVC (x264), AV1, VP9, VP8, MPEG-4, MPEG-2, VC-1, ProRes, WMV\n"
        "\u251c <b>HDR Profiles:</b> Dolby Vision (DV), HDR10, HDR10+, HLG, 10-bit &amp; 12-bit\n"
        "\u251c <b>Audio:</b> AAC, E-AC-3, AC-3, TrueHD, DTS, Opus, FLAC, MP3 (40+ ISO languages)\n"
        "\u2514 <b>Subtitles:</b> SRT, ASS/SSA, PGS, VobSub, WebVTT, DVB/DVD"
        "</blockquote>"
    )
    return text, _build_user_nav_kb(user_id, "formats")


# --- USER CALLBACK ROUTER ---
@app.on_callback_query(filters.regex(r"^usr:"))
async def user_callback_router(_, query):
    uid = query.from_user.id if query.from_user else None
    action = query.data.split(":", 1)[1]
    if action == "start":
        text, kb = _build_user_start_view(uid)
    elif action == "help":
        text, kb = _build_user_help_view(uid)
    elif action == "formats":
        text, kb = _build_user_formats_view(uid)
    else:
        return await query.answer()
    await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
    await query.answer()


# --- HELPER FOR SCANNING A CHANNEL ---
async def _execute_channel_scan(chat_id: int, limit: int, min_id: int = 0) -> tuple[int, int]:
    """Scan up to `limit` messages in `chat_id` and queue uncaptioned ones in chronological order."""
    limit = max(1, min(int(limit), 2000))
    to_queue = []
    skipped = 0
    async for msg in app.get_chat_history(chat_id, limit=limit):
        if min_id and getattr(msg, "id", 0) <= min_id:
            break
        if getattr(msg, "video_note", None) is not None and not (
            getattr(msg, "video", None)
            or getattr(msg, "document", None)
            or getattr(msg, "animation", None)
            or getattr(msg, "audio", None)
            or getattr(msg, "voice", None)
        ):
            continue
        if _media_of(msg) is None:
            continue
        if caption_has_media_info(msg.caption or ''):
            skipped += 1
            continue
        to_queue.append(msg)

    to_queue.reverse()
    queued = 0
    for msg in to_queue:
        if _enqueue_channel_message(chat_id, msg):
            queued += 1
    return queued, skipped


async def _authorize_chat_and_reply(message, chat_id: int):
    """Add `chat_id` to `authorized_chats`, persist in MongoDB, and open its Channel Inspector."""
    already = chat_id in authorized_chats
    authorized_chats.add(chat_id)
    _chat_title_cache.pop(chat_id, None)
    if not already:
        await save_runtime_settings(allowed_chats=sorted(authorized_chats))
    text, kb = await _build_chat_detail_view(chat_id, 0)
    banner = (
        f"\u2139\ufe0f <code>{chat_id}</code> is already authorized.\n\n"
        if already
        else f"\u2705 Authorized <code>{chat_id}</code> and saved to MongoDB.\n\n"
    )
    await message.reply_text(
        banner + text,
        parse_mode=ParseMode.HTML,
        reply_markup=kb,
    )


# --- ADMIN CALLBACK ROUTER ---
@app.on_callback_query(filters.regex(r"^adm:"))
async def admin_callback_router(_, query):
    global CAPTION_TEMPLATE, EDIT_DELAY, ENABLE_TAIL_PROBE, FULL_REPORT
    global AUTO_CAPTION_PAUSED, STREAM_CONCURRENCY, CHANNEL_CONCURRENCY
    global PROBE_TIMEOUT, STREAM_TIMEOUT, STREAM_RETRIES, TAIL_SIZE_MB, TAIL_SIZE
    global ERROR_THRESHOLD, _ALERT_COOLDOWN, MAX_RETRY_ATTEMPTS, LOG_LEVEL, USE_MONGO_SESSION
    global stream_semaphore, channel_semaphore

    if not query.from_user or query.from_user.id != ADMIN_ID:
        return await query.answer("\u26d4 Access denied: Admin only.", show_alert=True)

    parts = query.data.split(":")
    cmd = parts[1] if len(parts) > 1 else "home"

    if cmd == "noop":
        return await query.answer()

    if cmd == "close":
        _admin_input_state.pop(ADMIN_ID, None)
        await _delete_quietly(query.message)
        return await query.answer("Closed")

    if cmd == "cancel_input":
        _admin_input_state.pop(ADMIN_ID, None)
        text, kb = await _build_admin_home()
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer("Cancelled")

    if cmd == "home":
        _admin_input_state.pop(ADMIN_ID, None)
        text, kb = await _build_admin_home()
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer()

    # --- TABBED SETTINGS & MONGODB CONFIG HUB ---
    if cmd in ("db_config", "toggles", "cfg_tab"):
        _admin_input_state.pop(ADMIN_ID, None)
        tab = parts[2] if len(parts) > 2 else "probing"
        text, kb = _build_settings_hub_view(tab)
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer()

    if cmd == "cfg_step":
        key = parts[2]
        delta = float(parts[3])
        tab = parts[4] if len(parts) > 4 else "probing"
        if key == "probe_timeout":
            PROBE_TIMEOUT = max(3.0, min(120.0, PROBE_TIMEOUT + delta))
            await save_runtime_settings(probe_timeout=PROBE_TIMEOUT)
            note = f"probe_timeout = {PROBE_TIMEOUT:.0f}s"
        elif key == "stream_timeout":
            STREAM_TIMEOUT = max(10.0, min(600.0, STREAM_TIMEOUT + delta))
            await save_runtime_settings(stream_timeout=STREAM_TIMEOUT)
            note = f"stream_timeout = {STREAM_TIMEOUT:.0f}s"
        elif key == "stream_retries":
            STREAM_RETRIES = max(1, min(10, int(STREAM_RETRIES + delta)))
            await save_runtime_settings(stream_retries=STREAM_RETRIES)
            note = f"stream_retries = {STREAM_RETRIES}"
        elif key == "tail_size_mb":
            TAIL_SIZE_MB = max(1, min(16, int(TAIL_SIZE_MB + delta)))
            TAIL_SIZE = TAIL_SIZE_MB * 1024 * 1024
            await save_runtime_settings(tail_size_mb=TAIL_SIZE_MB)
            note = f"tail_size_mb = {TAIL_SIZE_MB} MB"
        elif key == "max_retry_attempts":
            MAX_RETRY_ATTEMPTS = max(1, min(20, int(MAX_RETRY_ATTEMPTS + delta)))
            await save_runtime_settings(max_retry_attempts=MAX_RETRY_ATTEMPTS)
            note = f"max_retry_attempts = {MAX_RETRY_ATTEMPTS}"
            tab = "pacing"
        elif key == "error_threshold":
            ERROR_THRESHOLD = max(1, min(25, int(ERROR_THRESHOLD + delta)))
            await save_runtime_settings(error_threshold=ERROR_THRESHOLD)
            note = f"error_threshold = {ERROR_THRESHOLD}"
            tab = "pacing"
        else:
            return await query.answer()
        text, kb = _build_settings_hub_view(tab)
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer(f"Saved: {note}")

    if cmd == "cfg_cycle_log":
        levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
        idx = (levels.index(LOG_LEVEL) + 1) % len(levels) if LOG_LEVEL in levels else 1
        LOG_LEVEL = levels[idx]
        logging.getLogger().setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
        await save_runtime_settings(log_level=LOG_LEVEL)
        text, kb = _build_settings_hub_view("system")
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer(f"Log level: {LOG_LEVEL}")

    if cmd == "cfg_edit_prompt":
        _admin_input_state[ADMIN_ID] = {"mode": "edit_cfg", "ts": time.time()}
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("\u274c Cancel", callback_data="adm:cfg_tab:system")]])
        await _safe_edit(
            query.message,
            "<b>\u270f\ufe0f Edit MongoDB Config Key</b>\n\n"
            "Send <code>key = value</code> in your next message to update any setting in MongoDB.\n\n"
            "<blockquote>"
            "<code>edit_delay</code>, <code>probe_timeout</code>, <code>stream_timeout</code>, "
            "<code>stream_retries</code>, <code>stream_concurrency</code>, <code>channel_concurrency</code>, "
            "<code>enable_tail_probe</code>, <code>tail_size_mb</code>, <code>full_report</code>, "
            "<code>paused</code>, <code>error_threshold</code>, <code>alert_cooldown</code>, "
            "<code>max_retry_attempts</code>, <code>log_level</code>, <code>gc_gen0</code>, "
            "<code>gc_gen1</code>, <code>gc_gen2</code>, <code>use_mongo_session</code>"
            "</blockquote>\n"
            "<i>Example:</i> <code>alert_cooldown = 900</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )
        return await query.answer()

    if cmd == "cfg_export":
        await query.answer("Exporting MongoDB config\u2026")
        snap = _current_config_snapshot()
        payload = json.dumps(snap, indent=2, ensure_ascii=False).encode("utf-8")
        buf = io.BytesIO(payload)
        buf.name = "mediainfo_bot_config.json"
        try:
            await query.message.reply_document(
                buf,
                caption="<b>\U0001F5C4 Active MongoDB Configuration Snapshot</b>",
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            await query.answer(f"Export failed: {e}"[:180], show_alert=True)
        return

    if cmd == "cfg_reset_confirm":
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("\u2705 Yes, Reset to .env", callback_data="adm:cfg_reset"),
                InlineKeyboardButton("\u274c Cancel", callback_data="adm:cfg_tab:system"),
            ]
        ])
        await _safe_edit(
            query.message,
            "<b>\u26a0\ufe0f Confirm Reset All Configs</b>\n\n"
            "<blockquote>Reset all runtime settings in MongoDB back to the initial "
            "<code>.env</code> defaults? (Authorized channels will be preserved.)</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )
        return await query.answer()

    if cmd == "cfg_reset":
        defaults = cfg.env_defaults_dict()
        defaults["allowed_chats"] = sorted(authorized_chats | set(ALLOWED_CHATS))
        _apply_config_dict(defaults)
        await save_runtime_settings(**defaults)
        if not AUTO_CAPTION_PAUSED:
            _resume_all_channel_workers()
        text, kb = _build_settings_hub_view("system")
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer("All configs reset to .env defaults")

    # --- CHANNELS & CHANNEL INSPECTOR ---
    if cmd == "chats":
        page = int(parts[2]) if len(parts) > 2 and parts[2].lstrip("-").isdigit() else 0
        text, kb = await _build_chats_view(page)
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer()

    if cmd == "chat_view":
        cid = int(parts[2])
        page = int(parts[3]) if len(parts) > 3 and parts[3].lstrip("-").isdigit() else 0
        if cid not in authorized_chats:
            text, kb = await _build_chats_view(page)
            await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
            return await query.answer("Channel is no longer authorized.")
        text, kb = await _build_chat_detail_view(cid, page)
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer()

    if cmd == "chat_clear_q":
        cid = int(parts[2])
        page = int(parts[3]) if len(parts) > 3 else 0
        dropped = len(channel_queues.get(cid, []))
        channel_queues.pop(cid, None)
        text, kb = await _build_chat_detail_view(cid, page)
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer(f"Cleared {dropped} queued item(s)")

    if cmd == "chat_rm_confirm":
        cid = int(parts[2])
        page = int(parts[3]) if len(parts) > 3 else 0
        title = await _get_chat_label(cid)
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("\u2705 Yes, Remove", callback_data=f"adm:chat_rm:{cid}:{page}"),
                InlineKeyboardButton("\u274c Cancel", callback_data=f"adm:chat_view:{cid}:{page}"),
            ]
        ])
        await _safe_edit(
            query.message,
            "<b>\u26a0\ufe0f Confirm Channel Removal</b>\n\n"
            f"<blockquote>Remove <b>{html.escape(title)}</b> (<code>{cid}</code>) from authorized channels "
            "and clear its pending queue?</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )
        return await query.answer()

    if cmd == "chat_rm":
        cid = int(parts[2])
        page = int(parts[3]) if len(parts) > 3 else 0
        authorized_chats.discard(cid)
        channel_queues.pop(cid, None)
        _chat_title_cache.pop(cid, None)
        await save_runtime_settings(allowed_chats=sorted(authorized_chats))
        text, kb = await _build_chats_view(page)
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer(f"Removed {cid}")

    if cmd == "chat_add_id":
        cid = int(parts[2])
        authorized_chats.add(cid)
        _chat_title_cache.pop(cid, None)
        await save_runtime_settings(allowed_chats=sorted(authorized_chats))
        await query.answer(f"Authorized {cid}!", show_alert=True)
        text, kb = await _build_chat_detail_view(cid, 0)
        return await query.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)

    if cmd == "chat_add_prompt":
        _admin_input_state[ADMIN_ID] = {"mode": "add_chat", "ts": time.time()}
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("\u274c Cancel", callback_data="adm:chats:0")]
        ])
        await _safe_edit(
            query.message,
            "<b>\u2795 Add Authorized Channel</b>\n\n"
            "<blockquote>"
            "\u251c <b>Option 1:</b> Send a numeric Chat ID (<code>-1001234567890</code>)\n"
            "\u251c <b>Option 2:</b> Send a public username (<code>@mychannel</code>)\n"
            "\u2514 <b>Option 3:</b> Forward any message from the channel here"
            "</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )
        return await query.answer()

    # --- QUEUES & RETRIES ---
    if cmd == "queue":
        text, kb = await _build_queue_view()
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer()

    if cmd == "toggle_pause":
        AUTO_CAPTION_PAUSED = not AUTO_CAPTION_PAUSED
        await save_runtime_settings(paused=AUTO_CAPTION_PAUSED)
        if not AUTO_CAPTION_PAUSED:
            _resume_all_channel_workers()
        origin = parts[2] if len(parts) > 2 else "home"
        if origin == "queue":
            text, kb = await _build_queue_view()
        else:
            text, kb = await _build_admin_home()
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer(
            "Auto-captioning paused" if AUTO_CAPTION_PAUSED else "Auto-captioning resumed"
        )

    if cmd == "queue_retry":
        await query.answer("Running retry queue\u2026")
        try:
            await retry_collection.update_many({}, {"$set": {"next_retry": _utcnow()}})
            await process_retry_queue()
        except Exception as e:
            return await query.answer(f"DB error: {e}"[:180], show_alert=True)
        text, kb = await _build_queue_view()
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return

    if cmd == "queue_clear_confirm":
        pending = sum(len(q) for q in channel_queues.values())
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(f"\u2705 Yes, Clear ({pending})", callback_data="adm:queue_clear"),
                InlineKeyboardButton("\u274c Cancel", callback_data="adm:queue"),
            ]
        ])
        await _safe_edit(
            query.message,
            "<b>\u26a0\ufe0f Confirm Clear Pending Queue</b>\n\n"
            f"<blockquote>Drop all <b>{pending}</b> pending item(s) waiting in memory?</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )
        return await query.answer()

    if cmd == "queue_clear":
        dropped = sum(len(q) for q in channel_queues.values())
        for q in channel_queues.values():
            q.clear()
        text, kb = await _build_queue_view()
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer(f"Cleared {dropped} pending items")

    if cmd == "retry_clear_confirm":
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("\u2705 Yes, Clear Retry DB", callback_data="adm:retry_clear"),
                InlineKeyboardButton("\u274c Cancel", callback_data="adm:queue"),
            ]
        ])
        await _safe_edit(
            query.message,
            "<b>\u26a0\ufe0f Confirm Clear Retry Queue</b>\n\n"
            "<blockquote>Permanently delete all queued failed edits from MongoDB?</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )
        return await query.answer()

    if cmd == "retry_clear":
        try:
            res = await retry_collection.delete_many({})
            deleted = res.deleted_count
        except Exception as e:
            return await query.answer(f"DB error: {e}"[:180], show_alert=True)
        text, kb = await _build_queue_view()
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer(f"Deleted {deleted} retry records")

    # --- STATISTICS ---
    if cmd == "stats":
        text, kb = await _build_stats_view()
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer()

    if cmd == "stats_reset_session":
        for k in _runtime:
            _runtime[k] = 0
        text, kb = await _build_stats_view()
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer("Session counters reset")

    if cmd == "stats_reset_db_confirm":
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("\u2705 Yes, Wipe DB Stats", callback_data="adm:stats_reset_db"),
                InlineKeyboardButton("\u274c Cancel", callback_data="adm:stats"),
            ]
        ])
        await _safe_edit(
            query.message,
            "<b>\u26a0\ufe0f Confirm Reset All-Time Statistics</b>\n\n"
            "<blockquote>Clear all per-channel processed counts in MongoDB?</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )
        return await query.answer()

    if cmd == "stats_reset_db":
        try:
            await stats_collection.delete_many({})
        except Exception as e:
            return await query.answer(f"DB error: {e}"[:180], show_alert=True)
        text, kb = await _build_stats_view()
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer("All-time DB stats cleared")

    # --- EDIT DELAY ---
    if cmd == "delay":
        text, kb = _build_delay_view()
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer()

    if cmd == "delay_set":
        val = max(1.0, min(60.0, float(parts[2])))
        EDIT_DELAY = round(val, 1)
        await save_runtime_settings(edit_delay=EDIT_DELAY)
        text, kb = _build_delay_view()
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer(f"Edit delay set to {EDIT_DELAY:.1f}s")

    if cmd == "delay_adj":
        delta = float(parts[2])
        origin = parts[3] if len(parts) > 3 else "delay"
        EDIT_DELAY = round(max(1.0, min(60.0, EDIT_DELAY + delta)), 1)
        await save_runtime_settings(edit_delay=EDIT_DELAY)
        if origin == "pacing":
            text, kb = _build_settings_hub_view("pacing")
        else:
            text, kb = _build_delay_view()
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer(f"Edit delay: {EDIT_DELAY:.1f}s")

    if cmd == "delay_custom":
        _admin_input_state[ADMIN_ID] = {"mode": "set_delay", "ts": time.time()}
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("\u274c Cancel", callback_data="adm:delay")]
        ])
        await _safe_edit(
            query.message,
            "<b>\u270f\ufe0f Set Custom Edit Delay</b>\n\n"
            f"<blockquote>Current delay: <code>{EDIT_DELAY:.1f}s</code>\n"
            "Send a number between <code>1.0</code> and <code>60.0</code> in your next message:</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )
        return await query.answer()

    # --- CAPTION STUDIO ---
    if cmd == "tpl":
        text, kb = _build_template_view()
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer()

    if cmd == "tpl_preset":
        preset_key = parts[2]
        if preset_key == "env":
            CAPTION_TEMPLATE = CAPTION_TEMPLATE_ENV
            label = "Env Default"
        elif preset_key in TEMPLATE_PRESETS:
            label, CAPTION_TEMPLATE = TEMPLATE_PRESETS[preset_key]
        else:
            return await query.answer("Unknown preset", show_alert=True)
        await save_runtime_settings(caption_template=CAPTION_TEMPLATE)
        text, kb = _build_template_view()
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer(f"Applied preset: {label}")

    if cmd == "tpl_custom":
        _admin_input_state[ADMIN_ID] = {"mode": "set_template", "ts": time.time()}
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("\u274c Cancel", callback_data="adm:tpl")]
        ])
        placeholders = (
            "{title} {video_line} {duration} {audio} {subtitle} "
            "{size} {bitrate} {fps} {resolution} {codec} {container} {filename}"
        )
        await _safe_edit(
            query.message,
            "<b>\u270f\ufe0f Set Custom Caption Template</b>\n\n"
            "Send the new HTML caption template in your next message.\n\n"
            f"<b>Placeholders:</b>\n<code>{html.escape(placeholders)}</code>\n\n"
            "<b>Current Template:</b>\n"
            f"<pre>{html.escape(CAPTION_TEMPLATE)}</pre>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )
        return await query.answer()

    # --- TOGGLES & CONCURRENCY ---
    if cmd == "tog":
        target = parts[2]
        tab = parts[3] if len(parts) > 3 else "probing"
        if target == "tail_probe":
            ENABLE_TAIL_PROBE = not ENABLE_TAIL_PROBE
            await save_runtime_settings(enable_tail_probe=ENABLE_TAIL_PROBE)
            msg_note = f"Tail Probe {'enabled' if ENABLE_TAIL_PROBE else 'disabled'}"
            text, kb = _build_settings_hub_view("probing")
        elif target == "full_report":
            FULL_REPORT = not FULL_REPORT
            await save_runtime_settings(full_report=FULL_REPORT)
            msg_note = f"Technical Report button {'enabled' if FULL_REPORT else 'disabled'}"
            text, kb = _build_settings_hub_view(tab if tab in ("probing", "pacing", "system") else "system")
        elif target == "pause":
            AUTO_CAPTION_PAUSED = not AUTO_CAPTION_PAUSED
            await save_runtime_settings(paused=AUTO_CAPTION_PAUSED)
            if not AUTO_CAPTION_PAUSED:
                _resume_all_channel_workers()
            msg_note = "Auto-captioning paused" if AUTO_CAPTION_PAUSED else "Auto-captioning active"
            text, kb = _build_settings_hub_view(tab if tab in ("probing", "pacing", "system") else "pacing")
        elif target == "mongo_session":
            USE_MONGO_SESSION = not USE_MONGO_SESSION
            await save_runtime_settings(use_mongo_session=USE_MONGO_SESSION)
            msg_note = f"Session store: {'MongoDB' if USE_MONGO_SESSION else 'File'} (applies on restart)"
            text, kb = _build_settings_hub_view("system")
        else:
            return await query.answer()
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer(msg_note)

    if cmd == "conc":
        kind = parts[2]
        delta = int(parts[3])
        tab = parts[4] if len(parts) > 4 else ("probing" if kind == "stream" else "pacing")
        if kind == "stream":
            STREAM_CONCURRENCY = max(1, min(8, STREAM_CONCURRENCY + delta))
            stream_semaphore = asyncio.Semaphore(STREAM_CONCURRENCY)
            app.max_concurrent_transmissions = max(2, STREAM_CONCURRENCY)
            await save_runtime_settings(stream_concurrency=STREAM_CONCURRENCY)
            note = f"Stream concurrency: {STREAM_CONCURRENCY}"
        else:
            CHANNEL_CONCURRENCY = max(1, min(8, CHANNEL_CONCURRENCY + delta))
            channel_semaphore = asyncio.Semaphore(CHANNEL_CONCURRENCY)
            await save_runtime_settings(channel_concurrency=CHANNEL_CONCURRENCY)
            note = f"Channel concurrency: {CHANNEL_CONCURRENCY}"
        text, kb = _build_settings_hub_view(tab)
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer(note)

    if cmd == "dc_reset":
        origin = parts[2] if len(parts) > 2 else "probing"
        await dc_stream.close_sessions()
        dc_stream.clear_flood()
        if origin == "server":
            text, kb = await _build_server_view()
        else:
            text, kb = _build_settings_hub_view("probing")
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer("Closed cached DC sessions & cleared flood timers")

    # --- SCANNER ---
    if cmd == "scan_menu":
        text, kb = await _build_scan_menu_view()
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer()

    if cmd == "scan_pick":
        cid = int(parts[2])
        text, kb = await _build_scan_pick_view(cid)
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer()

    if cmd in ("scan_run", "scan_since"):
        cid = int(parts[2])
        limit = int(parts[3]) if len(parts) > 3 else 100
        if cid not in authorized_chats:
            return await query.answer("Channel is not authorized.", show_alert=True)
        min_id = await get_last_id(cid) if cmd == "scan_since" else 0
        await query.answer(f"Scanning up to {limit} messages\u2026")
        await _safe_edit(
            query.message,
            f"\U0001F50E <b>Scanning up to {limit} messages in <code>{cid}</code>\u2026</b>",
            parse_mode=ParseMode.HTML,
        )
        try:
            queued, skipped = await _execute_channel_scan(cid, limit, min_id=min_id)
        except Exception as e:
            text, kb = await _build_scan_pick_view(cid)
            await _safe_edit(
                query.message,
                f"\u274c <b>Scan failed:</b> <code>{html.escape(str(e))[:200]}</code>\n\n{text}",
                parse_mode=ParseMode.HTML,
                reply_markup=kb,
            )
            return
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("\U0001F4E5 View Queue", callback_data="adm:queue"),
                InlineKeyboardButton("\U0001F4E1 Channel Inspector", callback_data=f"adm:chat_view:{cid}:0"),
            ],
            [InlineKeyboardButton("\U0001F39B Dashboard", callback_data="adm:home")],
        ])
        await _safe_edit(
            query.message,
            f"<b>\u2705 Scan Complete</b>\n\n"
            "<blockquote>"
            f"\u251c <b>Channel:</b> <code>{cid}</code>\n"
            f"\u251c <b>Queued for Captioning:</b> <code>{queued}</code>\n"
            f"\u2514 <b>Skipped (Already Captioned):</b> <code>{skipped}</code>"
            "</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )
        return

    # --- LOGS ---
    if cmd in ("logs", "logs_err"):
        n = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 20
        text, kb = _build_logs_view(n, errors_only=(cmd == "logs_err"))
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer()

    if cmd == "logs_clear":
        _log_buffer.clear()
        text, kb = _build_logs_view(20)
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer("Log buffer cleared")

    if cmd == "logs_dl":
        if not _log_buffer:
            return await query.answer("Log buffer is empty.", show_alert=True)
        await query.answer("Uploading log file\u2026")
        buf = io.BytesIO("\n".join(_log_buffer).encode("utf-8", errors="replace"))
        buf.name = f"mediainfo_bot_{int(time.time())}.log"
        try:
            await query.message.reply_document(
                buf,
                caption=f"\U0001F4DC <b>Log dump</b> (<code>{len(_log_buffer)}</code> lines)",
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            await query.answer(f"Upload failed: {e}"[:180], show_alert=True)
        return

    # --- SERVER & DIAGNOSTICS ---
    if cmd == "server":
        text, kb = await _build_server_view()
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer()

    if cmd == "ping":
        t0 = time.monotonic()
        await query.answer("Measuring latency\u2026")
        rtt = (time.monotonic() - t0) * 1000
        text, kb = await _build_server_view(ping_ms=rtt)
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return

    if cmd == "gc_clean":
        collected = gc.collect()
        _clean_tmp_dir()
        _prune_caches()
        text, kb = await _build_server_view()
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer(f"GC freed {collected} objects & cleaned temp dir")

    # --- BROADCAST ---
    if cmd == "bcast_prompt":
        _admin_input_state[ADMIN_ID] = {"mode": "broadcast", "ts": time.time()}
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("\u274c Cancel", callback_data="adm:cancel_input")]
        ])
        await _safe_edit(
            query.message,
            "<b>\U0001F4E2 Broadcast Message</b>\n\n"
            f"<blockquote>Send the message text you want to broadcast to all "
            f"<b>{len(authorized_chats)}</b> authorized channel(s):</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )
        return await query.answer()

    # --- POWER & MAINTENANCE ---
    if cmd == "power":
        text, kb = _build_power_view()
        await _safe_edit(query.message, text, parse_mode=ParseMode.HTML, reply_markup=kb)
        return await query.answer()

    if cmd == "confirm":
        act = parts[2]
        labels = {
            "update": ("Git Pull & Restart", "\U0001F504 Pull latest code from Git and restart?"),
            "restart": ("Restart Bot", "\u267b\ufe0f Restart the bot process now?"),
            "shutdown": ("Shutdown Bot", "\U0001F6D1 Shut down the bot process completely?"),
        }
        if act not in labels:
            return await query.answer()
        btn_label, desc = labels[act]
        kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(f"\u2705 {btn_label}", callback_data=f"adm:exec:{act}"),
                InlineKeyboardButton("\u274c Cancel", callback_data="adm:power"),
            ]
        ])
        await _safe_edit(
            query.message,
            f"<b>\u26a0\ufe0f Confirmation Required</b>\n\n<blockquote>{desc}</blockquote>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )
        return await query.answer()

    if cmd == "exec":
        act = parts[2]
        if act == "restart":
            await query.answer("Restarting\u2026")
            await _safe_edit(query.message, "\u267b\ufe0f <b>Restarting bot\u2026</b>", parse_mode=ParseMode.HTML)
            await _shutdown_cleanly()
            os.execv(sys.executable, [sys.executable] + sys.argv)
        elif act == "shutdown":
            await query.answer("Shutting down\u2026")
            await _safe_edit(query.message, "\U0001F6D1 <b>Shutting down bot\u2026</b>", parse_mode=ParseMode.HTML)
            await _shutdown_cleanly()
            asyncio.get_running_loop().call_soon(os._exit, 0)
        elif act == "update":
            await query.answer("Running git pull\u2026")
            await _perform_git_update(query.message)
        return


def _parse_bool_str(val: str) -> bool:
    low = val.strip().lower()
    if low in ("1", "true", "yes", "on", "y"):
        return True
    if low in ("0", "false", "no", "off", "n"):
        return False
    raise ValueError(f"Expected boolean (true/false), got {val!r}")


# --- CONVERSATIONAL INPUT HANDLER FOR ADMIN PANEL PROMPTS ---
@app.on_message(
    filters.private
    & filters.user(ADMIN_ID)
    & ~filters.command([
        "start", "help", "info", "panel", "admin", "settings", "config",
        "add", "remove", "chats", "stats", "broadcast", "queue",
        "clearqueue", "retrynow", "setdelay", "logs", "scan",
        "template", "ping", "server", "restart", "shutdown", "update",
    ])
    & ~_MEDIA_FILTER
)
async def admin_interactive_input_handler(_, message):
    global EDIT_DELAY, CAPTION_TEMPLATE

    st = _admin_input_state.get(ADMIN_ID)
    if not st:
        return

    mode = st.get("mode")
    _admin_input_state.pop(ADMIN_ID, None)

    if mode == "add_chat":
        cid: Optional[int] = None
        fwd_chat = _get_forwarded_chat(message)
        if fwd_chat:
            cid = fwd_chat.id
        elif message.text:
            token = message.text.strip().split()[0]
            try:
                cid = int(token)
            except ValueError:
                try:
                    chat = await app.get_chat(token)
                    cid = chat.id
                except Exception:
                    cid = None
        if cid is None:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("\u2b05\ufe0f Channels", callback_data="adm:chats:0")]])
            return await message.reply_text(
                "\u274c Could not resolve that chat. Send a numeric ID like "
                "<code>-100123456789</code>, a <code>@username</code>, or forward a message from the channel.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb,
            )
        await _authorize_chat_and_reply(message, cid)

    elif mode == "set_delay":
        try:
            val = float((message.text or "").strip())
            if not 1.0 <= val <= 60.0:
                raise ValueError("out of range")
        except ValueError:
            text, kb = _build_delay_view()
            return await message.reply_text(
                f"\u274c Invalid delay. Must be a number between 1.0 and 60.0.\n\n{text}",
                parse_mode=ParseMode.HTML,
                reply_markup=kb,
            )
        EDIT_DELAY = round(val, 1)
        await save_runtime_settings(edit_delay=EDIT_DELAY)
        text, kb = _build_delay_view()
        await message.reply_text(
            f"\u2705 Edit delay updated to <b>{EDIT_DELAY:.1f}s</b> (saved in MongoDB).\n\n{text}",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )

    elif mode == "set_template":
        new_tpl = message.text or ""
        err = cfg.validate_template(new_tpl)
        if err:
            text, kb = _build_template_view()
            return await message.reply_text(
                f"\u274c <b>Invalid template:</b> <code>{html.escape(err)}</code>\n\n{text}",
                parse_mode=ParseMode.HTML,
                reply_markup=kb,
            )
        CAPTION_TEMPLATE = new_tpl
        await save_runtime_settings(caption_template=CAPTION_TEMPLATE)
        text, kb = _build_template_view()
        await message.reply_text(
            f"\u2705 <b>Custom template saved to MongoDB!</b>\n\n{text}",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )

    elif mode == "edit_cfg":
        raw = (message.text or "").strip()
        if "=" not in raw:
            text, kb = _build_settings_hub_view("system")
            return await message.reply_text(
                "\u274c Invalid syntax. Use <code>key = value</code>.\n\n" + text,
                parse_mode=ParseMode.HTML,
                reply_markup=kb,
            )
        key, val_raw = [x.strip() for x in raw.split("=", 1)]
        key = key.lower()
        snap = _current_config_snapshot()
        try:
            if key in ("edit_delay", "probe_timeout", "stream_timeout"):
                snap[key] = float(val_raw)
            elif key in (
                "stream_retries", "stream_concurrency", "channel_concurrency",
                "tail_size_mb", "error_threshold", "alert_cooldown",
                "max_retry_attempts", "gc_gen0", "gc_gen1", "gc_gen2",
            ):
                snap[key] = int(val_raw)
            elif key in ("enable_tail_probe", "full_report", "paused", "use_mongo_session"):
                snap[key] = _parse_bool_str(val_raw)
            elif key == "log_level":
                lvl = val_raw.upper()
                if lvl not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
                    raise ValueError("Invalid log_level")
                snap[key] = lvl
            else:
                raise ValueError(f"Unsupported key: {key}")
        except Exception as e:
            text, kb = _build_settings_hub_view("system")
            return await message.reply_text(
                f"\u274c <b>Error:</b> <code>{html.escape(str(e))}</code>\n\n{text}",
                parse_mode=ParseMode.HTML,
                reply_markup=kb,
            )
        _apply_config_dict(snap)
        await save_runtime_settings(**snap)
        if not AUTO_CAPTION_PAUSED:
            _resume_all_channel_workers()
        text, kb = _build_settings_hub_view("system")
        await message.reply_text(
            f"\u2705 Saved <code>{html.escape(key)} = {html.escape(str(snap[key]))}</code> in MongoDB.\n\n{text}",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )

    elif mode == "broadcast":
        bcast_text = (message.text or "").strip()
        if not bcast_text:
            text, kb = await _build_admin_home()
            return await message.reply_text(
                "\u274c Broadcast text cannot be empty.",
                parse_mode=ParseMode.HTML,
                reply_markup=kb,
            )
        status = await message.reply_text("\u23f3 Broadcasting\u2026")
        sent, failed = await _run_broadcast(bcast_text)
        text, kb = await _build_admin_home()
        await _safe_edit(
            status,
            f"\u2705 <b>Broadcast complete</b> \u2014 Sent: <b>{sent}</b> | Failed: <b>{failed}</b>\n\n{text}",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )


# --- ADMIN SLASH COMMANDS (wired with the organized Inline Keyboards) ---

@app.on_message(filters.command(["panel", "admin", "settings"]) & filters.user(ADMIN_ID))
async def admin_panel_cmd(_, message):
    """Open the interactive Control Center."""
    text, kb = await _build_admin_home()
    await message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)


@app.on_message(filters.command("config") & filters.user(ADMIN_ID))
async def config_cmd(_, message):
    """Open the tabbed Settings & MongoDB Config Hub."""
    text, kb = _build_settings_hub_view("probing")
    await message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)


async def _resolve_chat_arg(message) -> Optional[int]:
    """Resolve the chat argument of /add and /remove."""
    if len(message.command) < 2:
        return message.chat.id
    token = message.command[1].strip()
    try:
        return int(token)
    except ValueError:
        pass
    try:
        chat = await app.get_chat(token)
        return chat.id
    except Exception as e:
        logger.warning(f"could not resolve chat '{token}': {e}")
        return None


@app.on_message(filters.command("add") & filters.user(ADMIN_ID))
async def add_chat(_, message):
    chat_id = await _resolve_chat_arg(message)
    if chat_id is None:
        return await message.reply_text(
            "\u274c Could not resolve that chat. Use a numeric id like "
            "<code>-100123456789</code>, a @username, or run /add inside the chat.",
            parse_mode=ParseMode.HTML,
        )
    await _authorize_chat_and_reply(message, chat_id)


@app.on_message(filters.command("remove") & filters.user(ADMIN_ID))
async def remove_chat(_, message):
    chat_id = await _resolve_chat_arg(message)
    if chat_id is None:
        return await message.reply_text("\u274c Could not resolve that chat.")
    if chat_id not in authorized_chats:
        return await message.reply_text("\u274c Chat ID not found in the list.")
    authorized_chats.discard(chat_id)
    channel_queues.pop(chat_id, None)
    _chat_title_cache.pop(chat_id, None)
    await save_runtime_settings(allowed_chats=sorted(authorized_chats))
    text, kb = await _build_chats_view(0)
    await message.reply_text(
        f"\u2705 Removed <code>{chat_id}</code> from authorized chats (saved in MongoDB).\n\n{text}",
        parse_mode=ParseMode.HTML,
        reply_markup=kb,
    )


@app.on_message(filters.command("chats") & filters.user(ADMIN_ID))
async def list_chats(_, message):
    text, kb = await _build_chats_view(0)
    await message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)


@app.on_message(filters.command("stats") & filters.user(ADMIN_ID))
async def stats_cmd(_, message):
    text, kb = await _build_stats_view()
    await message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)


async def _run_broadcast(text: str) -> tuple[int, int]:
    sent = failed = 0
    for chat_id in list(authorized_chats):
        try:
            await app.send_message(chat_id, text)
            sent += 1
        except FloodWait as e:
            await asyncio.sleep(e.value)
            try:
                await app.send_message(chat_id, text)
                sent += 1
            except Exception as err:
                logger.warning(f"Broadcast failed for {chat_id}: {err}")
                failed += 1
        except Exception as e:
            logger.warning(f"Broadcast failed for {chat_id}: {e}")
            failed += 1
        await asyncio.sleep(0.4)
    return sent, failed


@app.on_message(filters.command("broadcast") & filters.user(ADMIN_ID))
async def broadcast_cmd(_, message):
    if len(message.command) < 2:
        _admin_input_state[ADMIN_ID] = {"mode": "broadcast", "ts": time.time()}
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("\u274c Cancel", callback_data="adm:cancel_input")]])
        return await message.reply_text(
            "<b>\U0001F4E2 Broadcast Message</b>\n\nSend the message text in your next reply, "
            "or use <code>/broadcast Your message here</code>.",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )
    text = message.text.split(None, 1)[1]
    sent, failed = await _run_broadcast(text)
    await message.reply_text(
        f"\u2705 Broadcast done.\nSent: <b>{sent}</b> | Failed: <b>{failed}</b>",
        parse_mode=ParseMode.HTML,
    )


@app.on_message(filters.command("queue") & filters.user(ADMIN_ID))
async def queue_cmd(_, message):
    text, kb = await _build_queue_view()
    await message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)


@app.on_message(filters.command("clearqueue") & filters.user(ADMIN_ID))
async def clearqueue_cmd(_, message):
    dropped = sum(len(q) for q in channel_queues.values())
    for q in channel_queues.values():
        q.clear()
    text, kb = await _build_queue_view()
    await message.reply_text(
        f"\U0001F5D1 Cleared <b>{dropped}</b> pending item(s).\n\n{text}",
        parse_mode=ParseMode.HTML,
        reply_markup=kb,
    )


@app.on_message(filters.command("retrynow") & filters.user(ADMIN_ID))
async def retrynow_cmd(_, message):
    try:
        await retry_collection.update_many({}, {"$set": {"next_retry": _utcnow()}})
    except Exception as e:
        return await message.reply_text(f"\u274c Database error: {e}")
    status = await message.reply_text("\u23f3 Running the retry queue\u2026")
    await process_retry_queue()
    text, kb = await _build_queue_view()
    await _safe_edit(
        status,
        f"\u2705 <b>Retry pass finished.</b>\n\n{text}",
        parse_mode=ParseMode.HTML,
        reply_markup=kb,
    )


@app.on_message(filters.command("setdelay") & filters.user(ADMIN_ID))
async def setdelay_cmd(_, message):
    global EDIT_DELAY
    if len(message.command) < 2:
        text, kb = _build_delay_view()
        return await message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
    try:
        val = float(message.command[1])
    except ValueError:
        return await message.reply_text(
            "\u274c Invalid value. Use a number like <code>3.5</code>",
            parse_mode=ParseMode.HTML,
        )
    if not 1.0 <= val <= 60.0:
        return await message.reply_text("\u274c Delay must be between 1.0 and 60.0 seconds.")
    EDIT_DELAY = round(val, 1)
    await save_runtime_settings(edit_delay=EDIT_DELAY)
    text, kb = _build_delay_view()
    await message.reply_text(
        f"\u2705 Edit delay set to <b>{EDIT_DELAY:.1f}s</b> (saved in MongoDB)\n\n{text}",
        parse_mode=ParseMode.HTML,
        reply_markup=kb,
    )


@app.on_message(filters.command("logs") & filters.user(ADMIN_ID))
async def logs_cmd(_, message):
    try:
        n = int(message.command[1]) if len(message.command) > 1 else 20
        n = max(1, min(n, 200))
    except ValueError:
        n = 20
    text, kb = _build_logs_view(n)
    await message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)


@app.on_message(filters.command("scan") & filters.user(ADMIN_ID))
async def scan_cmd(_, message):
    """Backfill captions for older messages in a channel."""
    args = message.command[1:]
    if not args and message.chat.id == ADMIN_ID:
        text, kb = await _build_scan_menu_view()
        return await message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)

    chat_id: Optional[int] = None
    limit = 100

    if args:
        try:
            chat_id = int(args[0])
        except ValueError:
            chat_id = await _resolve_chat_arg(message)
    if chat_id is None:
        chat_id = message.chat.id
    if len(args) > 1:
        try:
            limit = max(1, min(int(args[1]), 2000))
        except ValueError:
            pass

    if chat_id not in authorized_chats:
        return await message.reply_text(
            f"\u274c <code>{chat_id}</code> is not authorized. Add it with /add first.",
            parse_mode=ParseMode.HTML,
        )

    status = await message.reply_text(f"\U0001F50E Scanning last {limit} messages\u2026")
    try:
        queued, skipped = await _execute_channel_scan(chat_id, limit)
    except Exception as e:
        return await _safe_edit(status, f"\u274c Scan failed: {html.escape(str(e))[:200]}")

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("\U0001F4E5 View Queue", callback_data="adm:queue"),
            InlineKeyboardButton("\U0001F39B Dashboard", callback_data="adm:home"),
        ]
    ])
    await _safe_edit(
        status,
        f"\u2705 Queued <b>{queued}</b> message(s), skipped <b>{skipped}</b> "
        f"already captioned.",
        parse_mode=ParseMode.HTML,
        reply_markup=kb,
    )


@app.on_message(filters.command("template") & filters.user(ADMIN_ID))
async def template_cmd(_, message):
    text, kb = _build_template_view()
    await message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)


@app.on_message(filters.command("ping") & filters.user(ADMIN_ID))
async def ping_cmd(_, message):
    started = time.monotonic()
    msg = await message.reply_text("\U0001F3D3 Pong\u2026")
    latency = (time.monotonic() - started) * 1000
    text, kb = await _build_server_view(ping_ms=latency)
    await _safe_edit(msg, text, parse_mode=ParseMode.HTML, reply_markup=kb)


@app.on_message(filters.command("start") & filters.private)
async def start(_, m):
    uid = m.from_user.id if m.from_user else None
    text, kb = _build_user_start_view(uid)
    await m.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)


@app.on_message(filters.command("help") & filters.private)
async def help_cmd(_, m):
    uid = m.from_user.id if m.from_user else None
    text, kb = _build_user_help_view(uid)
    await m.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)


@app.on_message(filters.command("server") & filters.user(ADMIN_ID))
async def server_cmd(_, m):
    text, kb = await _build_server_view()
    await m.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)


@app.on_message(filters.command("restart") & filters.user(ADMIN_ID))
async def restart_cmd(_, m):
    await m.reply_text("Restarting\u2026")
    await _shutdown_cleanly()
    os.execv(sys.executable, [sys.executable] + sys.argv)


@app.on_message(filters.command("shutdown") & filters.user(ADMIN_ID))
async def shutdown_cmd(_, m):
    await m.reply_text("Shutting down\u2026")
    await _shutdown_cleanly()
    asyncio.get_running_loop().call_soon(os._exit, 0)


async def _perform_git_update(status_msg):
    """Shared implementation of git pull + optional dependency install + restart."""
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("\U0001F39B Dashboard", callback_data="adm:home")]])
    try:
        rc, out_str, err_str = await _run_cmd_full(["git", "pull"], 120)
        combined = "\n".join(filter(None, [out_str, err_str])).strip()
        if rc != 0:
            return await _safe_edit(
                status_msg,
                f"\u274c <b>Git update failed (exit {rc}):</b>\n"
                f"<pre>{html.escape(combined[-600:] or 'Unknown git error')}</pre>",
                parse_mode=ParseMode.HTML,
                reply_markup=kb,
            )
        text = combined[-500:]
        if "Already up to date" in text or not text:
            return await _safe_edit(
                status_msg, "\u2705 Already up to date.", parse_mode=ParseMode.HTML, reply_markup=kb
            )
        if "requirements.txt" in text:
            pip_rc, pip_out, pip_err = await _run_cmd_full(
                [sys.executable, "-m", "pip", "install", "-r", "requirements.txt",
                 "--no-cache-dir", "-q"],
                300,
            )
            if pip_rc != 0:
                pip_msg = "\n".join(filter(None, [pip_out, pip_err])).strip()[-500:]
                return await _safe_edit(
                    status_msg,
                    f"\u274c <b>Dependency install failed (exit {pip_rc}):</b>\n"
                    f"<pre>{html.escape(pip_msg or 'pip install failed')}</pre>",
                    parse_mode=ParseMode.HTML,
                    reply_markup=kb,
                )
        await _safe_edit(
            status_msg,
            f"\u2705 Updated, restarting\u2026\n<pre>{html.escape(text)}</pre>",
            parse_mode=ParseMode.HTML,
        )
        await _shutdown_cleanly()
        os.execv(sys.executable, [sys.executable] + sys.argv)
    except Exception as e:
        await _safe_edit(
            status_msg,
            f"\u274c Update failed: <code>{html.escape(str(e))[:300]}</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=kb,
        )


@app.on_message(filters.command("update") & filters.user(ADMIN_ID))
async def update_cmd(_, m):
    status = await m.reply_text("Updating\u2026")
    await _perform_git_update(status)


def _install_deps():
    """Install the probe binaries when they are missing (only when running as root on Debian/Ubuntu)."""
    missing = [pkg for binary, pkg in (("ffprobe", "ffmpeg"), ("mediainfo", "mediainfo"))
               if not _which(binary)]
    if not missing:
        return
    if not _which("apt-get") or (hasattr(os, "geteuid") and os.geteuid() != 0):
        logger.error(
            f"Missing required binaries: {', '.join(missing)}. Install them "
            f"manually - captions will otherwise fall back to Telegram metadata."
        )
        return
    logger.info(f"Installing {', '.join(missing)}\u2026")
    try:
        subprocess.run(["apt-get", "update", "-y"],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=300)
        subprocess.run(["apt-get", "install", "-y", "--no-install-recommends", *missing],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=600)
    except Exception as e:
        logger.error(f"Could not install {missing}: {e}")


def _clean_tmp_dir():
    """Remove probe leftovers from a previous (crashed) run."""
    for directory in (TMP_DIR, '.'):
        if not os.path.isdir(directory):
            continue
        for name in os.listdir(directory):
            if name.startswith(('probe_', 'tail_', 'info_')) and name.endswith('.bin'):
                try:
                    os.remove(os.path.join(directory, name))
                except OSError:
                    pass


async def _ensure_indexes():
    """Create the indexes the queries above rely on (idempotent)."""
    try:
        await last_id_collection.create_index("chat_id", unique=True)
        await stats_collection.create_index("chat_id", unique=True)
        await retry_collection.create_index(
            [("chat_id", 1), ("message_id", 1)], unique=True
        )
        await retry_collection.create_index("next_retry")
        _db_ok_again()
    except Exception as e:
        _db_warn("_ensure_indexes", e)


# --- HEALTH CHECK ---
async def health_check(request):
    """Responds to platform pings to keep the instance 'Healthy'."""
    return web.Response(text="Bot is running!", status=200)


async def status_endpoint(request):
    """Small JSON status page, handy for uptime dashboards."""
    return web.json_response({
        "framework": "wzgram",
        "uptime_seconds": int(time.time() - START_TIME),
        "authorized_chats": len(authorized_chats),
        "paused": AUTO_CAPTION_PAUSED,
        "pending": sum(len(q) for q in channel_queues.values()),
        "deep_scans": _runtime["deep"],
        "fallbacks": _runtime["fallback"],
        "flood_waits": _runtime["flood"],
        "probe_failures": _runtime["probe_fail"],
        "flood_active_seconds": dc_stream.flood_left(),
        "media_sessions": dc_stream.active_dcs(),
        "mediainfo": _which("mediainfo"),
        "ffprobe": _which("ffprobe"),
        "database": _db_ok,
        "mongo_session": bool(USE_MONGO_SESSION and MongoStorage is not None),
    })


_web_runner: Optional[web.AppRunner] = None


async def start_health_server():
    """Start the background web server on the platform-assigned port."""
    global _web_runner
    app_web = web.Application()
    app_web.router.add_get("/", health_check)
    app_web.router.add_get("/status", status_endpoint)
    runner = web.AppRunner(app_web)
    await runner.setup()

    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    _web_runner = runner
    logger.info(f"Health check server active on port {port}")


_shutting_down = False


async def _shutdown_cleanly():
    """Close media sessions, the scheduler and the web server."""
    global _shutting_down
    if _shutting_down:
        return
    _shutting_down = True
    try:
        if scheduler.running:
            scheduler.shutdown(wait=False)
    except Exception:
        pass
    await dc_stream.close_sessions()
    if _web_runner is not None:
        try:
            await _web_runner.cleanup()
        except Exception:
            pass
    _clean_tmp_dir()
    logger.info("Shutdown complete")


async def main():
    global stream_semaphore, channel_semaphore

    problems = cfg.validate()
    if problems:
        for problem in problems:
            logger.error(f"Configuration error: {problem}")
        logger.error("Fix the configuration and restart.")
        return

    gc.set_threshold(*GC_THRESHOLD)
    _install_deps()
    _clean_tmp_dir()

    for binary in ("mediainfo", "ffprobe"):
        if not _which(binary):
            logger.warning(
                f"{binary} is unavailable; captions will rely on Telegram metadata."
            )

    await start_health_server()
    await _ensure_indexes()
    await sync_all_configs()

    # Bind semaphores to the active running event loop and apply MongoDB session mode
    stream_semaphore = asyncio.Semaphore(STREAM_CONCURRENCY)
    channel_semaphore = asyncio.Semaphore(CHANNEL_CONCURRENCY)
    app.max_concurrent_transmissions = max(2, STREAM_CONCURRENCY)
    _configure_client_storage(app)

    await app.start()
    me = await app.get_me()
    logger.info(f"@{me.username} started on WZGram")

    stop_event = asyncio.Event()

    def _request_stop(*_args):
        logger.info("Stop signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, _request_stop)
        except (NotImplementedError, RuntimeError, ValueError):
            try:
                signal.signal(sig, lambda *_: _request_stop())
            except (ValueError, OSError):
                pass

    try:
        startup_kb = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("\U0001F39B Control Center", callback_data="adm:home"),
                InlineKeyboardButton("\u2699\ufe0f Settings Hub", callback_data="adm:cfg_tab:probing"),
            ],
            [
                InlineKeyboardButton(f"\U0001F4E1 Channels ({len(authorized_chats)})", callback_data="adm:chats:0"),
                InlineKeyboardButton("\U0001F5A5 Server & Logs", callback_data="adm:server"),
            ],
        ])
        await app.send_message(
            ADMIN_ID,
            "<b>\U0001F680 MediaInfo Bot Online (WZGram)</b>\n\n"
            "<blockquote>"
            f"\u251c <b>Binaries:</b> mediainfo <code>{'ok' if _which('mediainfo') else 'MISSING'}</code> \u2022 "
            f"ffprobe <code>{'ok' if _which('ffprobe') else 'MISSING'}</code>\n"
            f"\u251c <b>Pipeline:</b> " + ("\u23f8 Paused" if AUTO_CAPTION_PAUSED else "\U0001F7E2 Active") +
            f" \u2022 <code>{len(authorized_chats)} channel(s)</code>\n"
            "\u2514 <b>Persistence:</b> <code>MongoDB Synced</code>"
            "</blockquote>\n"
            "Tap below or send <code>/panel</code> anytime.",
            parse_mode=ParseMode.HTML,
            reply_markup=startup_kb,
        )
    except Exception:
        pass

    scheduler.add_job(gc.collect, "interval", minutes=20)
    scheduler.add_job(process_retry_queue, "interval", minutes=2)
    scheduler.add_job(_clean_tmp_dir, "interval", minutes=30)
    scheduler.add_job(_prune_caches, "interval", minutes=10)
    scheduler.start()

    try:
        await stop_event.wait()
    finally:
        await _shutdown_cleanly()
        try:
            await app.stop()
        except Exception:
            pass


if __name__ == "__main__":
    app.run(main())