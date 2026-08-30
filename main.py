"""MediaInfo Bot - extracts media metadata from Telegram files and rewrites captions.

Probing strategy
----------------
Files are never fully downloaded. A small head range is fetched through
``dc_stream`` (a datacenter-aware ranged reader) and grown step by step until
``mediainfo`` / ``ffprobe`` can report a resolution. If the container keeps its
index at the end of the file (fragmented MP4, some MKV muxers) a tail range is
appended as well. Only when every step fails does the bot fall back to
Telegram's own metadata.
"""

import asyncio
import json
import subprocess
import os
import shutil
import html
import logging
import signal
import sys
import psutil
import gc
import re
import time
import uuid
import collections
import datetime
from motor.motor_asyncio import AsyncIOMotorClient
from aiohttp import web
from aiofiles import open as aiopen
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.file_id import FileId
from pyrogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from functools import lru_cache
from typing import Optional
from aiofiles.os import remove as aioremove
from pyrogram.errors import (
    MessageNotModified,
    FloodWait,
    MessageIdInvalid,
    MessageEmpty,
    MediaCaptionTooLong,
    FileReferenceExpired,
    FileReferenceEmpty,
    ChatWriteForbidden,
    ChatAdminRequired,
    ChannelInvalid,
    ChannelPrivate,
    PeerIdInvalid,
    RPCError,
)
from collections import defaultdict
import config as cfg
from config import (
    API_ID, API_HASH, BOT_TOKEN,
    ADMIN_ID, ALLOWED_CHATS,
    LOG_FORMAT, LOG_LEVEL,
    GC_THRESHOLD,
    CAPTION_TEMPLATE,
    MONGO_URI,
    PROBE_TIMEOUT, STREAM_TIMEOUT, STREAM_RETRIES,
    STREAM_CONCURRENCY, CHANNEL_CONCURRENCY,
    ENABLE_TAIL_PROBE, TMP_DIR, SESSION_DIR,
    EDIT_DELAY as EDIT_DELAY_DEFAULT,
    FULL_REPORT,
)
import dc_stream

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT, force=True)
logging.getLogger("pyrogram").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)

# --- IN-MEMORY LOG BUFFER (for /logs command) ---
_log_buffer: collections.deque = collections.deque(maxlen=200)


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
db = db_client["MediaInfo-Bot"]
last_id_collection = db["last_processed_id"]
settings_collection = db["settings"]
stats_collection = db["stats"]
retry_collection = db["retry_queue"]

# Telegram hard limit for media captions.
TG_CAPTION_LIMIT = 1024
START_TIME = time.time()



# --- DYNAMIC CHAT MANAGEMENT ---
authorized_chats: set[int] = set(ALLOWED_CHATS)
_db_ok = True


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


async def sync_chats():
    """Sync the authorized_chats set with MongoDB on startup.

    Env-provided ALLOWED_CHATS are merged in rather than discarded, so a fresh
    database never silently disables every configured channel.
    """
    global authorized_chats
    try:
        data = await settings_collection.find_one({"_id": "allowed_chats"})
        stored = set(data.get("chat_ids") or []) if data else set()
        merged = stored | set(ALLOWED_CHATS)
        authorized_chats = merged
        if merged != stored:
            await settings_collection.update_one(
                {"_id": "allowed_chats"},
                {"$set": {"chat_ids": sorted(merged)}},
                upsert=True,
            )
        _db_ok_again()
    except Exception as e:
        _db_warn("sync_chats", e)
        authorized_chats = set(ALLOWED_CHATS)
    logger.info(f"Authorized chats: {sorted(authorized_chats) or 'none'}")


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
                "$set": {"last_seen": datetime.datetime.now(datetime.timezone.utc)},
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



# --- ERROR COUNTER (for admin DM alerts) ---
_error_counts: dict[int, int] = defaultdict(int)
ERROR_THRESHOLD = 3
# Do not spam the admin: at most one alert per chat every 10 minutes.
_ALERT_COOLDOWN = 600
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
        await app.send_message(
            ADMIN_ID,
            f"\u26a0\ufe0f <b>Error Alert</b>\n"
            f"Chat: <code>{chat_id}</code>\n"
            f"Failed <b>{ERROR_THRESHOLD}x</b> in a row.\n"
            f"Reason: <code>{html.escape(str(reason))[:300]}</code>",
            parse_mode=ParseMode.HTML,
        )
    except Exception:
        pass


def _reset_error_count(chat_id: int):
    _error_counts[chat_id] = 0


def _utcnow() -> datetime.datetime:
    """Timezone-aware UTC now (datetime.utcnow() is deprecated in 3.12)."""
    return datetime.datetime.now(datetime.timezone.utc)


# --- RETRY QUEUE HELPERS ---
MAX_RETRY_ATTEMPTS = 5


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
            # Permanent: the bot cannot edit in this chat at all.
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
except OSError as _e:  # pragma: no cover - depends on the host filesystem
    logger.warning(f"Could not create SESSION_DIR {SESSION_DIR!r}: {_e}")

app = Client(
    "MediaInfo-Bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=6,
    sleep_threshold=60,
    workdir=SESSION_DIR,
    # Allow several concurrent ranged reads; the old default of 1 serialised
    # every probe and made multi-DC files crawl.
    max_concurrent_transmissions=max(2, STREAM_CONCURRENCY),
)

stream_semaphore = asyncio.Semaphore(STREAM_CONCURRENCY)
channel_semaphore = asyncio.Semaphore(CHANNEL_CONCURRENCY)
active_users: set = set()

_last_edit: dict[int, float] = {}
channel_queues: dict[int, list] = defaultdict(list)
channel_locks: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
# Tracks whether a worker task is already draining a channel's queue.
channel_workers: dict[int, bool] = defaultdict(bool)
last_edit_time: dict[int, float] = {}
EDIT_DELAY = EDIT_DELAY_DEFAULT

# Runtime counters exposed through /stats and /server.
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
    'zh': 'Chinese', 'chi': 'Chinese', 'cmn': 'Chinese',
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
    'no': 'Norwegian', 'nor': 'Norwegian',
    'uk': 'Ukrainian', 'ukr': 'Ukrainian',
    'ca': 'Catalan', 'cat': 'Catalan',
    'hr': 'Croatian', 'hrv': 'Croatian',
    'sk': 'Slovak', 'slk': 'Slovak', 'slo': 'Slovak',
    'sr': 'Serbian', 'srp': 'Serbian',
    'bg': 'Bulgarian', 'bul': 'Bulgarian',
    'unknown': 'Original Audio',
}


@lru_cache(maxsize=256)
def get_full_language_name(code: str) -> str:
    """Map an ISO-639 code (or an already spelled out name) to a display name.

    Handles the shapes mediainfo/ffprobe actually emit: ``en``, ``eng``,
    ``en-US``, ``en_US``, ``eng (US)``, ``English`` and comma separated lists.
    Unknown values fall back to ``Original Audio`` so captions stay clean.
    """
    if not code:
        return 'Original Audio'

    cleaned = str(code).split('(')[0].strip()
    cleaned = re.split(r'[;,/]', cleaned)[0].strip()
    if not cleaned:
        return 'Original Audio'

    # en-US / en_US -> en
    base = re.split(r'[-_]', cleaned)[0].strip().lower()

    if base in _LANGUAGE_MAP:
        return _LANGUAGE_MAP[base]
    if cleaned.lower() in _LANGUAGE_MAP:
        return _LANGUAGE_MAP[cleaned.lower()]

    # Already a display name we know about ("English", "hindi", ...).
    for name in set(_LANGUAGE_MAP.values()):
        if cleaned.lower() == name.lower():
            return name

    # Unrecognised but plausible name (e.g. "Assamese") - keep it readable.
    if cleaned.isalpha() and len(cleaned) > 3:
        return cleaned.title()

    return 'Original Audio'


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

    if any(x in codec for x in ('hevc', 'h.265', 'h265')):  parts.append('HEVC')
    elif 'av1' in codec:                                     parts.append('AV1')
    elif any(x in codec for x in ('avc', 'avc1', 'h.264', 'h264')): parts.append('x264')
    elif 'vp9' in codec:                                     parts.append('VP9')
    elif 'vp8' in codec:                                     parts.append('VP8')
    elif any(x in codec for x in ('mpeg4', 'xvid', 'divx')): parts.append('MPEG4')
    elif any(x in codec for x in ('mpeg-2', 'mpeg2')):       parts.append('MPEG2')
    elif any(x in codec for x in ('vc-1', 'vc1')):           parts.append('VC1')
    elif 'prores' in codec:                                  parts.append('ProRes')
    elif 'theora' in codec:                                  parts.append('Theora')
    elif any(x in codec for x in ('wmv', 'vc-9')):           parts.append('WMV')
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


# Substrings that reliably identify a *video* codec.
_VIDEO_CODEC_HINTS = (
    'avc', 'hevc', 'h.264', 'h264', 'h.265', 'h265', 'av1', 'vp8', 'vp9',
    'mpeg-4 visual', 'mpeg4', 'mpeg-2 video', 'xvid', 'divx', 'vc-1',
    'prores', 'theora', 'wmv',
)


def _is_video_track(track: dict) -> bool:
    """Decide whether a mediainfo track describes the video stream.

    ``@type`` is authoritative when present. The loose codec/profile matching
    used previously also matched *audio* tracks (AAC reports
    ``Format_Profile: Main``, DTS reports CodecID ``A_DTS`` ...), which made the
    parser overwrite the real video codec with an audio one and produce captions
    without any video information. Heuristics are therefore only applied when
    the track type is missing or unknown.
    """
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
    # A track that reports pixel dimensions is a picture track.
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

        # "1 h 42 min", "5 min 3 s", "1234 ms"
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
        # mediainfo raw output uses seconds, but some muxers report ms/us.
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




async def _run_mediainfo(path: str, parse_speed: str = "0.5") -> dict:
    """Run mediainfo on a (possibly truncated) file.

    ``--ParseSpeed=0`` was used before; on partial downloads that made mediainfo
    give up before it reached the track headers, which is one reason captions
    ended up empty. A middle parse speed is used by default and the caller can
    ask for a full parse (``1.0``) on the last attempt.
    """
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
        lang_tag = (
            tags.get('language') or tags.get('LANGUAGE')
            or tags.get('lang') or tags.get('title') or ''
        )

        if ctype == 'video':
            # Skip cover art / thumbnails embedded as video streams.
            if (s.get('codec_name') or '').lower() in ('mjpeg', 'png', 'bmp', 'gif'):
                continue
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

            # pix_fmt carries the real bit depth for 10/12-bit content.
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
            audio_langs.append(get_full_language_name(lang_tag or 'unknown'))
            if not info['audio_codec']:
                info['audio_codec'] = (s.get('codec_name') or '').upper()
                ch = s.get('channels')
                if ch:
                    info['audio_channels'] = {1: 'Mono', 2: 'Stereo',
                                              6: '5.1', 8: '7.1'}.get(ch, f"{ch}ch")
            if not info['duration']:
                info['duration'] = _parse_duration(s.get('duration'))

        elif ctype == 'subtitle':
            info['has_sub'] = True
            if lang_tag:
                sub_langs.append(get_full_language_name(lang_tag))

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
    """Read the first positive integer out of the given mediainfo fields.

    Handles the various shapes mediainfo produces: ``1080``, ``1 080``
    (grouping with a normal or non-breaking space), ``1,080`` and
    ``1 080 pixels``.
    """
    for field in fields:
        raw = str(track.get(field, '') or '')
        # Drop any unit suffix ("1 080 pixels" -> "1 080").
        raw = re.split(r'[A-Za-z]', raw, maxsplit=1)[0]
        # Remove digit grouping separators and whitespace.
        raw = re.sub(r'[\s\u00a0\u202f,]', '', raw).split('.')[0]
        if raw.isdigit() and int(raw) > 0:
            return int(raw)
    return 0



def _mi_tracks(mi_data: dict) -> list:
    """Normalise mediainfo's ``media.track`` into a list of dicts.

    With a single track mediainfo emits an object instead of an array, which
    previously made ``_parse_tracks`` iterate over dictionary *keys* and find
    nothing at all.
    """
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
            # Some muxers only expose text tracks through the General block.
            if _parse_int(track.get('TextCount')) > 0:
                info['has_sub'] = True

        elif t == 'audio':
            lang = None
            for field in ('Language', 'Language_String', 'Language_String3', 'Title'):
                v = track.get(field)
                if v:
                    lang = v
                    break
            audio_langs.append(get_full_language_name(lang or 'unknown'))
            if not info['audio_codec']:
                info['audio_codec'] = (track.get('Format', '') or '').upper()
                ch = _parse_int(track.get('Channels'))
                if ch:
                    info['audio_channels'] = {1: 'Mono', 2: 'Stereo',
                                              6: '5.1', 8: '7.1'}.get(ch, f"{ch}ch")
            if not info['duration']:
                info['duration'] = _parse_duration(track.get('Duration'))

        elif t in ('text', 'subtitle'):
            info['has_sub'] = True
            lang = track.get('Language') or track.get('Language_String') or ''
            if lang:
                sub_langs.append(get_full_language_name(lang))

        elif t == 'menu':
            # Chapters, not subtitles. Previously counted as a subtitle track,
            # which made every MKV with chapters report a bogus language.
            continue

        elif _is_video_track(track):
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
                # Dolby Vision is sometimes only visible in the codec profile.
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
    # Prefer the richer language list rather than the first non-empty one.
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


def _info_is_useful(info: dict) -> bool:
    """A probe is only worth keeping when it found real picture dimensions."""
    return bool(info.get('width') or info.get('height'))


async def _probe(path: str, *, deep: bool = False) -> dict:
    """Probe a local (possibly partial) file with mediainfo and ffprobe."""
    mi_data = await _run_mediainfo(path, "1.0" if deep else "0.5")
    mi = _parse_tracks(_mi_tracks(mi_data))

    fp = _blank_info()
    # ffprobe is the slower of the two; skip it when mediainfo already answered.
    if not _info_is_useful(mi) or not mi.get('duration') or not mi.get('audio_langs'):
        fp_data = await _run_ffprobe_full(path)
        if fp_data:
            fp = _parse_ffprobe(fp_data)

    if _info_is_useful(mi) and not _info_is_useful(fp):
        merged = mi
    elif _info_is_useful(fp) and not _info_is_useful(mi):
        merged = fp
    else:
        merged = _merge_info(mi, fp)
    return merged


def _safe_title(message, media) -> str:
    """Return a title safe for str.format (escapes stray braces)."""
    title = message.caption or getattr(media, 'file_name', None) or 'Video'
    return title.replace('{', '{{').replace('}', '}}')


def _trim_caption(caption: str, limit: int = TG_CAPTION_LIMIT) -> str:
    """Shorten a caption to Telegram's limit without breaking HTML tags."""
    if len(caption) <= limit:
        return caption
    cut = caption[:limit]
    # Never end inside a tag or an entity.
    for marker in ('<', '&'):
        idx = cut.rfind(marker)
        if idx > len(cut) - 12 and marker not in cut[idx:]:
            cut = cut[:idx]
    return cut.rstrip()


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
            f"\U0001F3AC <b>{defaults['video_line']}</b> | "
            f"\u23f3 <b>{defaults['duration']}</b>\n"
            f"\U0001F50A <b>{defaults['audio']}</b>\n"
        )
    return _trim_caption(caption)


def _video_line(info: dict) -> str:
    """Build the "1080p HEVC 10bit HDR" style summary line."""
    res_val = info.get('height') or info.get('width') or 0
    quality = get_standard_resolution(res_val)
    fmt = get_video_format(
        info.get('codec', ''),
        info.get('transfer', ''),
        info.get('hdr', ''),
        info.get('bit_depth', ''),
    )
    return ' '.join(filter(None, [quality, fmt])) or 'Unknown'


def _build_caption(message, media, info: dict) -> str:
    """Caption built from real probe results."""
    size = getattr(media, 'file_size', None)
    width, height = info.get('width'), info.get('height')
    return _render_caption(
        title=_safe_title(message, media),
        video_line=_video_line(info),
        duration=_fmt_duration(info['duration']) if info.get('duration') else 'Unknown',
        audio=info.get('audio') or 'Original Audio',
        subtitle=info.get('subtitle') or 'No Esubs',
        size=_fmt_size(size),
        bitrate=_fmt_bitrate(info.get('bitrate')),
        fps=_fmt_fps(info.get('fps')),
        resolution=f"{width}x{height}" if width and height else '',
        codec=info.get('codec_name') or info.get('codec') or '',
        container=info.get('container') or '',
        filename=(getattr(media, 'file_name', '') or '').replace('{', '{{').replace('}', '}}'),
    )



_RES_TOKEN_RE = re.compile(
    r'\b(240|360|480|540|576|720|1080|1440|2160|4320)\s*[pi]\b', re.IGNORECASE
)
_DIM_TOKEN_RE = re.compile(r'\b(\d{3,4})\s*[xX\u00d7]\s*(\d{3,4})\b')


def _extract_res_from_text(text: str) -> Optional[str]:
    """Parse a resolution out of a file name or caption."""
    if not text:
        return None
    match = _RES_TOKEN_RE.search(text)
    if match:
        return f"{match.group(1)}p"
    match = _DIM_TOKEN_RE.search(text)
    if match:
        return get_standard_resolution(int(match.group(2)))
    low = text.lower()
    if '8k' in low:
        return "4320p"
    if '4k' in low or 'uhd' in low or '2160' in low:
        return "2160p"
    if 'fhd' in low or 'full hd' in low:
        return "1080p"
    if 'hd rip' in low or 'hdrip' in low or '.hd.' in low:
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
    low = (text or '').lower()
    extras: list[str] = []
    if '10bit' in low or '10 bit' in low or 'hi10' in low:
        extras.append('10bit')
    elif '12bit' in low or '12 bit' in low:
        extras.append('12bit')
    if 'dolby vision' in low or re.search(r'\bdv\b', low):
        extras.append('DV')
    elif 'hdr10+' in low or 'hdr10plus' in low:
        extras.append('HDR10+')
    elif 'hdr' in low:
        extras.append('HDR')
    return extras


def _langs_from_text(text: str) -> list[str]:
    """Detect audio languages named in a release title (e.g. "Hindi + English")."""
    low = (text or '').lower()
    found: list[str] = []
    for name in sorted(set(_LANGUAGE_MAP.values())):
        if name == 'Original Audio':
            continue
        if re.search(rf'\b{re.escape(name.lower())}\b', low):
            found.append(name)
    if re.search(r'\bdual[\s._-]?audio\b', low) and len(found) < 2:
        found.append('Dual Audio')
    elif re.search(r'\bmulti[\s._-]?audio\b', low) and not found:
        found.append('Multi Audio')
    return _dedupe(found)


def _build_caption_from_tg(message, media) -> str:
    """Fallback caption built from Telegram metadata plus filename parsing."""
    raw_title = message.caption or getattr(media, 'file_name', None) or 'Video'
    file_name = getattr(media, 'file_name', '') or ''
    text_pool = f"{raw_title} {file_name}"

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
    has_sub = bool(re.search(r'\b(esub|esubs|msub|msubs|subbed|sub)\b',
                             text_pool, re.IGNORECASE))

    return _render_caption(
        title=raw_title.replace('{', '{{').replace('}', '}}'),
        video_line=video_line,
        duration=_fmt_duration(tg_dur) if tg_dur else 'Unknown',
        audio=', '.join(langs) if langs else 'Original Audio',
        subtitle='ESUB' if has_sub else 'No Esubs',
        size=_fmt_size(getattr(media, 'file_size', None)),
        bitrate='Unknown',
        fps='',
        resolution=(
            f"{getattr(media, 'width', 0)}x{getattr(media, 'height', 0)}"
            if getattr(media, 'width', None) and getattr(media, 'height', None) else ''
        ),
        codec=codec,
        container=(os.path.splitext(file_name)[1].lstrip('.').upper() or ''),
        filename=file_name.replace('{', '{{').replace('}', '}}'),
    )


def caption_has_media_info(caption: str) -> bool:
    """Detect a caption this bot (or a similar one) has already written."""
    if not caption:
        return False
    hits = (
        '\U0001F3AC' in caption,                                    # clapper
        bool(re.search(r'\u23f3\s*<?b?>?\s*\d{1,2}:\d{2}', caption)),  # hourglass + time
        '\U0001F50A' in caption,                                    # speaker
        '\U0001F4AC' in caption,                                    # speech balloon
        bool(re.search(r'\b\d{3,4}p\b.*\b(HEVC|x264|AV1|VP9|MPEG4)\b', caption)),
    )
    return sum(hits) >= 2



_STREAM_STEPS = [
    ("32 KB", 32 * 1024),
    ("1 MB", 1 * 1024 * 1024),
    ("4 MB", 4 * 1024 * 1024),
    ("10 MB", 10 * 1024 * 1024),
]
# How much of the file end is fetched when the header alone is not enough.
TAIL_SIZE = 1 * 1024 * 1024


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
    """Re-fetch a message to obtain a fresh file_reference.

    File references expire (typically after a few hours) and any further
    ``upload.GetFile`` call then fails with FILE_REFERENCE_EXPIRED. Re-reading
    the message hands us a new reference.
    """
    try:
        fresh = await app.get_messages(message.chat.id, message.id)
        if fresh:
            return fresh.video or fresh.document or fresh.audio or fresh.animation
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
    """Header-only download through pyrogram (used for CDN-hosted files)."""
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
    """Fetch a byte range with retries, flood handling and reference refresh.

    Returns ``(bytes_written, media)`` where ``media`` is the possibly refreshed
    media object so the caller keeps using a valid file reference.
    """
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
            # pyrogram's downloader understands CDN redirects, but it can only
            # start from the beginning of the file.
            if offset:
                logger.warning("CDN-hosted file: cannot read a tail range, skipping")
                _rollback()
                return 0, media
            logger.info("File is CDN hosted, using pyrogram's downloader")
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


async def probe_media(message, media, progress=None) -> tuple[dict, bool]:
    """Probe a Telegram media object without downloading the whole file.

    Returns ``(info, deep)`` where ``deep`` is True when real metadata was read
    from the file and False when the caller should fall back to Telegram data.
    """
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
                await progress(f"\U0001F4E6 Reading {label} from {home_hint}\u2026")

            written, media = await _fetch_range(
                message, media, total, target - total, tmp,
                'wb' if total == 0 else 'ab',
            )
            if not written:
                # Flood wait or a hard failure: stop wasting requests.
                if dc_stream.flood_left(_media_dc(media)):
                    break
                if total == 0:
                    continue
                break

            total += written
            info = await _probe(tmp, deep=index >= len(_STREAM_STEPS) - 1)
            if _info_is_useful(info):
                best = info
                # Dimensions plus a duration is everything the caption needs.
                if info.get('duration'):
                    logger.info(
                        f"probe ok via {info.get('source')} after {_fmt_size(total)} "
                        f"({home_hint}, msg {message.id})"
                    )
                    return info, True
            if file_size and total >= file_size:
                break
            if total % dc_stream.ALIGN:
                # Telegram returned a short (unaligned) read, meaning we hit the
                # end of the file; appending further ranges would duplicate data
                # because offsets must stay 4 KiB aligned.
                break

        # Header data alone was not enough: some containers (fragmented MP4,
        # MKV written by streaming muxers) keep the index at the end of the file.
        if ENABLE_TAIL_PROBE and file_size > TAIL_SIZE * 2 and not (
            _info_is_useful(best) and best.get('duration')
        ):
            if not dc_stream.flood_left(_media_dc(media)):
                if progress:
                    await progress("\U0001F50E Checking file tail\u2026")
                tail = _tmp_path(f"tail_{message.id}_{uuid.uuid4().hex[:8]}.bin")
                try:
                    written, media = await _fetch_range(
                        message, media, max(0, file_size - TAIL_SIZE),
                        TAIL_SIZE, tail, 'wb',
                    )
                    if written:
                        tail_info = await _probe(tail, deep=True)
                        if _info_is_useful(tail_info):
                            best = (
                                _merge_info(best, tail_info)
                                if _info_is_useful(best) else tail_info
                            )
                        elif tail_info.get('duration') and _info_is_useful(best):
                            best['duration'] = best['duration'] or tail_info['duration']
                finally:
                    await _cleanup(tail)

        if _info_is_useful(best):
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
            await _safe_edit(progress_msg, text)

    await _update("\u26a1 Starting scan\u2026")

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


async def _safe_edit(msg, text: str, parse_mode=None):
    """Edit a status message, rate limited to avoid FloodWait storms."""
    if not msg:
        return
    key = msg.id
    now = asyncio.get_event_loop().time()
    if key in _last_edit and now - _last_edit[key] < 1.7:
        return
    try:
        await msg.edit_text(text, parse_mode=parse_mode)
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

    # Only drop a channel's state while no worker is running for it, otherwise
    # the defaultdict would hand a *new* lock to the next message and two
    # workers could edit the same channel at once.
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



async def _edit_caption(message, caption: str) -> bool:
    """Apply a caption, trimming it if Telegram rejects the length."""
    try:
        await message.edit_caption(caption, parse_mode=ParseMode.HTML)
        return True
    except MediaCaptionTooLong:
        await message.edit_caption(_trim_caption(caption), parse_mode=ParseMode.HTML)
        return True


async def _process_channel_queue(channel_id: int):
    global EDIT_DELAY
    async with channel_semaphore:
        async with channel_locks[channel_id]:
            while channel_queues[channel_id]:
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
                    # Back off permanently a bit; Telegram is telling us the
                    # channel is being edited too fast.
                    EDIT_DELAY = min(60.0, max(EDIT_DELAY, e.value / 10 + 1))
                    logger.warning(
                        f"Edit FloodWait {e.value}s on {channel_id}; "
                        f"delay is now {EDIT_DELAY:.1f}s"
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
                    # No point queueing retries we can never complete.
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


# Every media type the bot can describe.
_MEDIA_FILTER = (
    filters.video | filters.document | filters.animation | filters.audio
)


@app.on_message(filters.channel & _MEDIA_FILTER)
async def channel_handler(_, message):
    if message.chat.id not in authorized_chats:
        return
    if caption_has_media_info(message.caption or ''):
        return
    if _media_of(message) is None:
        return

    channel_id = message.chat.id
    channel_queues[channel_id].append(message)

    # Only spawn a worker if one isn't already draining this channel's queue.
    if not channel_workers[channel_id]:
        channel_workers[channel_id] = True
        asyncio.create_task(_channel_worker(channel_id))



async def _channel_worker(channel_id: int):
    try:
        await _process_channel_queue(channel_id)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        # Without this the exception would only surface as an unretrieved task
        # error and the channel would silently stop being processed.
        logger.exception(f"Channel worker for {channel_id} crashed: {e}")
        await _report_error(channel_id, str(e))
    finally:
        channel_workers[channel_id] = False
        # Re-arm if new messages arrived after the queue drained.
        if channel_queues[channel_id]:
            channel_workers[channel_id] = True
            asyncio.create_task(_channel_worker(channel_id))


@app.on_message(filters.private & _MEDIA_FILTER)
async def private_handler(_, message):
    if not message.from_user:
        return
    user_id = message.from_user.id
    if user_id in active_users:
        await message.reply_text("\u26a0\ufe0f Please wait until your current file is processed.")
        return
    active_users.add(user_id)
    asyncio.create_task(_handle_private(message))


async def _handle_private(message):
    progress_msg = None
    user_id = message.from_user.id
    try:
        progress_msg = await message.reply_text("\u23f3 Processing\u2026")
        media = _media_of(message)
        if media is None:
            await _safe_edit(progress_msg, "\u26a0\ufe0f Unsupported media type.")
            return

        info, deep = await probe_media(
            message, media, lambda t: _safe_edit(progress_msg, t)
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

        markup = _report_markup(media, info) if deep else None
        try:
            await message.reply_text(
                caption, parse_mode=ParseMode.HTML, reply_markup=markup
            )
            await _delete_quietly(progress_msg)
        except Exception:
            await _safe_edit(progress_msg, caption, parse_mode=ParseMode.HTML)
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
# token -> report text. Small bounded cache so callback buttons stay cheap.
_report_cache: "collections.OrderedDict[str, str]" = collections.OrderedDict()
_REPORT_CACHE_MAX = 128


def _cache_report(text: str) -> str:
    token = uuid.uuid4().hex[:12]
    _report_cache[token] = text
    while len(_report_cache) > _REPORT_CACHE_MAX:
        _report_cache.popitem(last=False)
    return token


def _full_report(media, info: dict) -> str:
    """Human readable, detailed report used by the MediaInfo button."""
    width, height = info.get('width'), info.get('height')
    dc_id = _media_dc(media)
    rows = [
        ("File", getattr(media, 'file_name', None) or '-'),
        ("Size", _fmt_size(getattr(media, 'file_size', None))),
        ("Container", info.get('container') or '-'),
        ("Duration", _fmt_duration(info['duration']) if info.get('duration') else '-'),
        ("Bitrate", _fmt_bitrate(info.get('bitrate'))),
        ("Resolution", f"{width}x{height}" if width and height else '-'),
        ("Quality", get_standard_resolution(height or width or 0) or '-'),
        ("Video codec", info.get('codec_name') or info.get('codec') or '-'),
        ("Bit depth", f"{info['bit_depth']} bit" if info.get('bit_depth') else '-'),
        ("HDR", info.get('hdr') or '-'),
        ("Frame rate", _fmt_fps(info.get('fps')) or '-'),
        ("Audio codec", info.get('audio_codec') or '-'),
        ("Channels", info.get('audio_channels') or '-'),
        ("Audio langs", ', '.join(info.get('audio_langs') or []) or '-'),
        ("Subtitles", info.get('subtitle') or 'None'),
        ("Probed with", info.get('source') or '-'),
        ("Datacenter", f"DC{dc_id}" if dc_id else '-'),
    ]
    body = '\n'.join(f"{label:<13}: {html.escape(str(value))}" for label, value in rows)
    return f"<b>\U0001F4CB MediaInfo</b>\n<pre>{body}</pre>"


def _report_markup(media, info: dict) -> Optional[InlineKeyboardMarkup]:
    if not FULL_REPORT:
        return None
    try:
        token = _cache_report(_full_report(media, info))
    except Exception as e:
        logger.warning(f"could not build report: {e}")
        return None
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("\U0001F4CB Full MediaInfo", callback_data=f"rep:{token}")]]
    )


@app.on_callback_query(filters.regex(r"^rep:"))
async def report_callback(_, query):
    token = query.data.split(":", 1)[1]
    report = _report_cache.get(token)
    if not report:
        return await query.answer("Report expired, send the file again.", show_alert=True)
    try:
        await query.message.reply_text(report, parse_mode=ParseMode.HTML)
        await query.answer()
    except Exception as e:
        await query.answer(f"Failed: {e}"[:180], show_alert=True)


async def _info_reply(message, target, media):
    """Shared implementation of the /info command."""
    status = await message.reply_text("\u23f3 Scanning\u2026")
    try:
        info, deep = await probe_media(target, media, lambda t: _safe_edit(status, t))
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



# --- ADMIN COMMANDS ---

async def _resolve_chat_arg(message) -> Optional[int]:
    """Resolve the chat argument of /add and /remove.

    Accepts a numeric id, a @username, or nothing at all (meaning "this chat").
    """
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
    if chat_id in authorized_chats:
        return await message.reply_text(f"\u2139\ufe0f <code>{chat_id}</code> is already authorized.",
                                        parse_mode=ParseMode.HTML)
    authorized_chats.add(chat_id)
    try:
        await settings_collection.update_one(
            {"_id": "allowed_chats"},
            {"$addToSet": {"chat_ids": chat_id}},
            upsert=True,
        )
    except Exception as e:
        _db_warn("add_chat", e)
        return await message.reply_text(
            f"\u26a0\ufe0f Added <code>{chat_id}</code> in memory, but the database "
            f"write failed: <code>{html.escape(str(e))[:150]}</code>",
            parse_mode=ParseMode.HTML,
        )
    await message.reply_text(f"\u2705 Added <code>{chat_id}</code> to authorized chats.",
                             parse_mode=ParseMode.HTML)


@app.on_message(filters.command("remove") & filters.user(ADMIN_ID))
async def remove_chat(_, message):
    chat_id = await _resolve_chat_arg(message)
    if chat_id is None:
        return await message.reply_text("\u274c Could not resolve that chat.")
    if chat_id not in authorized_chats:
        return await message.reply_text("\u274c Chat ID not found in the list.")
    authorized_chats.discard(chat_id)
    try:
        await settings_collection.update_one(
            {"_id": "allowed_chats"},
            {"$pull": {"chat_ids": chat_id}},
        )
    except Exception as e:
        _db_warn("remove_chat", e)
    await message.reply_text(f"\u2705 Removed <code>{chat_id}</code> from authorized chats.",
                             parse_mode=ParseMode.HTML)


@app.on_message(filters.command("chats") & filters.user(ADMIN_ID))
async def list_chats(_, message):
    if not authorized_chats:
        return await message.reply_text("\U0001F4ED No authorized chats.")
    lines = []
    for cid in sorted(authorized_chats):
        try:
            chat = await app.get_chat(cid)
            title = chat.title or chat.first_name or ''
        except Exception:
            title = "unreachable"
        lines.append(f"\u2022 <code>{cid}</code> \u2014 {html.escape(str(title))[:40]}")
    await message.reply_text(
        f"<b>Allowed Chats ({len(lines)}):</b>\n" + "\n".join(lines),
        parse_mode=ParseMode.HTML,
    )


@app.on_message(filters.command("stats") & filters.user(ADMIN_ID))
async def stats_cmd(_, message):
    all_stats = await get_all_stats()
    total = sum(s.get("count", 0) for s in all_stats)
    lines = [
        "\U0001F4CA <b>Statistics</b>",
        f"Total processed: <b>{total}</b>",
        f"This session: deep <b>{_runtime['deep']}</b> | "
        f"fallback <b>{_runtime['fallback']}</b>",
        f"Flood waits: <b>{_runtime['flood']}</b> | "
        f"probe failures: <b>{_runtime['probe_fail']}</b>",
    ]
    dcs = dc_stream.active_dcs()
    if dcs:
        lines.append("Media sessions: " + ", ".join(f"DC{d}" for d in dcs))
    if all_stats:
        lines.append("")
        for s in all_stats[:20]:
            deep = s.get("deep", 0)
            fb = s.get("fallback", 0)
            lines.append(
                f"\u2022 <code>{s['chat_id']}</code> \u2192 <b>{s.get('count', 0)}</b>"
                + (f" (deep {deep}, fallback {fb})" if deep or fb else "")
            )
    await message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)



@app.on_message(filters.command("broadcast") & filters.user(ADMIN_ID))
async def broadcast_cmd(_, message):
    if len(message.command) < 2:
        return await message.reply_text("\u274c Usage: `/broadcast Your message here`")
    text = message.text.split(None, 1)[1]
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
    await message.reply_text(f"\u2705 Broadcast done.\nSent: <b>{sent}</b> | Failed: <b>{failed}</b>", parse_mode=ParseMode.HTML)


@app.on_message(filters.command("queue") & filters.user(ADMIN_ID))
async def queue_cmd(_, message):
    lines = ["\U0001F4CB <b>Pending Queue</b>\n"]
    total = 0
    for cid, q in list(channel_queues.items()):
        if q:
            worker = "running" if channel_workers.get(cid) else "idle"
            lines.append(
                f"\u2022 <code>{cid}</code> \u2192 <b>{len(q)}</b> pending ({worker})"
            )
            total += len(q)

    try:
        retry_count = await retry_collection.count_documents({})
    except Exception:
        retry_count = -1

    if total == 0 and retry_count <= 0:
        return await message.reply_text("\u2705 All queues are empty.")
    if total:
        lines.append(f"\n<b>Total pending: {total}</b>")
    if retry_count > 0:
        lines.append(f"<b>Retry queue: {retry_count}</b>")
    flood = dc_stream.flood_left()
    if flood:
        lines.append(f"\u23f3 Flood wait active for <b>{flood}s</b>")
    await message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


@app.on_message(filters.command("clearqueue") & filters.user(ADMIN_ID))
async def clearqueue_cmd(_, message):
    """Drop everything waiting to be processed (in-memory queues only)."""
    dropped = sum(len(q) for q in channel_queues.values())
    for q in channel_queues.values():
        q.clear()
    await message.reply_text(f"\U0001F5D1 Cleared <b>{dropped}</b> pending item(s).",
                             parse_mode=ParseMode.HTML)


@app.on_message(filters.command("retrynow") & filters.user(ADMIN_ID))
async def retrynow_cmd(_, message):
    """Force the persistent retry queue to run immediately."""
    try:
        await retry_collection.update_many({}, {"$set": {"next_retry": _utcnow()}})
    except Exception as e:
        return await message.reply_text(f"\u274c Database error: {e}")
    await message.reply_text("\u23f3 Running the retry queue\u2026")
    await process_retry_queue()
    try:
        left = await retry_collection.count_documents({})
    except Exception:
        left = -1
    await message.reply_text(
        f"\u2705 Retry pass finished. Remaining: <b>{left if left >= 0 else '?'}</b>",
        parse_mode=ParseMode.HTML,
    )


@app.on_message(filters.command("setdelay") & filters.user(ADMIN_ID))
async def setdelay_cmd(_, message):
    global EDIT_DELAY
    if len(message.command) < 2:
        return await message.reply_text(
            f"\u23f1 Current delay: <b>{EDIT_DELAY}s</b>\nUsage: <code>/setdelay 5.0</code>",
            parse_mode=ParseMode.HTML,
        )
    try:
        val = float(message.command[1])
    except ValueError:
        return await message.reply_text(
            "\u274c Invalid value. Use a number like <code>3.5</code>",
            parse_mode=ParseMode.HTML,
        )
    if not 1.0 <= val <= 60.0:
        return await message.reply_text("\u274c Delay must be between 1.0 and 60.0 seconds.")
    EDIT_DELAY = val
    await message.reply_text(f"\u2705 Edit delay set to <b>{EDIT_DELAY}s</b>",
                             parse_mode=ParseMode.HTML)


@app.on_message(filters.command("logs") & filters.user(ADMIN_ID))
async def logs_cmd(_, message):
    try:
        n = int(message.command[1]) if len(message.command) > 1 else 20
        n = max(1, min(n, 200))
    except ValueError:
        n = 20
    lines = list(_log_buffer)[-n:]
    if not lines:
        return await message.reply_text("\U0001F4ED No logs yet.")
    log_text = html.escape("\n".join(lines))
    if len(log_text) > 3800:
        log_text = "\u2026" + log_text[-3800:]
    await message.reply_text(f"<pre>{log_text}</pre>", parse_mode=ParseMode.HTML)



@app.on_message(filters.command("scan") & filters.user(ADMIN_ID))
async def scan_cmd(_, message):
    """Backfill captions for older messages in a channel.

    Usage: ``/scan [chat_id|@username] [limit]``. Walks the history
    newest-first and queues anything that still lacks a media info caption.
    """
    args = message.command[1:]
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
    queued = skipped = 0
    try:
        async for msg in app.get_chat_history(chat_id, limit=limit):
            if _media_of(msg) is None:
                continue
            if caption_has_media_info(msg.caption or ''):
                skipped += 1
                continue
            channel_queues[chat_id].append(msg)
            queued += 1
    except Exception as e:
        return await _safe_edit(status, f"\u274c Scan failed: {html.escape(str(e))[:200]}")

    if queued and not channel_workers[chat_id]:
        channel_workers[chat_id] = True
        asyncio.create_task(_channel_worker(chat_id))

    await _safe_edit(
        status,
        f"\u2705 Queued <b>{queued}</b> message(s), skipped <b>{skipped}</b> "
        f"already captioned.",
        parse_mode=ParseMode.HTML,
    )


@app.on_message(filters.command("template") & filters.user(ADMIN_ID))
async def template_cmd(_, message):
    """Show the active caption template and its placeholders."""
    placeholders = (
        "{title} {video_line} {duration} {audio} {subtitle} "
        "{size} {bitrate} {fps} {resolution} {codec} {container} {filename}"
    )
    await message.reply_text(
        "<b>Active caption template</b>\n"
        f"<pre>{html.escape(CAPTION_TEMPLATE)}</pre>\n"
        f"<b>Placeholders</b>\n<code>{html.escape(placeholders)}</code>\n\n"
        "Change it with the <code>CAPTION_TEMPLATE</code> environment variable.",
        parse_mode=ParseMode.HTML,
    )


@app.on_message(filters.command("ping") & filters.user(ADMIN_ID))
async def ping_cmd(_, message):
    started = time.monotonic()
    msg = await message.reply_text("\U0001F3D3 Pong\u2026")
    latency = (time.monotonic() - started) * 1000
    try:
        db_start = time.monotonic()
        await db.command("ping")
        db_ms = f"{(time.monotonic() - db_start) * 1000:.0f} ms"
    except Exception:
        db_ms = "unreachable"
    await _safe_edit(
        msg,
        f"\U0001F3D3 <b>Pong</b>\nTelegram: <b>{latency:.0f} ms</b>\n"
        f"MongoDB: <b>{db_ms}</b>",
        parse_mode=ParseMode.HTML,
    )


@app.on_message(filters.command("start") & filters.private)
async def start(_, m):
    await m.reply_text(
        "<b>\U0001F3AC Media Info Bot</b>\n\n"
        "Send me any video, audio or document and I'll extract detailed media "
        "information.\n\n"
        "I provide:\n"
        "\u2022 \U0001F39E Video quality, codec &amp; bit depth\n"
        "\u2022 \u23f3 Duration\n"
        "\u2022 \U0001F50A Audio languages\n"
        "\u2022 \U0001F4AC Subtitle info\n\n"
        "<b>Commands</b>\n"
        "\u2022 <code>/info</code> \u2014 reply to a file, or send it with the command\n"
        "\u2022 <code>/help</code> \u2014 how it works\n\n"
        "<b>\u26a1 Fast \u2022 Clean \u2022 Accurate</b>\n\n"
        "\U0001F4CC <i>Note:</i> Send one file at a time.\n\n"
        "\U0001F916 Bot by @piroxbots",
        parse_mode=ParseMode.HTML,
    )


@app.on_message(filters.command("help") & filters.private)
async def help_cmd(_, m):
    text = (
        "<b>\U0001F4D8 How it works</b>\n\n"
        "Only the first few megabytes of a file are streamed, so nothing is "
        "fully downloaded. <code>mediainfo</code> and <code>ffprobe</code> read "
        "the container headers; when they are stored at the end of the file the "
        "tail is fetched too.\n\n"
        "<b>Commands</b>\n"
        "\u2022 <code>/info</code> \u2014 media info for a replied or attached file\n"
        "\u2022 <code>/start</code> \u2014 about this bot\n"
    )
    if m.from_user and m.from_user.id == ADMIN_ID:
        text += (
            "\n<b>Admin</b>\n"
            "\u2022 <code>/add</code>, <code>/remove</code>, <code>/chats</code>\n"
            "\u2022 <code>/scan [chat] [limit]</code> \u2014 backfill old messages\n"
            "\u2022 <code>/stats</code>, <code>/queue</code>, <code>/clearqueue</code>, "
            "<code>/retrynow</code>\n"
            "\u2022 <code>/setdelay</code>, <code>/template</code>, <code>/logs</code>, "
            "<code>/ping</code>\n"
            "\u2022 <code>/server</code>, <code>/restart</code>, <code>/shutdown</code>, "
            "<code>/update</code>\n"
        )
    await m.reply_text(text, parse_mode=ParseMode.HTML)


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


@app.on_message(filters.command("server") & filters.user(ADMIN_ID))
async def server_cmd(_, m):
    proc = psutil.Process()
    mem = proc.memory_info().rss
    disk_path = os.path.abspath(TMP_DIR) if os.path.isdir(TMP_DIR) else os.path.abspath('.')
    try:
        disk = psutil.disk_usage(disk_path).percent
    except Exception:
        disk = -1
    dcs = dc_stream.active_dcs()
    await m.reply_text(
        f"<b>\U0001F5A5 Server</b>\n"
        f"CPU: <b>{psutil.cpu_percent()}%</b>\n"
        f"RAM: <b>{psutil.virtual_memory().percent}%</b> "
        f"(bot {_fmt_size(mem)})\n"
        f"Disk: <b>{disk if disk >= 0 else '?'}%</b>\n"
        f"Uptime: <b>{_fmt_uptime(time.time() - START_TIME)}</b>\n"
        f"Edit delay: <b>{EDIT_DELAY:.1f}s</b>\n"
        f"Media sessions: <b>{', '.join(f'DC{d}' for d in dcs) or 'none'}</b>\n"
        f"mediainfo: <b>{'yes' if _which('mediainfo') else 'MISSING'}</b> | "
        f"ffprobe: <b>{'yes' if _which('ffprobe') else 'MISSING'}</b>",
        parse_mode=ParseMode.HTML,
    )



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


@app.on_message(filters.command("update") & filters.user(ADMIN_ID))
async def update_cmd(_, m):
    status = await m.reply_text("Updating\u2026")
    try:
        out = await _run_cmd(["git", "pull"], 120) or b''
        text = out.decode(errors='replace').strip()[-500:]
        if "Already up to date" in text:
            return await _safe_edit(status, "\u2705 Already up to date.")
        await _run_cmd(
            [sys.executable, "-m", "pip", "install", "-r", "requirements.txt",
             "--no-cache-dir", "-q"],
            300,
        )
        await _safe_edit(
            status,
            f"\u2705 Updated, restarting\u2026\n<pre>{html.escape(text)}</pre>",
            parse_mode=ParseMode.HTML,
        )
        await _shutdown_cleanly()
        os.execv(sys.executable, [sys.executable] + sys.argv)
    except Exception as e:
        await _safe_edit(status, f"Update failed: {html.escape(str(e))[:300]}")


def _install_deps():
    """Install the probe binaries when they are missing (Debian/Ubuntu images)."""
    missing = [pkg for binary, pkg in (("ffprobe", "ffmpeg"), ("mediainfo", "mediainfo"))
               if not _which(binary)]
    if not missing:
        return
    if not _which("apt-get"):
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
        "uptime_seconds": int(time.time() - START_TIME),
        "authorized_chats": len(authorized_chats),
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
    await sync_chats()

    await app.start()
    me = await app.get_me()
    logger.info(f"@{me.username} started")

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
            # Windows and some restricted environments.
            try:
                signal.signal(sig, lambda *_: _request_stop())
            except (ValueError, OSError):
                pass

    try:
        await app.send_message(
            ADMIN_ID,
            "\U0001F680 <b>Bot started</b>\n"
            f"mediainfo: {'ok' if _which('mediainfo') else 'MISSING'} | "
            f"ffprobe: {'ok' if _which('ffprobe') else 'MISSING'}\n"
            f"Chats: {len(authorized_chats)}\n"
            "New: /scan /retrynow /clearqueue /template /ping /help",
            parse_mode=ParseMode.HTML,
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

