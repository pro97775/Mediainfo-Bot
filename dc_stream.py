"""Datacenter-aware partial media downloader for WZGram / Pyrogram.

Why this module exists
----------------------
``Client.stream_media`` opens a *brand new* MTProto session for every call and,
when the file lives on a datacenter other than the one the bot is logged into,
performs a full ``auth.ExportAuthorization`` + ``auth.ImportAuthorization``
handshake each time.

The bot probes a file in several growing steps (32 KB, 1 MB, 4 MB ...), so a
single file used to trigger one session + one authorisation handshake per step.
On foreign DCs Telegram answers that burst with ``FloodWait`` or
``AUTH_BYTES_INVALID``.

What this module does
---------------------
* keeps **one long-lived media session per datacenter** and reuses it,
* uses **per-DC locks** so a slow foreign DC handshake never blocks cached DCs,
* downloads **4 KiB-aligned power-of-2 byte ranges** (strictly satisfying
  Telegram's ``1048576 % limit == 0`` and ``offset % limit == 0`` rules so
  multi-step probes never fail with ``400 LIMIT_INVALID``),
* slices any unaligned leading/trailing bytes so ``iter_range`` yields the exact
  requested ``[offset, offset + length)`` slice,
* records ``FloodWait`` **per datacenter** instead of globally,
* recovers from ``AuthBytesInvalid`` by rebuilding the session,
* follows and persists ``FileMigrate`` across chunks,
* reports CDN redirects so the caller can fall back to ``client.stream_media``.
"""

import asyncio
import logging
import time
from collections import defaultdict
from typing import AsyncGenerator, Optional

try:
    from wzgram import raw, utils
    from wzgram.errors import (
        AuthBytesInvalid,
        FileMigrate,
        FloodWait,
        InternalServerError,
        ServiceUnavailable,
    )
    from wzgram.file_id import FileId, FileType, ThumbnailSource
    from wzgram.session import Auth, Session
except ImportError:  # pragma: no cover - fallback if imported under plain pyrogram
    from pyrogram import raw, utils
    from pyrogram.errors import (
        AuthBytesInvalid,
        FileMigrate,
        FloodWait,
        InternalServerError,
        ServiceUnavailable,
    )
    from pyrogram.file_id import FileId, FileType, ThumbnailSource
    from pyrogram.session import Auth, Session

logger = logging.getLogger(__name__)

# Telegram requires upload.GetFile offsets/limits to be 4 KiB aligned,
# limit to be a power-of-2 divisor of 1 MiB (4096 .. 1048576), and a single
# request may never cross a 1 MiB boundary.
ALIGN = 4 * 1024
CHUNK = 1024 * 1024

_sessions: dict[int, Session] = {}
_dc_locks: dict[int, asyncio.Lock] = defaultdict(asyncio.Lock)
_dc_flood_until: dict[int, float] = {}


class CdnRedirect(Exception):
    """Raised when Telegram serves the file from a CDN we do not handle."""


def flood_left(dc_id: Optional[int] = None) -> int:
    """Seconds left of an active flood wait for a DC (0 when clear)."""
    now = time.monotonic()
    if dc_id is None:
        if not _dc_flood_until:
            return 0
        return max(0, int(max(_dc_flood_until.values()) - now))
    return max(0, int(_dc_flood_until.get(dc_id, 0) - now))


def note_flood(dc_id: int, seconds: float) -> None:
    """Remember that a DC asked us to back off."""
    _dc_flood_until[dc_id] = time.monotonic() + max(0.0, float(seconds))


def clear_flood(dc_id: Optional[int] = None) -> None:
    """Clear flood-wait tracking for one DC or all DCs."""
    if dc_id is None:
        _dc_flood_until.clear()
    else:
        _dc_flood_until.pop(dc_id, None)


def _location(file_id: FileId):
    """Build the InputFileLocation for a decoded file id."""
    if file_id.file_type == FileType.CHAT_PHOTO:
        if file_id.chat_id > 0:
            peer = raw.types.InputPeerUser(
                user_id=file_id.chat_id,
                access_hash=file_id.chat_access_hash,
            )
        elif file_id.chat_access_hash == 0:
            peer = raw.types.InputPeerChat(chat_id=-file_id.chat_id)
        else:
            peer = raw.types.InputPeerChannel(
                channel_id=utils.get_channel_id(file_id.chat_id),
                access_hash=file_id.chat_access_hash,
            )
        return raw.types.InputPeerPhotoFileLocation(
            peer=peer,
            photo_id=file_id.media_id,
            big=file_id.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG,
        )

    if file_id.file_type == FileType.PHOTO:
        return raw.types.InputPhotoFileLocation(
            id=file_id.media_id,
            access_hash=file_id.access_hash,
            file_reference=file_id.file_reference,
            thumb_size=file_id.thumbnail_size,
        )

    return raw.types.InputDocumentFileLocation(
        id=file_id.media_id,
        access_hash=file_id.access_hash,
        file_reference=file_id.file_reference,
        thumb_size=file_id.thumbnail_size,
    )


async def _safe_stop(session: Session) -> None:
    try:
        await session.stop()
    except Exception:
        pass


async def _new_session(client, dc_id: int) -> Session:
    """Create (and authorise, if needed) a media session for ``dc_id``."""
    test_mode = await client.storage.test_mode()
    home_dc = await client.storage.dc_id()

    if dc_id == home_dc:
        auth_key = await client.storage.auth_key()
    else:
        auth_key = await Auth(client, dc_id, test_mode).create()

    session = Session(client, dc_id, auth_key, test_mode, is_media=True)
    await session.start()

    if dc_id != home_dc:
        # Bots must import an exported authorisation before they may read
        # files that live on a foreign datacenter.
        for attempt in range(3):
            try:
                exported = await client.invoke(
                    raw.functions.auth.ExportAuthorization(dc_id=dc_id)
                )
                await session.invoke(
                    raw.functions.auth.ImportAuthorization(
                        id=exported.id, bytes=exported.bytes
                    )
                )
                break
            except AuthBytesInvalid:
                logger.warning(
                    f"DC{dc_id}: AUTH_BYTES_INVALID on import "
                    f"(attempt {attempt + 1}/3)"
                )
                await asyncio.sleep(1)
            except Exception as exc:
                await _safe_stop(session)
                raise exc
        else:
            await _safe_stop(session)
            raise AuthBytesInvalid(
                "AUTH_BYTES_INVALID: could not authorise media session"
            )

    logger.info(f"DC{dc_id}: media session ready (home DC{home_dc})")
    return session


async def get_session(client, dc_id: int) -> Session:
    """Return a cached media session for ``dc_id``, creating it on demand."""
    session = _sessions.get(dc_id)
    if session is not None:
        return session
    async with _dc_locks[dc_id]:
        session = _sessions.get(dc_id)
        if session is not None:
            return session
        session = await _new_session(client, dc_id)
        _sessions[dc_id] = session
        return session


async def drop_session(dc_id: int) -> None:
    """Discard a broken session so the next call rebuilds it."""
    async with _dc_locks[dc_id]:
        session = _sessions.pop(dc_id, None)
    if session is not None:
        await _safe_stop(session)
        logger.info(f"DC{dc_id}: media session dropped")


async def close_sessions() -> None:
    """Shut every cached media session down (called on bot shutdown or reset)."""
    dc_ids = list(_sessions.keys())
    sessions: list[tuple[int, Session]] = []
    for dc_id in dc_ids:
        async with _dc_locks[dc_id]:
            sess = _sessions.pop(dc_id, None)
            if sess is not None:
                sessions.append((dc_id, sess))
    for dc_id, session in sessions:
        await _safe_stop(session)
        logger.info(f"DC{dc_id}: media session closed")


def active_dcs() -> list[int]:
    """DC ids we currently hold a media session for (for /server output)."""
    return sorted(_sessions)


def _pick_limit(pos: int, remaining: int) -> int:
    """Compute a valid MTProto ``upload.GetFile`` limit for ``pos``.

    Telegram requires:
      1. ``pos % 4096 == 0``
      2. ``limit % 4096 == 0``
      3. ``1048576 % limit == 0`` (limit is a power of 2 in 4 KiB .. 1 MiB)
      4. ``pos % limit == 0`` and ``(pos // 1 MiB) == ((pos + limit - 1) // 1 MiB)``
    """
    max_aligned = CHUNK if (pos % CHUNK == 0) else (pos & -pos)
    max_aligned = max(ALIGN, min(CHUNK, max_aligned))

    if remaining >= max_aligned:
        return max_aligned

    step = ALIGN
    while step < remaining and step < max_aligned:
        step <<= 1
    return min(step, max_aligned)


async def _get_file(client, dc_id: int, location, offset: int, limit: int):
    """One ``upload.GetFile`` call with recovery for the usual failures.

    Returns ``(result, active_dc_id)`` so callers keep any ``FileMigrate`` DC.
    """
    last_error: Optional[BaseException] = None

    for attempt in range(3):
        session = await get_session(client, dc_id)
        try:
            res = await session.invoke(
                raw.functions.upload.GetFile(
                    location=location, offset=offset, limit=limit
                ),
                sleep_threshold=0,  # surface FloodWait, we handle it ourselves
            )
            return res, dc_id
        except FloodWait as e:
            if e.value <= 10 and attempt < 2:
                logger.info(f"DC{dc_id}: short FloodWait ({e.value}s), waiting inline")
                await asyncio.sleep(e.value + 1)
                continue
            note_flood(dc_id, e.value + 5)
            raise
        except AuthBytesInvalid as e:
            last_error = e
            logger.warning(f"DC{dc_id}: auth bytes invalid, rebuilding session")
            await drop_session(dc_id)
        except FileMigrate as e:
            new_dc = int(getattr(e, "value", 0) or 0)
            logger.warning(f"DC{dc_id}: file migrated to DC{new_dc}")
            if new_dc and new_dc != dc_id:
                dc_id = new_dc
                continue
            raise
        except (OSError, TimeoutError, InternalServerError, ServiceUnavailable) as e:
            last_error = e
            logger.warning(
                f"DC{dc_id}: transport error ({type(e).__name__}: {e}), "
                f"retry {attempt + 1}/3"
            )
            await drop_session(dc_id)
            await asyncio.sleep(1 + attempt)

    if last_error:
        raise last_error
    raise OSError(f"DC{dc_id}: upload.GetFile failed")


async def iter_range(
    client, file_id: FileId, offset: int, length: int
) -> AsyncGenerator[bytes, None]:
    """Yield up to ``length`` bytes of a file starting at ``offset``.

    Requests are aligned to 4 KiB boundaries to satisfy MTProto rules, and any
    leading or trailing alignment padding is sliced off so callers receive the
    exact ``[offset, offset + length)`` byte range.
    """
    if length <= 0 or offset < 0:
        return

    location = _location(file_id)
    dc_id = file_id.dc_id

    pos = max(0, offset - (offset % ALIGN))
    lead = offset - pos
    end = offset + length

    while pos < end:
        limit = _pick_limit(pos, end - pos)

        result, dc_id = await _get_file(client, dc_id, location, pos, limit)

        if isinstance(result, raw.types.upload.FileCdnRedirect):
            raise CdnRedirect(f"DC{dc_id}: file served from CDN")

        chunk = getattr(result, "bytes", b"") or b""
        if not chunk:
            break

        raw_len = len(chunk)
        pos += raw_len

        if lead:
            if raw_len <= lead:
                lead -= raw_len
                if raw_len < limit:
                    break
                continue
            chunk = chunk[lead:]
            lead = 0

        if pos > end:
            excess = pos - end
            chunk = chunk[: max(0, len(chunk) - excess)]

        clear_flood(dc_id)
        if chunk:
            yield chunk

        if raw_len < limit:  # end of file reached
            break