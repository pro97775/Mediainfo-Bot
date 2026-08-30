# """Datacenter-aware partial media downloader.

# Why this module exists
# ----------------------
# ``Client.stream_media`` (pyrogram / pyrofork) opens a *brand new* MTProto
# session for every call and, when the file lives on a datacenter other than the
# one the bot is logged into, performs a full
# ``auth.ExportAuthorization`` + ``auth.ImportAuthorization`` handshake each time.

# The bot probes a file in several growing steps (16 KB, 1 MB, 3 MB ...), so a
# single file used to trigger one session + one authorisation handshake per step.
# On foreign DCs Telegram answers that burst with ``FloodWait`` or
# ``AUTH_BYTES_INVALID``; pyrogram catches those internally, logs them and simply
# returns an empty generator.  The caller then sees a 0-byte file and silently
# falls back to Telegram's own (very poor) metadata -- this is the
# "sometimes no media info for files from other DC" bug.

# What this module does
# ---------------------
# * keeps **one long-lived media session per datacenter** and reuses it,
# * downloads **byte ranges** so probe steps can append instead of restarting,
# * records ``FloodWait`` **per datacenter** instead of globally,
# * recovers from ``AuthBytesInvalid`` by rebuilding the session,
# * follows ``FileMigrate`` (file moved to another DC),
# * reports CDN redirects so the caller can fall back to pyrogram.
# """

import asyncio
import logging
import time
from typing import AsyncGenerator, Optional

from pyrogram import raw, utils
from pyrogram.errors import (
    AuthBytesInvalid,
    FloodWait,
    FileMigrate,
    InternalServerError,
    ServiceUnavailable,
)
from pyrogram.file_id import FileId, FileType, ThumbnailSource
from pyrogram.session import Auth, Session

logger = logging.getLogger(__name__)

# Telegram requires upload.GetFile offsets/limits to be 4 KiB aligned and a
# single request may not cross a 1 MiB boundary.
ALIGN = 4 * 1024
CHUNK = 1024 * 1024

_sessions: dict[int, Session] = {}
_session_lock = asyncio.Lock()
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
    _dc_flood_until[dc_id] = time.monotonic() + seconds


def clear_flood(dc_id: int) -> None:
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
        else:
            await _safe_stop(session)
            raise AuthBytesInvalid(
                "AUTH_BYTES_INVALID: could not authorise media session"
            )

    logger.info(f"DC{dc_id}: media session ready (home DC{home_dc})")
    return session


async def get_session(client, dc_id: int) -> Session:
    """Return a cached media session for ``dc_id``, creating it on demand."""
    async with _session_lock:
        session = _sessions.get(dc_id)
        if session is not None:
            return session
        session = await _new_session(client, dc_id)
        _sessions[dc_id] = session
        return session


async def drop_session(dc_id: int) -> None:
    """Discard a broken session so the next call rebuilds it."""
    async with _session_lock:
        session = _sessions.pop(dc_id, None)
    if session is not None:
        await _safe_stop(session)
        logger.info(f"DC{dc_id}: media session dropped")


async def close_sessions() -> None:
    """Shut every cached media session down (called on bot shutdown)."""
    async with _session_lock:
        sessions = list(_sessions.items())
        _sessions.clear()
    for dc_id, session in sessions:
        await _safe_stop(session)
        logger.info(f"DC{dc_id}: media session closed")


def active_dcs() -> list[int]:
    """DC ids we currently hold a media session for (for /server output)."""
    return sorted(_sessions)


async def _get_file(client, dc_id: int, location, offset: int, limit: int):
    """One ``upload.GetFile`` call with recovery for the usual failures."""
    last_error: Optional[BaseException] = None

    for attempt in range(3):
        session = await get_session(client, dc_id)
        try:
            return await session.invoke(
                raw.functions.upload.GetFile(
                    location=location, offset=offset, limit=limit
                ),
                sleep_threshold=0,  # surface FloodWait, we handle it ourselves
            )
        except FloodWait as e:
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

    ``offset`` is aligned down to a 4 KiB boundary (a Telegram requirement);
    the caller receives the aligned data, which is fine for container parsing.
    """
    location = _location(file_id)
    dc_id = file_id.dc_id

    pos = offset - (offset % ALIGN)
    end = pos + length

    while pos < end:
        room = CHUNK - (pos % CHUNK)                    # stay in the 1 MiB window
        want = min(CHUNK, end - pos, room)
        limit = min(room, -(-want // ALIGN) * ALIGN)    # round up to 4 KiB

        result = await _get_file(client, dc_id, location, pos, limit)

        if isinstance(result, raw.types.upload.FileCdnRedirect):
            raise CdnRedirect(f"DC{dc_id}: file served from CDN")

        chunk = getattr(result, "bytes", b"") or b""
        if not chunk:
            break

        clear_flood(dc_id)
        yield chunk

        pos += len(chunk)
        if len(chunk) < limit:  # end of file reached
            break


