import asyncio
import os
import re
import uuid
from pathlib import Path
from typing import Dict, Optional

import aiohttp
import aiofiles
import urllib.parse
from pydantic import BaseModel
from pyrogram import errors

from anony import config, logger, app

# ------------------------------------------------------------------
# Global HTTP session (shared, like in downloader.py)
# ------------------------------------------------------------------

_session: Optional[aiohttp.ClientSession] = None
_session_lock = asyncio.Lock()

# Chunk size similar to downloader.py (1 MiB)
CHUNK_SIZE = 1 << 20

# 11-char YouTube ID regex (same logic as downloader.py) :contentReference[oaicite:0]{index=0}
YOUTUBE_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{11}$)


async def get_http_session() -> aiohttp.ClientSession:
    """Return a shared aiohttp session."""
    global _session
    if _session and not _session.closed:
        return _session

    async with _session_lock:
        if _session and not _session.closed:
            return _session

        timeout = aiohttp.ClientTimeout(total=600, sock_connect=20, sock_read=60)
        connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300, enable_cleanup_closed=True)
        _session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        return _session


async def close_http_session() -> None:
    """Close shared aiohttp session (if needed on shutdown)."""
    global _session
    async with _session_lock:
        if _session and not _session.closed:
            await _session.close()
        _session = None


def extract_video_id(link: str) -> str:
    """
    Extract a YouTube video id from:
      - direct id
      - full youtube url
      - youtu.be short url
    Mirrors the logic in downloader.py. :contentReference[oaicite:1]{index=1}
    """
    if not link:
        return ""
    s = link.strip()
    if YOUTUBE_ID_RE.match(s):
        return s
    if "v=" in s:
        return s.split("v=")[-1].split("&")[0]
    last = s.split("/")[-1].split("?")[0]
    if YOUTUBE_ID_RE.match(last):
        return last
    return ""


class MusicTrack(BaseModel):
    cdnurl: str
    key: str
    name: str
    artist: str
    tc: str
    # cover: str
    # lyrics: str
    # album: str
    # year: int
    # duration: int
    # platform: str


class FallenApi:
    """
    Fallen API client reworked to behave similar to downloader.py:
      1) First try merged_api /song/{video_id}?media_type=audio&return_file=true
      2) If that fails -> fallback to old JSON /track + Telegram/CDN download.
    """

    def __init__(self, retries: int = 3, timeout: int = 15):
        # For merged_api style backend
        self.api_url = (config.API_URL or "").rstrip("/")
        self.api_key = config.API_KEY

        self.retries = retries
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.download_dir = Path("downloads")
        self.download_dir.mkdir(exist_ok=True)

    def _get_headers(self) -> Dict[str, str]:
        # Old style header for JSON /track API (we keep for fallback)
        return {
            "X-API-Key": self.api_key,
            "Accept": "application/json",
        }

    # ------------------------------------------------------------------
    # New: merged_api-style binary /song downloader (like downloader.py) :contentReference[oaicite:2]{index=2}
    # ------------------------------------------------------------------

    async def _download_via_merged_api(self, url: str) -> Optional[str]:
        """
        Call merged_api-like backend:

            GET {API_URL}/song/{video_id}?media_type=audio&api_key=...&return_file=true

        The backend:
          - downloads from YouTube on the API server
          - uploads to DB-channel (on API side)
          - returns final audio bytes in the HTTP response

        We:
          - choose extension from headers (Content-Disposition / Content-Type)
          - stream to local 'downloads/{video_id}.{ext}'
        """
        if not self.api_url or not self.api_key:
            return None

        vid = extract_video_id(url)
        if not vid:
            logger.warning(f"[FALLEN] Could not extract video id from url='{url}'")
            return None

        endpoint = f"{self.api_url}/song/{vid}"
        params = {
            "media_type": "audio",
            "api_key": self.api_key,
            "return_file": "true",
        }

        try:
            session = await get_http_session()
            logger.info(f"[FALLEN] Merged API audio request: {endpoint} params={params}")
            async with session.get(endpoint, params=params, timeout=self.timeout) as resp:
                if resp.status != 200:
                    body = ""
                    try:
                        body = await resp.text()
                    except Exception:
                        pass
                    logger.warning(
                        f"[FALLEN] Merged API returned HTTP {resp.status} for {vid}. "
                        f"Body: {body[:200]}"
                    )
                    return None

                content_type = (resp.headers.get("Content-Type") or "").lower()
                cd = resp.headers.get("Content-Disposition") or ""

                # Try derive filename / ext from Content-Disposition first
                ext = None
                filename = None

                if cd:
                    match = re.findall(r'filename="?([^";]+)"?', cd)
                    if match:
                        filename = match[0]

                if filename and "." in filename:
                    ext = filename.rsplit(".", 1)[-1].lower()

                # Fallback: guess extension from Content-Type
                if not ext:
                    if "webm" in content_type or "opus" in content_type or "ogg" in content_type:
                        ext = "webm"
                    elif "mp3" in content_type:
                        ext = "mp3"
                    elif "mpeg" in content_type or "aac" in content_type or "mp4" in content_type:
                        ext = "m4a"
                    else:
                        # Safe default for audio
                        ext = "m4a"

                out_path = self.download_dir / f"{vid}.{ext}"
                logger.info(
                    f"[FALLEN] Saving merged API audio for {vid}: "
                    f"{out_path} (Content-Type='{content_type}')"
                )

                async with aiofiles.open(out_path, "wb") as f:
                    async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                        if not chunk:
                            break
                        await f.write(chunk)

            if out_path.exists():
                return str(out_path)

            logger.warning(
                f"[FALLEN] Merged API download finished but file not found for {vid}: {out_path}"
            )
            return None

        except Exception as e:
            logger.warning(f"[FALLEN] Merged API error for {vid}: {e}", exc_info=True)
            return None

    # ------------------------------------------------------------------
    # Legacy JSON /track + CDN/TG code (kept as fallback) :contentReference[oaicite:3]{index=3}
    # ------------------------------------------------------------------

    async def get_track(self, url: str) -> Optional[MusicTrack]:
        endpoint = f"{self.api_url}/track?url={urllib.parse.quote(url)}"

        for attempt in range(1, self.retries + 1):
            try:
                session = await get_http_session()
                async with session.get(
                    endpoint,
                    headers=self._get_headers(),
                    timeout=self.timeout,
                ) as resp:
                    data = await resp.json(content_type=None)
                    if resp.status == 200:
                        return MusicTrack(**data)

                    error_msg = data.get("error") or data.get("message")
                    status = data.get("status", resp.status)
                    logger.warning(
                        f"[API ERROR] {error_msg or 'Unexpected error'} (status {status})"
                    )
                    return None

            except aiohttp.ClientError as e:
                logger.warning(
                    f"[NETWORK ERROR] Attempt {attempt}/{self.retries} failed: {e}"
                )
            except asyncio.TimeoutError:
                logger.warning(
                    f"[TIMEOUT] Attempt {attempt}/{self.retries} exceeded timeout."
                )
            except Exception as e:
                logger.warning(f"[UNEXPECTED ERROR] {e}")

            await asyncio.sleep(1)

        logger.warning("[FAILED] All retry attempts exhausted in get_track.")
        return None

    async def download_cdn(self, cdn_url: str) -> Optional[str]:
        for attempt in range(1, self.retries + 1):
            try:
                session = await get_http_session()
                async with session.get(cdn_url, timeout=self.timeout) as resp:
                    if resp.status != 200:
                        logger.warning(
                            f"[HTTP {resp.status}] Failed to download from {cdn_url}"
                        )
                        return None

                    cd = resp.headers.get("Content-Disposition")
                    if cd:
                        match = re.findall(r'filename="?([^";]+)"?', cd)
                        filename = match[0] if match else None
                    else:
                        filename = None

                    if not filename:
                        filename = (
                            os.path.basename(cdn_url.split("?")[0])
                            or f"{uuid.uuid4()[:8]}.mp3"
                        )

                    save_path = self.download_dir / filename

                    async with aiofiles.open(save_path, "wb") as f:
                        async for chunk in resp.content.iter_chunked(16 * 1024):
                            if chunk:
                                await f.write(chunk)

                    logger.info(f"Download complete: {save_path}")
                    return str(save_path)

            except aiohttp.ClientError as e:
                logger.warning(
                    f"[NETWORK ERROR] Attempt {attempt}/{self.retries} failed: {e}"
                )
            except asyncio.TimeoutError:
                logger.warning(
                    f"[TIMEOUT] Attempt {attempt}/{self.retries} exceeded timeout."
                )
            except Exception as e:
                logger.warning(f"[UNEXPECTED ERROR] {e}")

            await asyncio.sleep(1)

        logger.warning("[FAILED] CDN download attempts exhausted.")
        return None

    # ------------------------------------------------------------------
    # Public entry: download_track (new flow + fallback) :contentReference[oaicite:4]{index=4}
    # ------------------------------------------------------------------

    async def download_track(self, url: str) -> Optional[str]:
        """
        Main entry used by your bot:

        1) Try merged_api-style /song audio (exactly like downloader.py).
        2) If that fails, fall back to:
           - /track JSON
           - Telegram t.me link
           - CDN direct file download
        """

        # 1) Preferred: merged_api /song backend
        merged_path = await self._download_via_merged_api(url)
        if merged_path:
            logger.info(f"[FALLEN] Downloaded via merged API: {merged_path}")
            return merged_path

        # 2) Fallback: original JSON + TG/CDN
        track = await self.get_track(url)
        if not track:
            logger.warning("[❌] No track metadata found from fallback JSON API.")
            return None

        dl_url = track.cdnurl
        tg_match = re.match(r"https?://t\.me/([^/]+)/(\d+)", dl_url)
        if tg_match:
            chat, msg_id = tg_match.groups()
            try:
                msg = await app.get_messages(chat_id=chat, message_ids=int(msg_id))
                file_path = await msg.download(file_name=self.download_dir)
                logger.info(f"Telegram media downloaded: {file_path}")
                return file_path
            except errors.FloodWait as e:
                logger.warning(f"[FLOODWAIT] Sleeping {e.value}s before retry.")
                await asyncio.sleep(e.value)
                # Single retry to avoid infinite recursion
                try:
                    msg = await app.get_messages(chat_id=chat, message_ids=int(msg_id))
                    file_path = await msg.download(file_name=self.download_dir)
                    logger.info(
                        f"Telegram media downloaded after FloodWait: {file_path}"
                    )
                    return file_path
                except Exception as e2:
                    logger.warning(f"[TG DOWNLOAD ERROR after FloodWait] {e2}")
                    return None
            except Exception as e:
                logger.warning(f"[TG DOWNLOAD ERROR] {e}")
                return None

        # If not a t.me link, fall back to CDN download
        return await self.download_cdn(dl_url)
