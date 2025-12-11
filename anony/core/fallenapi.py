# fallenapi.py
# Robust FallenApi client (arcapi-style) with v2 handling for search-terms and urls,
# jobStatus polling, flexible response parsing, CDN + t.me downloads.
import asyncio
import os
import re
import uuid
from pathlib import Path
from typing import Dict, Optional, Any, Union

import aiohttp
from pydantic import BaseModel

# project imports (fall back to minimal stubs when running standalone)
try:
    from anony import logger, app
    from config import Config
except Exception:
    import logging as _logging
    logger = getattr(_logging, "getLogger")("fallenapi")
    logger.setLevel(_logging.DEBUG)
    if not logger.handlers:
        logger.addHandler(_logging.StreamHandler())

    class _Cfg:
        API_URL = os.environ.get("API_URL", "")
        API_KEY = os.environ.get("API_KEY", "")
    config = _Cfg()
    app = None

try:
    from pyrogram import errors as pyrogram_errors
except Exception:
    class _PE: pass
    pyrogram_errors = _PE()

# regex for extracting a YouTube video id
_YT_ID_RE = re.compile(
    r"""(?x)
    (?:v=|\/)([A-Za-z0-9_-]{11})      # watch?v=ID or /ID
    |youtu\.be\/([A-Za-z0-9_-]{11})   # youtu.be/ID
    """
)

# t.me link regex
_TG_RE = re.compile(r"https?://t\.me/([^/]+)/(\d+)", re.IGNORECASE)

class MusicTrack(BaseModel):
    cdnurl: Optional[str] = None
    key: Optional[str] = None
    name: Optional[str] = None
    artist: Optional[str] = None
    tc: Optional[str] = None

class FallenApi:
    def __init__(
        self,
        api_url: Optional[str] = None,
        api_key: Optional[str] = None,
        retries: int = 3,
        timeout: int = 20,
        download_dir: str = "downloads",
        job_poll_attempts: int = 20,
        job_poll_interval: float = 2.0,
    ):
        self.api_url = Config.API_URL.rstrip("/")
        self.api_key = Config.API_KEY
        self.retries = retries
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.job_poll_attempts = job_poll_attempts
        self.job_poll_interval = job_poll_interval

    def _get_headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        return headers

    async def _request_json(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[Any]:
        """
        Generic GET with retries. Returns parsed JSON (could be dict/list/string).
        """
        if not self.api_url:
            logger.warning("[FallenApi] API_URL not configured.")
            return None

        if endpoint.startswith("http://") or endpoint.startswith("https://"):
            url = endpoint
        else:
            url = f"{self.api_url}/{endpoint.lstrip('/')}"

        params = dict(params or {})
        if self.api_key and "api_key" not in params:
            params["api_key"] = self.api_key

        last_exc = None
        for attempt in range(1, self.retries + 1):
            try:
                async with aiohttp.ClientSession(timeout=self.timeout) as session:
                    async with session.get(url, headers=self._get_headers(), params=params) as resp:
                        text = await resp.text()
                        # try json, but some endpoints might return plain text (URL)
                        try:
                            data = await resp.json(content_type=None)
                        except Exception:
                            # not JSON, return raw text (often a direct URL)
                            stripped = text.strip()
                            logger.debug("[FallenApi] Non-JSON response from %s: %s", url, stripped[:200])
                            return stripped

                        # return parsed JSON
                        if 200 <= resp.status < 300:
                            return data
                        else:
                            logger.warning("[FallenApi] API error %s (status %s) from %s", data, resp.status, url)
                            return data
            except asyncio.TimeoutError as e:
                last_exc = e
                logger.warning("[FallenApi] Timeout attempt %s/%s for %s", attempt, self.retries, url)
            except aiohttp.ClientError as e:
                last_exc = e
                logger.warning("[FallenApi] Network error attempt %s/%s for %s: %s", attempt, self.retries, url, e)
            except Exception as e:
                last_exc = e
                logger.exception("[FallenApi] Unexpected error attempt %s/%s for %s: %s", attempt, self.retries, url, e)

            await asyncio.sleep(1)

        logger.warning("[FallenApi] All retries exhausted for %s. Last error: %s", url, repr(last_exc))
        return None

    # -----------------
    # arc-style endpoints
    # -----------------

    async def youtube_search(self, query: str) -> Optional[Any]:
        return await self._request_json("youtube/search", params={"query": query})

    async def youtube_v1_download(self, query: str, isVideo: bool = False) -> Optional[Any]:
        return await self._request_json("youtube/v1/download", params={"query": query, "isVideo": str(isVideo).lower()})

    async def youtube_v2_download(self, query: str, isVideo: bool = False) -> Optional[Any]:
        """
        If query is a YouTube URL, extract id; otherwise send the raw query (supports search terms like 'husn').
        """
        query_to_send = query
        if isinstance(query, str) and ("youtube." in query or "youtu.be" in query):
            m = _YT_ID_RE.search(query)
            if m:
                vid = m.group(1) or m.group(2)
                if vid:
                    query_to_send = vid
            else:
                # fallback: try to extract watch param or last path segment
                if "watch?v=" in query:
                    query_to_send = query.split("watch?v=")[-1].split("&")[0]
                else:
                    last = query.rstrip("/").split("/")[-1]
                    if last:
                        query_to_send = last

        return await self._request_json("youtube/v2/download", params={"query": query_to_send, "isVideo": str(isVideo).lower()})

    async def youtube_job_status(self, job_id: str) -> Optional[Any]:
        return await self._request_json("youtube/jobStatus", params={"job_id": job_id})

    async def spotify_get_name(self, link: str) -> Optional[Any]:
        return await self._request_json("spotify/name", params={"link": link})

    async def spotify_download(self, link: str, isVideo: bool = False) -> Optional[Any]:
        return await self._request_json("spotify/download", params={"link": link, "isVideo": str(isVideo).lower()})

    # -----------------
    # download helpers
    # -----------------

    async def _download_cdn_to_file(self, cdn_url: str) -> Optional[str]:
        for attempt in range(1, self.retries + 1):
            try:
                async with aiohttp.ClientSession(timeout=self.timeout) as session:
                    async with session.get(cdn_url) as resp:
                        if resp.status != 200:
                            logger.warning("[FallenApi] CDN responded %s for %s", resp.status, cdn_url)
                            return None

                        cd = resp.headers.get("Content-Disposition")
                        filename = None
                        if cd:
                            match = re.findall('filename="?([^";]+)"?', cd)
                            if match:
                                filename = match[0]

                        if not filename:
                            filename = os.path.basename(cdn_url.split("?")[0]) or f"{uuid.uuid4().hex[:8]}.bin"
                            ct = resp.headers.get("Content-Type", "")
                            if "audio" in ct and not os.path.splitext(filename)[1]:
                                filename += ".mp3"

                        save_path = self.download_dir / filename
                        with open(save_path, "wb") as f:
                            async for chunk in resp.content.iter_chunked(16 * 1024):
                                if chunk:
                                    f.write(chunk)

                        logger.info("[FallenApi] CDN download complete: %s", save_path)
                        return str(save_path)
            except asyncio.TimeoutError:
                logger.warning("[FallenApi] CDN download timeout attempt %s/%s", attempt, self.retries)
            except aiohttp.ClientError as e:
                logger.warning("[FallenApi] CDN network error attempt %s/%s: %s", attempt, self.retries, e)
            except Exception as e:
                logger.exception("[FallenApi] Unexpected error when downloading CDN: %s", e)
            await asyncio.sleep(1)
        logger.warning("[FallenApi] CDN download attempts exhausted for %s", cdn_url)
        return None

    async def _download_telegram_media(self, tme_url: str) -> Optional[str]:
        if not app:
            logger.warning("[FallenApi] Pyrogram app not available; cannot download t.me media.")
            return None

        tg_match = _TG_RE.match(tme_url)
        if tg_match:
            chat, msg_id = tg_match.groups()
        else:
            # allow chat/msg path like 'channel/123'
            parts = tme_url.rstrip("/").split("/")
            if len(parts) >= 2 and parts[-1].isdigit():
                chat = parts[-2]
                msg_id = parts[-1]
            else:
                return None

        try:
            msg = await app.get_messages(chat_id=chat, message_ids=int(msg_id))
            file_path = await msg.download(file_name=str(self.download_dir))
            logger.info("[FallenApi] Telegram media downloaded: %s", file_path)
            return file_path
        except Exception as e:
            if hasattr(pyrogram_errors, "FloodWait") and isinstance(e, getattr(pyrogram_errors, "FloodWait")):
                wait = getattr(e, "value", 5)
                logger.warning("[FallenApi] FloodWait %s - sleeping then retrying.", wait)
                await asyncio.sleep(wait)
                return await self._download_telegram_media(tme_url)
            logger.warning("[FallenApi] Error downloading telegram media %s: %s", tme_url, e)
            return None

    # -----------------
    # high-level flow
    # -----------------

    def _extract_candidate_from_obj(self, obj: Any) -> Optional[str]:
        """
        Given a parsed JSON (dict/list/string), try to extract the first useful download URL.
        Returns cdn/download url or None.
        """
        if obj is None:
            return None

        # if server returned a plain string (often a direct url), return it
        if isinstance(obj, str):
            s = obj.strip()
            if s:
                return s

        # if it's a list, inspect first element
        if isinstance(obj, list) and obj:
            first = obj[0]
            return self._extract_candidate_from_obj(first)

        # if it's a dict, check common fields and nested wrappers
        if isinstance(obj, dict):
            # direct keys
            for k in ("cdnurl", "download_url", "url", "media", "file"):
                v = obj.get(k)
                if isinstance(v, str) and v.strip():
                    return v.strip()
            # wrapper keys: results, data, result
            for wrapper in ("results", "data", "result"):
                w = obj.get(wrapper)
                if w:
                    candidate = self._extract_candidate_from_obj(w)
                    if candidate:
                        return candidate
            # sometimes results are under items -> [0] -> cdnurl
            items = obj.get("items") or obj.get("tracks")
            if isinstance(items, list) and items:
                return self._extract_candidate_from_obj(items[0])
        return None

    async def download_track(self, query: str, prefer_v2: bool = True, isVideo: bool = False) -> Optional[str]:
        """
        Request v2 (preferred) with the provided query (search-term or URL).
        Handle job_id polling and multiple response shapes. Return local file path or None.
        """
        logger.info("[FallenApi] Requesting download for %s (prefer_v2=%s)", query, prefer_v2)

        resp = None
        if prefer_v2:
            resp = await self.youtube_v2_download(query, isVideo=isVideo)
            if not resp:
                logger.info("[FallenApi] youtube_v2_download returned empty, falling back to v1")
                resp = await self.youtube_v1_download(query, isVideo=isVideo)
        else:
            resp = await self.youtube_v1_download(query, isVideo=isVideo)

        if resp is None:
            logger.warning("[FallenApi] No response from API for download request.")
            return None

        # If the API returned a raw string URL or JSON, attempt to extract candidate
        dl_url = self._extract_candidate_from_obj(resp)

        # If API returned a job id but no url -> poll jobStatus
        job_id = None
        if isinstance(resp, dict):
            job_id = resp.get("job_id") or resp.get("job")

        if job_id and not dl_url:
            logger.info("[FallenApi] Received job_id %s, polling jobStatus up to %s attempts.", job_id, self.job_poll_attempts)
            for attempt in range(1, self.job_poll_attempts + 1):
                await asyncio.sleep(self.job_poll_interval)
                status = await self.youtube_job_status(str(job_id))
                if not status:
                    logger.debug("[FallenApi] jobStatus empty on attempt %s/%s", attempt, self.job_poll_attempts)
                    continue
                candidate = self._extract_candidate_from_obj(status)
                if candidate:
                    dl_url = candidate
                    resp = status
                    logger.info("[FallenApi] Job completed on attempt %s/%s - found url.", attempt, self.job_poll_attempts)
                    break
                else:
                    logger.debug("[FallenApi] jobStatus attempt %s/%s: still processing.", attempt, self.job_poll_attempts)

            if not dl_url:
                logger.warning("[FallenApi] job did not complete within polling window (job_id=%s).", job_id)
                return None

        if not dl_url:
            logger.warning("[FallenApi] No downloadable URL found in API response.")
            return None

        # If it's a t.me link or telegram-like, download via app
        if isinstance(dl_url, str) and (dl_url.startswith("https://t.me/") or _TG_RE.match(dl_url)):
            tg_path = await self._download_telegram_media(dl_url)
            if tg_path:
                return tg_path

        # else download from CDN
        file_path = await self._download_cdn_to_file(dl_url)
        return file_path

# module-level default client
client = FallenApi()
