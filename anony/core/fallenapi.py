# fallenapi.py
# Updated to prefer job.result.public_url when jobStatus returns server-side results.
# Converts relative public_url -> full URL and downloads it.
import asyncio
import os
import re
import uuid
import shutil
import subprocess
from pathlib import Path
from typing import Dict, Optional, Any

import aiohttp
from pydantic import BaseModel

# project imports (fall back to minimal stubs when used standalone)
try:
    from anony import logger, config, app
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

# Regex for YouTube IDs & t.me
_YT_ID_RE = re.compile(r"""(?x)(?:v=|\/)([A-Za-z0-9_-]{11})|youtu\.be\/([A-Za-z0-9_-]{11})""")
_TG_RE = re.compile(r"https?://t\.me/(?:(c)/(\d+)|([^/]+)/(\d+))", re.IGNORECASE)

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
        timeout: int = 30,
        download_dir: str = "downloads",
        job_poll_attempts: int = 40,
        job_poll_interval: float = 2.0,
        job_poll_backoff: float = 1.25,
        min_valid_size_bytes: int = 1024 * 5,
    ):
        self.api_url = (api_url or getattr(config, "API_URL", "") or "").rstrip("/")
        self.api_key = api_key or getattr(config, "API_KEY", "") or ""
        self.retries = retries
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.job_poll_attempts = job_poll_attempts
        self.job_poll_interval = job_poll_interval
        self.job_poll_backoff = job_poll_backoff
        self.min_valid_size_bytes = min_valid_size_bytes
        self.ffmpeg_path = shutil.which("ffmpeg")

    def _get_headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        return headers

    async def _request_json(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[Any]:
        if not self.api_url:
            logger.warning("[FallenApi] API_URL not configured.")
            return None

        url = endpoint if endpoint.startswith("http") else f"{self.api_url}/{endpoint.lstrip('/')}"
        params = dict(params or {})
        if self.api_key and "api_key" not in params:
            params["api_key"] = self.api_key

        last_exc = None
        for attempt in range(1, self.retries + 1):
            try:
                async with aiohttp.ClientSession(timeout=self.timeout) as session:
                    async with session.get(url, headers=self._get_headers(), params=params) as resp:
                        text = await resp.text()
                        try:
                            data = await resp.json(content_type=None)
                        except Exception:
                            stripped = text.strip()
                            logger.debug("[FallenApi] Non-JSON response from %s: %s", url, stripped[:400])
                            return stripped
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

    # arc endpoints
    async def youtube_v2_download(self, query: str, isVideo: bool = False) -> Optional[Any]:
        query_to_send = query
        if isinstance(query, str) and ("youtube." in query or "youtu.be" in query):
            m = _YT_ID_RE.search(query)
            if m:
                vid = m.group(1) or m.group(2)
                if vid:
                    query_to_send = vid
            else:
                if "watch?v=" in query:
                    query_to_send = query.split("watch?v=")[-1].split("&")[0]
                else:
                    last = query.rstrip("/").split("/")[-1]
                    if last:
                        query_to_send = last
        return await self._request_json("youtube/v2/download", params={"query": query_to_send, "isVideo": str(isVideo).lower()})

    async def youtube_job_status(self, job_id: str) -> Optional[Any]:
        return await self._request_json("youtube/jobStatus", params={"job_id": job_id})

    async def youtube_v1_download(self, query: str, isVideo: bool = False) -> Optional[Any]:
        return await self._request_json("youtube/v1/download", params={"query": query, "isVideo": str(isVideo).lower()})

    # extractor: now includes public_url & file_path handling inside nested job/result wrapper
    def _extract_candidate_from_obj(self, obj: Any) -> Optional[str]:
        if obj is None:
            return None
        # raw string -> direct URL or path
        if isinstance(obj, str):
            s = obj.strip()
            return s if s else None
        # list -> first
        if isinstance(obj, list) and obj:
            return self._extract_candidate_from_obj(obj[0])
        # dict -> many shapes
        if isinstance(obj, dict):
            # 1) if top-level 'job' wrapper like arcapi: {"status":"success","job":{...}}
            if "job" in obj and isinstance(obj["job"], dict):
                job = obj["job"]
                # prefer job.result.public_url
                res = job.get("result")
                if isinstance(res, dict):
                    # public_url preferred (relative path on API host)
                    pub = res.get("public_url")
                    if isinstance(pub, str) and pub.strip():
                        return pub.strip()
                    # fallback to result.file_path (server local path) -> not directly downloadable
                    fp = res.get("file_path")
                    if isinstance(fp, str) and fp.strip():
                        return fp.strip()
                # sometimes result is wrapped differently
            # 2) common direct keys
            for k in ("cdnurl", "download_url", "url", "public_url", "file_path", "file", "media", "tg_link", "telegram_link", "message_link"):
                v = obj.get(k)
                if isinstance(v, str) and v.strip():
                    return v.strip()
            # 3) nested wrappers
            for wrapper in ("result", "results", "data", "items", "tracks", "payload", "message"):
                w = obj.get(wrapper)
                if w:
                    cand = self._extract_candidate_from_obj(w)
                    if cand:
                        return cand
        return None

    # build a full http url from relative public_url returned by API
    def _normalize_candidate_to_url(self, candidate: str) -> Optional[str]:
        if not candidate:
            return None
        candidate = candidate.strip()
        # If candidate looks like a t.me link -> return as-is
        if candidate.startswith("http://") or candidate.startswith("https://"):
            return candidate
        # If candidate starts with '/' treat as relative public URL from the API host
        if candidate.startswith("/"):
            # self.api_url may have trailing slash trimmed; add slash between host and path
            return f"{self.api_url.rstrip('/')}{candidate}"
        # If candidate is a server absolute file path (e.g. /root/...), it's not directly downloadable
        # So return None here (we expect API to also provide public_url)
        if candidate.startswith("/root") or candidate.startswith("/home"):
            return None
        # otherwise if it looks like a filename (gJLVTKhTnog.mp4) -> try to construct /media/<name> fallback
        if re.match(r"^[\w\-. ]+\.(mp3|mp4|webm|m4a|ogg|wav|flac)$", candidate, re.IGNORECASE):
            return f"{self.api_url.rstrip('/')}/media/{candidate.lstrip('/')}"
        # last resort: treat as path and prefix with host
        return f"{self.api_url.rstrip('/')}/{candidate.lstrip('/')}"

    # minimal extension selection & ffmpeg conversion (kept from prior)
    def _choose_extension_from_ct(self, content_type: Optional[str]) -> str:
        if not content_type:
            return ".webm"
        ct = content_type.lower()
        if "mpeg" in ct or "mp3" in ct:
            return ".mp3"
        if "wav" in ct:
            return ".wav"
        if "ogg" in ct:
            return ".ogg"
        if "flac" in ct:
            return ".flac"
        if "aac" in ct:
            return ".aac"
        if "mp4" in ct or "video" in ct:
            return ".mp4"
        return ".webm"

    async def _download_cdn_to_file(self, cdn_url: str, preferred_name: Optional[str] = None) -> Optional[str]:
        for attempt in range(1, self.retries + 1):
            try:
                async with aiohttp.ClientSession(timeout=self.timeout) as session:
                    async with session.get(cdn_url) as resp:
                        if resp.status != 200:
                            logger.warning("[FallenApi] CDN responded %s for %s", resp.status, cdn_url)
                            return None
                        cd = resp.headers.get("Content-Disposition")
                        ct = resp.headers.get("Content-Type", "")
                        filename = None
                        if preferred_name:
                            filename = preferred_name
                        elif cd:
                            m = re.findall('filename="?([^";]+)"?', cd)
                            if m:
                                filename = m[0]
                        if not filename:
                            base = os.path.basename(cdn_url.split("?")[0]) or f"{uuid.uuid4().hex[:8]}"
                            ext = self._choose_extension_from_ct(ct)
                            filename = base if os.path.splitext(base)[1] else f"{base}{ext}"
                        save_path = self.download_dir / filename
                        with open(save_path, "wb") as f:
                            async for chunk in resp.content.iter_chunked(16 * 1024):
                                if chunk:
                                    f.write(chunk)
                        size = save_path.stat().st_size if save_path.exists() else 0
                        logger.info("[FallenApi] Download saved to %s (%s bytes). Content-Type=%s", save_path, size, ct)
                        if size < self.min_valid_size_bytes:
                            logger.warning("[FallenApi] Downloaded file too small (%s bytes) -> deleting and retrying.", size)
                            try:
                                save_path.unlink(missing_ok=True)
                            except Exception:
                                pass
                            continue
                        # optional conversion step omitted here for brevity; you can add ffmpeg conversion if needed
                        return str(save_path)
            except Exception as e:
                logger.exception("[FallenApi] Error downloading CDN: %s", e)
            await asyncio.sleep(1)
        logger.warning("[FallenApi] CDN download attempts exhausted for %s", cdn_url)
        return None

    async def _download_telegram_media(self, tme_url: str) -> Optional[str]:
        if not app:
            logger.warning("[FallenApi] Pyrogram app not available; cannot download t.me media.")
            return None
        # support both /c/ and username style
        match = _TG_RE.match(tme_url)
        if match:
            if match.group(1):  # c style
                _, channel_id, _, _ = match.groups()
                # convert internal channel id to -100... if needed (pyrogram uses numeric chat_id)
                # channel_id is numeric string like '123456789' so chat id becomes -100123456789
                chat_id = int(f"-100{channel_id}")
                msg_id = int(match.group(4)) if match.group(4) else None
            else:
                # username style: group 3 = username, group4 = msg id
                username = match.group(3)
                msg_id = int(match.group(4))
                chat_id = username
        else:
            # fallback parse last two path segments
            parts = tme_url.rstrip("/").split("/")
            if len(parts) >= 2 and parts[-1].isdigit():
                chat_id = parts[-2]
                msg_id = int(parts[-1])
            else:
                return None
        try:
            msg = await app.get_messages(chat_id=chat_id, message_ids=int(msg_id))
            file_path = await msg.download(file_name=str(self.download_dir))
            logger.info("[FallenApi] Telegram media downloaded: %s", file_path)
            return file_path
        except Exception as e:
            if hasattr(pyrogram_errors, "FloodWait") and isinstance(e, getattr(pyrogram_errors, "FloodWait")):
                wait = getattr(e, "value", 5)
                logger.warning("[FallenApi] FloodWait %s - sleeping then retrying.", wait)
                await asyncio.sleep(wait)
                return await self._download_telegram_media(tme_url)
            logger.exception("[FallenApi] Error downloading telegram media: %s", e)
            return None

    async def download_track(self, query: str, isVideo: bool = False, prefer_v2: bool = True) -> Optional[str]:
        logger.info("[FallenApi] Requesting download for %s (prefer_v2=%s)", query, prefer_v2)
        resp = None
        if prefer_v2:
            resp = await self.youtube_v2_download(query, isVideo=isVideo)
            if not resp:
                logger.debug("[FallenApi] v2 empty, falling back to v1")
                resp = await self.youtube_v1_download(query, isVideo=isVideo)
        else:
            resp = await self.youtube_v1_download(query, isVideo=isVideo)
        if resp is None:
            logger.warning("[FallenApi] No response from API for download request.")
            return None

        # debug snapshot
        try:
            if isinstance(resp, dict):
                logger.debug("[FallenApi] v2 response keys: %s", list(resp.keys()))
        except Exception:
            pass

        # try immediate candidate (could be public_url string or nested job.result.public_url)
        candidate = self._extract_candidate_from_obj(resp)

        # if candidate looks like a server-only file path (starting with /root or similar), ignore it;
        # prefer public_url from jobStatus
        job_id = None
        if isinstance(resp, dict):
            job_id = resp.get("job_id") or resp.get("job")

        # if job_id present and no candidate or candidate is server-local path -> poll jobStatus
        if job_id and (not candidate or candidate.startswith("/root") or candidate.startswith("/home")):
            logger.info("[FallenApi] Received job_id %s, polling jobStatus up to %s attempts.", job_id, self.job_poll_attempts)
            interval = self.job_poll_interval
            for attempt in range(1, self.job_poll_attempts + 1):
                await asyncio.sleep(interval)
                status = await self.youtube_job_status(str(job_id))
                if not status:
                    logger.debug("[FallenApi] jobStatus empty on attempt %s/%s", attempt, self.job_poll_attempts)
                    interval *= self.job_poll_backoff
                    continue
                # prefer nested job.result.public_url / file_path
                candidate = self._extract_candidate_from_obj(status)
                # If candidate is public_url (relative path) or t.me or direct cdnurl, break
                if candidate:
                    logger.info("[FallenApi] jobStatus returned candidate on attempt %s: %s", attempt, str(candidate)[:200])
                    break
                interval *= self.job_poll_backoff
            if not candidate:
                logger.warning("[FallenApi] job did not complete within polling window (job_id=%s).", job_id)
                return None

        if not candidate:
            logger.warning("[FallenApi] No downloadable candidate found in API response.")
            return None

        # Normalize candidate to an actual HTTP URL or t.me link
        normalized = self._normalize_candidate_to_url(candidate)
        if not normalized:
            # candidate might be a t.me link (checked in extractor) -> try downloading via telegram if possible
            if candidate.startswith("https://t.me") or candidate.startswith("http://t.me"):
                tg_path = await self._download_telegram_media(candidate)
                return tg_path
            logger.warning("[FallenApi] Candidate could not be normalized to URL: %s", candidate)
            return None

        # If normalized points to API host and is a relative public_url we've constructed, download it
        # Use preferred_name when query is youtube id/url
        preferred_name = None
        if isinstance(query, str) and ("youtube." in query or "youtu.be" in query):
            m = _YT_ID_RE.search(query)
            vid = (m.group(1) or m.group(2)) if m else None
            if not vid and "watch?v=" in query:
                vid = query.split("watch?v=")[-1].split("&")[0]
            if vid:
                preferred_name = f"{vid}.mp4" if isVideo else f"{vid}.webm"

        # download the normalized URL
        file_path = await self._download_cdn_to_file(normalized, preferred_name=preferred_name)
        if not file_path:
            logger.warning("[FallenApi] Failed to download file from normalized url: %s", normalized)
            return None

        # validate size
        try:
            p = Path(file_path)
            if not p.exists():
                logger.warning("[FallenApi] Downloaded path missing: %s", file_path)
                return None
            size = p.stat().st_size
            if size < self.min_valid_size_bytes:
                logger.warning("[FallenApi] Downloaded file too small (%s bytes): %s", size, file_path)
                try:
                    p.unlink(missing_ok=True)
                except Exception:
                    pass
                return None
        except Exception as e:
            logger.exception("[FallenApi] Error validating downloaded file: %s", e)
            return None

        logger.info("[FallenApi] download_track_v2 completed -> %s", file_path)
        return file_path

# module-level client
client = FallenApi()
