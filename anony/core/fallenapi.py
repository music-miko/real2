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
        API_URL2 = os.environ.get("API_URL2", "")
        API_KEY2 = os.environ.get("API_KEY2", "")
    config = _Cfg()
    app = None

try:
    from pyrogram import errors as pyrogram_errors
except Exception:
    class _PE:
        class FloodWait(Exception):
            value = 5
    pyrogram_errors = _PE()

_YT_ID_RE = re.compile(r"""(?x)(?:v=|\/)([A-Za-z0-9_-]{11})|youtu\.be\/([A-Za-z0-9_-]{11})""")
_TG_RE = re.compile(r"https?://t\.me/(?:(c)/(\d+)|([^/]+)/(\d+))", re.IGNORECASE)


class MusicTrack(BaseModel):
    cdnurl: Optional[str] = None
    key: Optional[str] = None
    name: Optional[str] = None
    artist: Optional[str] = None
    tc: Optional[str] = None


def _as_download_dir(path: Path) -> str:
    """
    IMPORTANT: force pyrogram to treat this as DIRECTORY.
    Some pyrogram versions treat file_name as file path unless it ends with separator.
    """
    p = str(path.resolve())
    if not p.endswith(os.sep):
        p += os.sep
    return p


def _resolve_if_dir(download_result: str) -> Optional[str]:
    """
    Sometimes pyrogram can return a directory-ish path; ensure we return a file.
    If it's a dir, pick the newest file inside it.
    """
    if not download_result:
        return None
    p = Path(download_result)
    if p.exists() and p.is_file():
        return str(p)
    if p.exists() and p.is_dir():
        files = [x for x in p.iterdir() if x.is_file()]
        if not files:
            return None
        newest = max(files, key=lambda x: x.stat().st_mtime)
        return str(newest)
    # If it returned something like "/root/real/downloads/downloads" (file path)
    # and it doesn't exist yet, just return it as-is (caller will treat as failure).
    return download_result


# -------------------------
# V2 API Client (Primary)
# -------------------------
class V2ApiClient:
    def __init__(
        self,
        api_url: Optional[str] = None,
        api_key: Optional[str] = None,
        retries: int = 1,      # ✅ requested
        timeout: int = 30,
        download_dir: str = "downloads",
        job_poll_attempts: int = 8,
        job_poll_interval: float = 2.0,
        job_poll_backoff: float = 1.2,
        min_valid_size_bytes: int = 1024 * 5,
    ):
        self.api_url = (api_url or getattr(config, "API_URL", "") or "").rstrip("/")
        self.api_key = api_key or getattr(config, "API_KEY", "") or ""
        self.retries = retries
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.download_dir = Path(download_dir).resolve()
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
            logger.warning("[V2] API_URL not configured.")
            return None

        url = endpoint if endpoint.startswith(("http://", "https://")) else f"{self.api_url}/{endpoint.lstrip('/')}"
        params = dict(params or {})
        if self.api_key and "api_key" not in params:
            params["api_key"] = self.api_key

        last_exc = None
        for attempt in range(1, self.retries + 1):
            logger.info("[V2] REQUEST attempt=%s/%s url=%s params=%s", attempt, self.retries, url, params)
            try:
                async with aiohttp.ClientSession(timeout=self.timeout) as session:
                    async with session.get(url, headers=self._get_headers(), params=params) as resp:
                        text = await resp.text()
                        try:
                            data = await resp.json(content_type=None)
                        except Exception:
                            stripped = text.strip()
                            logger.warning("[V2] Non-JSON response url=%s body=%s", url, stripped[:300])
                            return stripped

                        logger.info("[V2] RESPONSE status=%s url=%s", resp.status, url)
                        if 200 <= resp.status < 300:
                            return data

                        logger.warning("[V2] API error status=%s data=%s", resp.status, str(data)[:300])
                        return data

            except asyncio.TimeoutError as e:
                last_exc = e
                logger.warning("[V2] Timeout attempt=%s/%s url=%s", attempt, self.retries, url)
            except aiohttp.ClientError as e:
                last_exc = e
                logger.warning("[V2] Network error attempt=%s/%s url=%s err=%s", attempt, self.retries, url, e)
            except Exception as e:
                last_exc = e
                logger.exception("[V2] Unexpected error attempt=%s/%s url=%s err=%s", attempt, self.retries, url, e)

            await asyncio.sleep(1)

        logger.warning("[V2] All retries exhausted url=%s last_err=%s", url, repr(last_exc))
        return None

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

        logger.info("[V2] youtube_v2_download normalized=%s isVideo=%s", query_to_send, isVideo)
        return await self._request_json("youtube/v2/download", params={"query": query_to_send, "isVideo": str(isVideo).lower()})

    async def youtube_v1_download(self, query: str, isVideo: bool = False) -> Optional[Any]:
        logger.info("[V2] youtube_v1_download compat isVideo=%s", isVideo)
        return await self._request_json("youtube/v1/download", params={"query": query, "isVideo": str(isVideo).lower()})

    async def youtube_job_status(self, job_id: str) -> Optional[Any]:
        logger.info("[V2] youtube_job_status job_id=%s", job_id)
        return await self._request_json("youtube/jobStatus", params={"job_id": job_id})

    def _extract_candidate_from_obj(self, obj: Any) -> Optional[str]:
        if obj is None:
            return None
        if isinstance(obj, str):
            s = obj.strip()
            return s if s else None
        if isinstance(obj, list) and obj:
            return self._extract_candidate_from_obj(obj[0])
        if isinstance(obj, dict):
            if "job" in obj and isinstance(obj["job"], dict):
                job = obj["job"]
                res = job.get("result")
                if isinstance(res, dict):
                    pub = res.get("public_url")
                    if isinstance(pub, str) and pub.strip():
                        return pub.strip()
                    for k in ("cdnurl", "download_url", "url", "file_path"):
                        v = res.get(k)
                        if isinstance(v, str) and v.strip():
                            return v.strip()

            for k in ("public_url", "cdnurl", "download_url", "url", "file_path", "file", "media", "tg_link", "telegram_link", "message_link"):
                v = obj.get(k)
                if isinstance(v, str) and v.strip():
                    return v.strip()

            for wrapper in ("result", "results", "data", "items", "tracks", "payload", "message"):
                w = obj.get(wrapper)
                if w:
                    cand = self._extract_candidate_from_obj(w)
                    if cand:
                        return cand
        return None

    def _looks_like_status_message(self, s: Optional[str]) -> bool:
        if not s or not isinstance(s, str):
            return False
        s_str = s.strip().lower()
        return any(ind in s_str for ind in (
            "download started", "background", "jobstatus", "job_id",
            "processing", "queued", "check status",
        ))

    def _normalize_candidate_to_url(self, candidate: str) -> Optional[str]:
        if not candidate:
            return None
        c = candidate.strip()
        if c.startswith(("http://", "https://")):
            return c
        if c.startswith("/"):
            if c.startswith("/root") or c.startswith("/home"):
                return None
            return f"{self.api_url.rstrip('/')}{c}"
        return f"{self.api_url.rstrip('/')}/{c.lstrip('/')}"

    async def _download_cdn_to_file(self, cdn_url: str, preferred_name: Optional[str] = None) -> Optional[str]:
        logger.info("[V2] CDN download start url=%s preferred=%s", cdn_url, preferred_name)
        for attempt in range(1, self.retries + 1):
            try:
                async with aiohttp.ClientSession(timeout=self.timeout) as session:
                    async with session.get(cdn_url) as resp:
                        logger.info("[V2] CDN response status=%s", resp.status)
                        if resp.status != 200:
                            return None

                        cd = resp.headers.get("Content-Disposition")
                        ct = resp.headers.get("Content-Type", "")
                        filename = preferred_name

                        if not filename and cd:
                            m = re.findall('filename="?([^";]+)"?', cd)
                            if m:
                                filename = m[0]

                        if not filename:
                            base = os.path.basename(cdn_url.split("?")[0]) or f"{uuid.uuid4().hex[:8]}"
                            if not os.path.splitext(base)[1]:
                                if "mp4" in ct.lower():
                                    base += ".mp4"
                                else:
                                    base += ".webm"
                            filename = base

                        save_path = self.download_dir / filename
                        logger.info("[V2] Writing file=%s", save_path)

                        with open(save_path, "wb") as f:
                            async for chunk in resp.content.iter_chunked(16 * 1024):
                                if chunk:
                                    f.write(chunk)

                        size = save_path.stat().st_size if save_path.exists() else 0
                        logger.info("[V2] Saved size=%s path=%s", size, save_path)
                        if size < self.min_valid_size_bytes:
                            try:
                                save_path.unlink(missing_ok=True)
                            except Exception:
                                pass
                            return None

                        return str(save_path)

            except Exception as e:
                logger.warning("[V2] CDN error attempt=%s/%s err=%s", attempt, self.retries, e)
                await asyncio.sleep(1)

        return None

    async def _download_telegram_media(self, tme_url: str) -> Optional[str]:
        logger.info("[V2] Telegram download start url=%s", tme_url)
        if not app:
            logger.warning("[V2] app is None; cannot download from Telegram.")
            return None

        match = _TG_RE.match(tme_url)
        if match:
            if match.group(1):
                channel_id = match.group(2)
                chat_id = int(f"-100{channel_id}")
                msg_id = int(tme_url.rstrip("/").split("/")[-1])
            else:
                chat_id = match.group(3)
                msg_id = int(match.group(4))
        else:
            parts = tme_url.rstrip("/").split("/")
            if len(parts) >= 2 and parts[-1].isdigit():
                chat_id = parts[-2]
                msg_id = int(parts[-1])
            else:
                logger.warning("[V2] Invalid Telegram URL: %s", tme_url)
                return None

        try:
            # ✅ FORCE DIRECTORY DOWNLOAD (fix for downloads.temp issue)
            dl_dir = _as_download_dir(self.download_dir)
            logger.info("[V2] msg.download -> dir=%s chat=%s msg=%s", dl_dir, chat_id, msg_id)
            msg = await app.get_messages(chat_id=chat_id, message_ids=int(msg_id))
            res = await msg.download(file_name=dl_dir)

            fixed = _resolve_if_dir(res)
            logger.info("[V2] Telegram download result=%s fixed=%s", res, fixed)

            if fixed and Path(fixed).exists():
                return fixed
            return None

        except Exception as e:
            if hasattr(pyrogram_errors, "FloodWait") and isinstance(e, getattr(pyrogram_errors, "FloodWait")):
                wait = getattr(e, "value", 5)
                logger.warning("[V2] FloodWait=%ss retrying...", wait)
                await asyncio.sleep(wait)
                return await self._download_telegram_media(tme_url)
            logger.warning("[V2] Telegram download error: %s", e)
            return None

    async def download_from_v2(self, query: str, isVideo: bool) -> Optional[str]:
        logger.info("[V2] START download_from_v2 query=%s isVideo=%s", query, isVideo)

        resp = await self.youtube_v2_download(query, isVideo=isVideo)
        if not resp:
            logger.warning("[V2] v2 empty -> trying v1 compat")
            resp = await self.youtube_v1_download(query, isVideo=isVideo)

        if resp is None:
            logger.warning("[V2] API returned None")
            return None

        candidate = self._extract_candidate_from_obj(resp)
        logger.info("[V2] candidate=%s", candidate)

        if candidate and self._looks_like_status_message(candidate):
            candidate = None

        job_id = None
        if isinstance(resp, dict):
            job_id = resp.get("job_id") or resp.get("job")
            if isinstance(job_id, dict) and "id" in job_id:
                job_id = job_id.get("id")

        if job_id and not candidate:
            logger.info("[V2] job_id=%s polling jobStatus", job_id)
            interval = self.job_poll_interval
            for i in range(1, self.job_poll_attempts + 1):
                await asyncio.sleep(interval)
                logger.info("[V2] jobStatus poll %s/%s", i, self.job_poll_attempts)
                status = await self.youtube_job_status(str(job_id))
                candidate = self._extract_candidate_from_obj(status) if status else None
                if candidate and self._looks_like_status_message(candidate):
                    candidate = None
                if candidate:
                    break
                interval *= self.job_poll_backoff

        if not candidate:
            logger.warning("[V2] no candidate after polling")
            return None

        if candidate.startswith(("http://t.me", "https://t.me")):
            return await self._download_telegram_media(candidate)

        normalized = self._normalize_candidate_to_url(candidate)
        logger.info("[V2] normalized=%s", normalized)
        if not normalized:
            return None

        preferred_name = None
        m = _YT_ID_RE.search(query) if isinstance(query, str) else None
        vid = (m.group(1) or m.group(2)) if m else None
        if not vid and isinstance(query, str) and "watch?v=" in query:
            vid = query.split("watch?v=")[-1].split("&")[0]
        if vid:
            preferred_name = f"{vid}.mp4" if isVideo else f"{vid}.webm"

        return await self._download_cdn_to_file(normalized, preferred_name=preferred_name)


# ----------------------------------------
# Fallen Track API Client (Audio fallback)
# ----------------------------------------
class FallenTrackFallback:
    def __init__(self, retries: int = 1, timeout: int = 15, download_dir: str = "downloads"):  # ✅ 1 retry
        self.api_url = (getattr(config, "API_URL2", "") or "").rstrip("/")
        self.api_key = getattr(config, "API_KEY2", "") or ""
        self.retries = retries
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.download_dir = Path(download_dir).resolve()
        self.download_dir.mkdir(parents=True, exist_ok=True)

    def _get_headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        return headers

    async def _get_track_obj(self, url: str) -> Optional[dict]:
        if not self.api_url:
            logger.warning("[FALLBACK] API_URL2 not configured")
            return None

        import urllib.parse
        endpoint = f"{self.api_url}/track?url={urllib.parse.quote(url)}"
        logger.info("[FALLBACK] REQUEST attempt=1/1 endpoint=%s", endpoint)

        try:
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.get(endpoint, headers=self._get_headers()) as resp:
                    data = await resp.json(content_type=None)
                    logger.info("[FALLBACK] RESPONSE status=%s data=%s", resp.status, str(data)[:300])
                    if resp.status == 200 and isinstance(data, dict):
                        return data
        except Exception as e:
            logger.warning("[FALLBACK] track request error: %s", e)

        return None

    async def _download_telegram_media(self, tme_url: str) -> Optional[str]:
        logger.info("[FALLBACK] Telegram download start url=%s", tme_url)
        if not app:
            logger.warning("[FALLBACK] app is None; cannot download from Telegram.")
            return None

        match = _TG_RE.match(tme_url)
        if match:
            if match.group(1):
                channel_id = match.group(2)
                chat_id = int(f"-100{channel_id}")
                msg_id = int(tme_url.rstrip("/").split("/")[-1])
            else:
                chat_id = match.group(3)
                msg_id = int(match.group(4))
        else:
            parts = tme_url.rstrip("/").split("/")
            if len(parts) >= 2 and parts[-1].isdigit():
                chat_id = parts[-2]
                msg_id = int(parts[-1])
            else:
                logger.warning("[FALLBACK] Invalid Telegram URL: %s", tme_url)
                return None

        try:
            # ✅ FORCE DIRECTORY DOWNLOAD (fix for downloads.temp issue)
            dl_dir = _as_download_dir(self.download_dir)
            logger.info("[FALLBACK] msg.download -> dir=%s chat=%s msg=%s", dl_dir, chat_id, msg_id)
            msg = await app.get_messages(chat_id=chat_id, message_ids=int(msg_id))
            res = await msg.download(file_name=dl_dir)

            fixed = _resolve_if_dir(res)
            logger.info("[FALLBACK] Telegram download result=%s fixed=%s", res, fixed)

            if fixed and Path(fixed).exists():
                logger.info("[FALLBACK] Telegram download complete path=%s", fixed)
                return fixed
            logger.warning("[FALLBACK] Telegram returned non-existing path=%s", fixed)
            return None

        except Exception as e:
            if hasattr(pyrogram_errors, "FloodWait") and isinstance(e, getattr(pyrogram_errors, "FloodWait")):
                wait = getattr(e, "value", 5)
                logger.warning("[FALLBACK] FloodWait=%ss retrying...", wait)
                await asyncio.sleep(wait)
                return await self._download_telegram_media(tme_url)
            logger.warning("[FALLBACK] Telegram download error: %s", e)
            return None

    async def _download_cdn(self, cdn_url: str) -> Optional[str]:
        logger.info("[FALLBACK] CDN download start url=%s", cdn_url)
        try:
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.get(cdn_url) as resp:
                    logger.info("[FALLBACK] CDN response status=%s", resp.status)
                    if resp.status != 200:
                        return None

                    cd = resp.headers.get("Content-Disposition")
                    filename = None
                    if cd:
                        m = re.findall('filename="?([^";]+)"?', cd)
                        if m:
                            filename = m[0]
                    if not filename:
                        base = os.path.basename(cdn_url.split("?")[0]) or f"{uuid.uuid4().hex[:8]}.mp3"
                        if "." not in base:
                            base += ".mp3"
                        filename = base

                    save_path = self.download_dir / filename
                    logger.info("[FALLBACK] Writing file=%s", save_path)
                    with open(save_path, "wb") as f:
                        async for chunk in resp.content.iter_chunked(16 * 1024):
                            if chunk:
                                f.write(chunk)

                    logger.info("[FALLBACK] CDN download complete path=%s", save_path)
                    return str(save_path)
        except Exception as e:
            logger.warning("[FALLBACK] CDN download error: %s", e)
            return None

    async def download_audio(self, url: str) -> Optional[str]:
        logger.info("[FALLBACK] START download_audio url=%s", url)

        track = await self._get_track_obj(url)
        if not track:
            logger.warning("[FALLBACK] no track object")
            return None

        dl_url = track.get("cdnurl") or track.get("url") or track.get("download_url")
        if not dl_url or not isinstance(dl_url, str):
            logger.warning("[FALLBACK] missing dl_url in track=%s", str(track)[:250])
            return None

        dl_url = dl_url.strip()
        logger.info("[FALLBACK] resolved_download_url=%s", dl_url)

        if dl_url.startswith(("http://t.me", "https://t.me")):
            return await self._download_telegram_media(dl_url)

        return await self._download_cdn(dl_url)


class FallenApi:
    def __init__(self, download_dir: str = "downloads", v2_timeout: int = 30, fallen_timeout: int = 15):
        self.v2 = V2ApiClient(
            api_url=getattr(config, "API_URL", None),
            api_key=getattr(config, "API_KEY", None),
            retries=1,  # ✅ V2=10
            timeout=v2_timeout,
            download_dir=download_dir,
        )
        self.fallen = FallenTrackFallback(
            retries=1,   # ✅ Fallen=1
            timeout=fallen_timeout,
            download_dir=download_dir,
        )

    async def download(self, query: str, isVideo: bool = False) -> Optional[str]:
        logger.info("[MAIN] START download query=%s isVideo=%s", query, isVideo)

        if isVideo:
            logger.info("[MAIN] Video -> V2 ONLY")
            return await self.v2.download_from_v2(query, isVideo=True)

        logger.info("[MAIN] Audio -> try V2 first")
        path = await self.v2.download_from_v2(query, isVideo=False)
        if path:
            logger.info("[MAIN] Audio downloaded via V2 path=%s", path)
            return path

        logger.warning("[MAIN] V2 audio failed -> trying FALLBACK (final)")
        fb = await self.fallen.download_audio(query)
        if fb:
            logger.info("[MAIN] Audio downloaded via FALLBACK path=%s", fb)
        else:
            logger.warning("[MAIN] FALLBACK also failed query=%s", query)
        return fb


client = FallenApi()
