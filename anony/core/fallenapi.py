# fallenapi.py
# FallenApi client with robust youtube/v2 handling, jobStatus polling,
# public_url normalization, status-message detection, t.me + CDN downloads,
# file validation and optional ffmpeg conversion.

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

# Project imports expected in your codebase; fall back to stubs for standalone runs.
try:
    from anony import logger, config, app  # app must be your async pyrogram.Client
except Exception:
    import logging as _logging

    logger = getattr(_logging, "getLogger")("fallenapi")
    logger.setLevel(_logging.DEBUG)
    if not logger.handlers:
        logger.addHandler(_logging.StreamHandler())

    class _Cfg:
        API_URL = os.environ.get("API_URL", "")
        API_KEY = os.environ.get("API_KEY", "")
        SONGS_CHANNEL = os.environ.get("SONGS_CHANNEL", "")
        DB_CHANNEL = os.environ.get("DB_CHANNEL", "")
    config = _Cfg()
    app = None

try:
    from pyrogram import errors as pyrogram_errors
except Exception:
    class _PE: pass
    pyrogram_errors = _PE()

# Regexes
_YT_ID_RE = re.compile(r"""(?x)(?:v=|\/)([A-Za-z0-9_-]{11})|youtu\.be\/([A-Za-z0-9_-]{11})""")
_TG_RE = re.compile(r"https?://t\.me/(?:(c)/(\d+)|([^/]+)/(\d+))", re.IGNORECASE)

# Simple track model for convenience
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
        job_poll_attempts: int = 60,     # default increased to wait longer for background jobs
        job_poll_interval: float = 2.0,
        job_poll_backoff: float = 1.2,
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
        self.songs_channel = getattr(config, "SONGS_CHANNEL", None) or getattr(config, "DB_CHANNEL", None)

    def _get_headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        return headers

    async def _request_json(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[Any]:
        """
        HTTP GET with retries. Returns parsed JSON (dict/list/etc.) or raw string if server returns plain text.
        """
        if not self.api_url:
            logger.warning("[FallenApi] API_URL not configured.")
            return None

        url = endpoint if endpoint.startswith("http://") or endpoint.startswith("https://") else f"{self.api_url}/{endpoint.lstrip('/')}"
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
                            # not JSON — return raw text (often direct URL or status message)
                            stripped = text.strip()
                            logger.debug("[FallenApi] Non-JSON response from %s: %s", url, stripped[:400])
                            return stripped
                        if 200 <= resp.status < 300:
                            return data
                        else:
                            logger.warning("[FallenApi] API returned error (status %s) from %s: %s", resp.status, url, data)
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

    # ------------------
    # arc endpoints
    # ------------------
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

    # ------------------
    # utilities: candidate extraction & normalization
    # ------------------
    def _extract_candidate_from_obj(self, obj: Any) -> Optional[str]:
        """
        Extract a plausible candidate string from shapes returned by API:
        - direct string (URL or status message)
        - dict with keys: public_url, cdnurl, download_url, url, file_path, ...
        - nested wrappers: job.result.public_url, result, data, results, items, tracks
        """
        if obj is None:
            return None

        if isinstance(obj, str):
            s = obj.strip()
            return s if s else None

        if isinstance(obj, list) and obj:
            return self._extract_candidate_from_obj(obj[0])

        if isinstance(obj, dict):
            # handle arcapi job wrapper
            if "job" in obj and isinstance(obj["job"], dict):
                job = obj["job"]
                res = job.get("result")
                if isinstance(res, dict):
                    # public_url preferred
                    pub = res.get("public_url")
                    if isinstance(pub, str) and pub.strip():
                        return pub.strip()
                    # fallback: cdnurl / download_url / url
                    for k in ("cdnurl", "download_url", "url", "file_path"):
                        v = res.get(k)
                        if isinstance(v, str) and v.strip():
                            return v.strip()
            # direct keys at top level
            for k in ("public_url", "cdnurl", "download_url", "url", "file_path", "file", "media", "tg_link", "telegram_link", "message_link"):
                v = obj.get(k)
                if isinstance(v, str) and v.strip():
                    return v.strip()
            # nested wrappers
            for wrapper in ("result", "results", "data", "items", "tracks", "payload", "message"):
                w = obj.get(wrapper)
                if w:
                    cand = self._extract_candidate_from_obj(w)
                    if cand:
                        return cand
        return None

    def _looks_like_status_message(self, s: Optional[str]) -> bool:
        """
        Basic heuristics to detect non-downloadable status messages from the API,
        e.g. "Download started in background. Use /youtube/jobStatus?job_id=..."
        """
        if not s or not isinstance(s, str):
            return False
        s_str = s.strip().lower()
        indicators = [
            "download started", "background", "use /youtube/jobstatus", "job_id",
            "in background", "processing", "queued", "started in background",
            "check status", "use /youtube/jobstatus"
        ]
        if any(ind in s_str for ind in indicators):
            return True
        # long plain text without URL-like chars -> status
        if " " in s_str and "/" not in s_str and "." not in s_str and len(s_str) > 30:
            return True
        return False

    def _normalize_candidate_to_url(self, candidate: str) -> Optional[str]:
        """
        Convert candidate into a downloadable HTTP/HTTPS URL if possible.
        - If candidate already starts with http(s) -> return as-is
        - If candidate starts with '/', treat as relative path served by API host -> prefix api_url
        - If candidate looks like an absolute server filepath (/root/...), return None (not downloadable)
        - If candidate looks like bare filename (xxx.mp4) -> map to /media/filename on API host
        - Otherwise prefix with API host
        """
        if not candidate:
            return None
        c = candidate.strip()
        if c.startswith("http://") or c.startswith("https://"):
            return c
        if c.startswith("/"):
            # if it's server local path under root (e.g. /root/... ) -> not downloadable
            if c.startswith("/root") or c.startswith("/home"):
                return None
            # otherwise assume relative public_url -> prefix with API host
            return f"{self.api_url.rstrip('/')}{c}"
        # bare filename like gJLVTKhTnog.mp4 -> try /media/<name>
        if re.match(r"^[\w\-\._ ]+\.(mp3|mp4|webm|m4a|ogg|wav|flac)$", c, re.IGNORECASE):
            return f"{self.api_url.rstrip('/')}/media/{c.lstrip('/')}"
        # fallback: prefix with API host
        return f"{self.api_url.rstrip('/')}/{c.lstrip('/')}"

    # ------------------
    # download helpers (CDN + Telegram) + conversion
    # ------------------
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

    async def _convert_to_mp3(self, src_path: Path, dest_path: Path) -> bool:
        if not self.ffmpeg_path:
            logger.warning("[FallenApi] ffmpeg not found; skipping conversion.")
            return False
        loop = asyncio.get_event_loop()
        cmd = [self.ffmpeg_path, "-y", "-i", str(src_path), "-vn", "-acodec", "libmp3lame", "-q:a", "4", str(dest_path)]
        logger.info("[FallenApi] Converting to mp3: %s", " ".join(cmd))
        try:
            res = await loop.run_in_executor(None, lambda: subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE))
            if res.returncode == 0:
                logger.info("[FallenApi] ffmpeg conversion succeeded: %s", dest_path)
                return True
            else:
                logger.warning("[FallenApi] ffmpeg conversion failed (code %s). stderr: %s", res.returncode, res.stderr.decode()[:500])
                return False
        except Exception as e:
            logger.exception("[FallenApi] Exception while converting with ffmpeg: %s", e)
            return False

    async def _download_cdn_to_file(self, cdn_url: str, preferred_name: Optional[str] = None) -> Optional[str]:
        """
        Download cdn_url to local filesystem. Uses preferred_name if provided.
        """
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
                        # Convert to mp3 optionally for non-audio containers (best-effort)
                        ext = save_path.suffix.lower()
                        accepted = (".mp3", ".m4a", ".aac", ".ogg", ".wav", ".flac", ".webm", ".mp4")
                        if ext not in accepted:
                            dest = save_path.with_suffix(".mp3")
                            ok = await self._convert_to_mp3(save_path, dest)
                            if ok:
                                try:
                                    save_path.unlink(missing_ok=True)
                                except Exception:
                                    pass
                                return str(dest)
                            else:
                                return str(save_path)
                        # optionally convert some containers to mp3 for consistent playback
                        if ext != ".mp3" and self.ffmpeg_path and ext in (".m4a", ".aac", ".ogg", ".wav", ".flac"):
                            dest = save_path.with_suffix(".mp3")
                            converted = await self._convert_to_mp3(save_path, dest)
                            if converted:
                                try:
                                    save_path.unlink(missing_ok=True)
                                except Exception:
                                    pass
                                return str(dest)
                        return str(save_path)
            except asyncio.TimeoutError:
                logger.warning("[FallenApi] CDN download timeout attempt %s/%s for %s", attempt, self.retries, cdn_url)
            except aiohttp.ClientError as e:
                logger.warning("[FallenApi] CDN network error attempt %s/%s for %s: %s", attempt, self.retries, cdn_url, e)
            except Exception as e:
                logger.exception("[FallenApi] Unexpected error when downloading CDN: %s", e)
            await asyncio.sleep(1)
        logger.warning("[FallenApi] CDN download attempts exhausted for %s", cdn_url)
        return None

    async def _download_telegram_media(self, tme_url: str) -> Optional[str]:
        """
        Download a t.me message using pyrogram.
        Supports username/ID forms and the /c/<channel_id>/<msg_id> internal links.
        """
        if not app:
            logger.warning("[FallenApi] Pyrogram app not available; cannot download t.me media.")
            return None

        match = _TG_RE.match(tme_url)
        if match:
            if match.group(1):  # matched c/<id>/<msg>
                _, channel_id, _, _ = match.groups()
                chat_id = int(f"-100{channel_id}")
                # msg id might be in group 4 when matched differently; parse fallback below
                # extract message id from path fallback
                parts = tme_url.rstrip("/").split("/")
                msg_id = int(parts[-1]) if parts[-1].isdigit() else None
            else:
                username = match.group(3)
                msg_id = int(match.group(4))
                chat_id = username
        else:
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

    # ------------------
    # high-level v2 flow
    # ------------------
    async def download_track_v2(self, query: str, isVideo: bool = False, prefer_v2: bool = True) -> Optional[str]:
        """
        1) request /youtube/v2/download (prefers v2)
        2) if job_id or status message, poll /youtube/jobStatus until candidate appears
        3) normalize candidate to HTTP URL or t.me link
        4) download (CDN or Telegram), validate, convert if needed
        Returns local file path or None.
        """
        logger.info("[FallenApi] Requesting download for %s (prefer_v2=%s)", query, prefer_v2)

        resp = None
        if prefer_v2:
            resp = await self.youtube_v2_download(query, isVideo=isVideo)
            if not resp:
                logger.debug("[FallenApi] v2 returned empty, falling back to v1")
                resp = await self.youtube_v1_download(query, isVideo=isVideo)
        else:
            resp = await self.youtube_v1_download(query, isVideo=isVideo)

        if resp is None:
            logger.warning("[FallenApi] No response from API for download request.")
            return None

        # debug snapshot
        try:
            if isinstance(resp, str):
                logger.debug("[FallenApi] v2 raw string resp: %s", resp[:300])
            elif isinstance(resp, dict):
                logger.debug("[FallenApi] v2 resp keys: %s", list(resp.keys()))
            elif isinstance(resp, list):
                logger.debug("[FallenApi] v2 resp is list (len=%s)", len(resp))
        except Exception:
            pass

        # 1) try to extract candidate from the immediate response
        candidate = self._extract_candidate_from_obj(resp)

        # If candidate is a status message, ignore it and force polling jobStatus
        if candidate and self._looks_like_status_message(candidate):
            logger.info("[FallenApi] Candidate looks like a status message -> ignoring: %s", candidate[:200])
            candidate = None

        # 2) if there's a job id and no downloadable candidate yet, poll jobStatus until we find one
        job_id = None
        if isinstance(resp, dict):
            job_id = resp.get("job_id") or resp.get("job")
            # sometimes job is inside top-level job wrapper
            if isinstance(job_id, dict) and "id" in job_id:
                job_id = job_id.get("id")

        if job_id and not candidate:
            logger.info("[FallenApi] Received job_id %s, polling jobStatus up to %s attempts.", job_id, self.job_poll_attempts)
            interval = self.job_poll_interval
            for attempt in range(1, self.job_poll_attempts + 1):
                await asyncio.sleep(interval)
                status = await self.youtube_job_status(str(job_id))
                if not status:
                    logger.debug("[FallenApi] jobStatus empty on attempt %s/%s", attempt, self.job_poll_attempts)
                    interval *= self.job_poll_backoff
                    continue
                # prefer job.result.public_url and other nested keys
                candidate = self._extract_candidate_from_obj(status)
                if candidate and self._looks_like_status_message(candidate):
                    logger.info("[FallenApi] jobStatus returned status message -> continue polling: %s", candidate[:200])
                    candidate = None
                if candidate:
                    logger.info("[FallenApi] jobStatus returned candidate on attempt %s: %s", attempt, str(candidate)[:200])
                    break
                interval *= self.job_poll_backoff
            if not candidate:
                logger.warning("[FallenApi] job did not complete within polling window (job_id=%s).", job_id)
                return None

        # If still no candidate, try a last attempt at extracting from resp
        if not candidate:
            candidate = self._extract_candidate_from_obj(resp)
            if candidate and self._looks_like_status_message(candidate):
                candidate = None

        if not candidate:
            logger.warning("[FallenApi] No downloadable candidate found in v2 response.")
            return None

        # 3) Normalize candidate to URL or handle t.me link
        normalized = self._normalize_candidate_to_url(candidate)
        # If normalization returned None but the candidate is a t.me link, attempt a Telegram download
        if not normalized and (candidate.startswith("http://t.me") or candidate.startswith("https://t.me")):
            logger.info("[FallenApi] Candidate is t.me link, attempting Telegram download: %s", candidate)
            return await self._download_telegram_media(candidate)
        if not normalized:
            # Candidate might be a server filepath (like /root/...) or invalid status; fail
            logger.warning("[FallenApi] Candidate could not be normalized to URL: %s", candidate)
            return None

        # 4) prepare a preferred filename if query is a YouTube id/url (keeps YouTube.download expectations)
        preferred_name = None
        if isinstance(query, str) and ("youtube." in query or "youtu.be" in query):
            m = _YT_ID_RE.search(query)
            vid = (m.group(1) or m.group(2)) if m else None
            if not vid and "watch?v=" in query:
                vid = query.split("watch?v=")[-1].split("&")[0]
            if vid:
                preferred_name = f"{vid}.mp4" if isVideo else f"{vid}.webm"

        # 5) download normalized URL
        file_path = await self._download_cdn_to_file(normalized, preferred_name=preferred_name)
        if not file_path:
            logger.warning("[FallenApi] Failed to download file from normalized url: %s", normalized)
            return None

        # validate size
        try:
            p = Path(file_path)
            if not p.exists():
                logger.warning("[FallenApi] Downloaded path does not exist: %s", file_path)
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

    # convenience wrapper: download and send as audio (if app available)
    async def download_and_send_v2(self, query: str, chat_id: int, caption: Optional[str] = None, isVideo: bool = False):
        if not app:
            logger.warning("[FallenApi] Pyrogram app not available; cannot send audio.")
            return None
        local_path = await self.download_track_v2(query, isVideo=isVideo, prefer_v2=True)
        if not local_path:
            try:
                await app.send_message(chat_id, f"Failed to download: {query}")
            except Exception:
                pass
            return None
        try:
            msg = await app.send_audio(chat_id=chat_id, audio=local_path, caption=caption or "")
            logger.info("[FallenApi] Sent audio to chat %s message_id=%s", chat_id, getattr(msg, "message_id", None))
            return msg
        except Exception as e:
            logger.exception("[FallenApi] send_audio failed, trying send_document: %s", e)
            try:
                msg = await app.send_document(chat_id=chat_id, document=local_path, caption=caption or "")
                return msg
            except Exception as e2:
                logger.exception("[FallenApi] send_document also failed: %s", e2)
                return None


# module-level client for convenience
client = FallenApi()
