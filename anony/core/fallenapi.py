# fallenapi.py
# FallenApi client tuned to integrate with your YouTube class:
# - Prefer youtube/v2/download
# - Poll youtube/jobStatus when job_id present
# - Save files as downloads/{video_id}.webm for audio (or .mp4 for video) when input is a YouTube url/id
# - Validate downloaded file and optionally convert to mp3 with ffmpeg
# - Robust parsing of many API response shapes (strings, lists, dicts)
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

# Regex to extract YouTube id
_YT_ID_RE = re.compile(
    r"""(?x)
    (?:v=|\/)([A-Za-z0-9_-]{11})
    |youtu\.be\/([A-Za-z0-9_-]{11})
    """
)
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
        timeout: int = 30,
        download_dir: str = "downloads",
        job_poll_attempts: int = 20,
        job_poll_interval: float = 2.0,
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
        self.min_valid_size_bytes = min_valid_size_bytes
        self.ffmpeg_path = shutil.which("ffmpeg")

    def _get_headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        return headers

    async def _request_json(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[Any]:
        """
        Generic GET with retries. Returns parsed JSON or raw string when server returns plain text.
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
                        try:
                            data = await resp.json(content_type=None)
                        except Exception:
                            # not JSON — often a direct URL string; return raw text
                            stripped = text.strip()
                            logger.debug("[FallenApi] Non-JSON response from %s: %s", url, stripped[:300])
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

    # --- arc endpoints ---
    async def youtube_v2_download(self, query: str, isVideo: bool = False) -> Optional[Any]:
        """
        Sends query (search term or video id). If a YouTube URL is passed, we extract the id.
        """
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

    async def youtube_search(self, query: str) -> Optional[Any]:
        return await self._request_json("youtube/search", params={"query": query})

    # -------------------------
    # Response parsing helpers
    # -------------------------
    def _extract_candidate_from_obj(self, obj: Any) -> Optional[str]:
        """
        Try to extract a direct download URL (cdnurl/download_url/url) from response shapes.
        """
        if obj is None:
            return None
        if isinstance(obj, str):
            s = obj.strip()
            return s if s else None
        if isinstance(obj, list) and obj:
            return self._extract_candidate_from_obj(obj[0])
        if isinstance(obj, dict):
            for k in ("cdnurl", "download_url", "url", "file", "media"):
                v = obj.get(k)
                if isinstance(v, str) and v.strip():
                    return v.strip()
            for wrapper in ("results", "data", "result", "items", "tracks"):
                w = obj.get(wrapper)
                if w:
                    cand = self._extract_candidate_from_obj(w)
                    if cand:
                        return cand
        return None

    # -------------------------
    # file naming / conversion helpers
    # -------------------------
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
        # default to webm for audio from youtube
        return ".webm"

    async def _convert_to_mp3(self, src_path: Path, dest_path: Path) -> bool:
        if not self.ffmpeg_path:
            logger.warning("[FallenApi] ffmpeg not found; cannot convert to mp3.")
            return False
        loop = asyncio.get_event_loop()
        cmd = [self.ffmpeg_path, "-y", "-i", str(src_path), "-vn", "-acodec", "libmp3lame", "-q:a", "4", str(dest_path)]
        logger.info("[FallenApi] Converting to mp3: %s", " ".join(cmd))
        try:
            res = await loop.run_in_executor(None, lambda: subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE))
            if res.returncode == 0:
                logger.info("[FallenApi] Conversion succeeded: %s", dest_path)
                return True
            else:
                logger.warning("[FallenApi] ffmpeg failed (code %s). stderr: %s", res.returncode, res.stderr.decode()[:500])
                return False
        except Exception as e:
            logger.exception("[FallenApi] Exception while converting with ffmpeg: %s", e)
            return False

    # -------------------------
    # download helpers (preferred_name support)
    # -------------------------
    async def _download_cdn_to_file(self, cdn_url: str, preferred_name: Optional[str] = None) -> Optional[str]:
        """
        Download cdn_url into download_dir. If preferred_name provided, use it (overwrites if exists=False).
        Returns local path or None.
        """
        for attempt in range(1, self.retries + 1):
            try:
                async with aiohttp.ClientSession(timeout=self.timeout) as session:
                    async with session.get(cdn_url) as resp:
                        if resp.status != 200:
                            logger.warning("[FallenApi] CDN returned %s for %s", resp.status, cdn_url)
                            return None

                        cd = resp.headers.get("Content-Disposition")
                        content_type = resp.headers.get("Content-Type", "")
                        filename = None
                        if preferred_name:
                            filename = preferred_name
                        elif cd:
                            match = re.findall('filename="?([^";]+)"?', cd)
                            if match:
                                filename = match[0]

                        if not filename:
                            # fallback: basename or uuid + extension guessed from content-type
                            base = os.path.basename(cdn_url.split("?")[0]) or f"{uuid.uuid4().hex[:8]}"
                            ext = self._choose_extension_from_ct(content_type)
                            filename = base if os.path.splitext(base)[1] else f"{base}{ext}"

                        save_path = self.download_dir / filename

                        # write stream
                        with open(save_path, "wb") as f:
                            async for chunk in resp.content.iter_chunked(16 * 1024):
                                if chunk:
                                    f.write(chunk)

                        size = save_path.stat().st_size if save_path.exists() else 0
                        logger.info("[FallenApi] Download saved to %s (%s bytes). Content-Type=%s", save_path, size, content_type)

                        if size < self.min_valid_size_bytes:
                            logger.warning("[FallenApi] Downloaded file too small (%s bytes) -> deleting and retrying.", size)
                            try:
                                save_path.unlink(missing_ok=True)
                            except Exception:
                                pass
                            continue

                        # If extension is not an expected audio/video container, try to convert to mp3
                        ext = save_path.suffix.lower()
                        # common audio/video suffixes accepted
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

                        # Optionally convert non-mp3 audio to mp3 for uniform playback (not forced)
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
                last = getattr(e, "errno", e)
                logger.warning("[FallenApi] CDN network error attempt %s/%s for %s: %s", attempt, self.retries, cdn_url, last)
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
            parts = tme_url.rstrip("/").split("/")
            if len(parts) >= 2 and parts[-1].isdigit():
                chat = parts[-2]
                msg_id = parts[-1]
            else:
                return None

        try:
            msg = await app.get_messages(chat_id=chat, message_ids=int(msg_id))
            file_path = await msg.download(file_name=str(self.download_dir))
            size = Path(file_path).stat().st_size if Path(file_path).exists() else 0
            logger.info("[FallenApi] Telegram media downloaded: %s (%s bytes)", file_path, size)
            if size < self.min_valid_size_bytes:
                logger.warning("[FallenApi] Telegram media too small (%s bytes).", size)
            return file_path
        except Exception as e:
            if hasattr(pyrogram_errors, "FloodWait") and isinstance(e, getattr(pyrogram_errors, "FloodWait")):
                wait = getattr(e, "value", 5)
                logger.warning("[FallenApi] FloodWait %s - sleeping then retrying.", wait)
                await asyncio.sleep(wait)
                return await self._download_telegram_media(tme_url)
            logger.warning("[FallenApi] Error downloading telegram media %s: %s", tme_url, e)
            return None

    # -------------------------
    # High level: request v2 -> poll -> download
    # -------------------------
    async def download_track(self, query: str, isVideo: bool = False, prefer_v2: bool = True) -> Optional[str]:
        """
        Returns a local file path for the requested track.
        If query is a YouTube URL/id, attempt to save file as downloads/{video_id}.{ext} to match YouTube.download's expectations.
        """
        logger.info("[FallenApi] Requesting download for %s (prefer_v2=%s)", query, prefer_v2)

        # derive preferred filename when query is a youtube id/url
        preferred_name = None
        if isinstance(query, str) and ("youtube." in query or "youtu.be" in query):
            m = _YT_ID_RE.search(query)
            vid = (m.group(1) or m.group(2)) if m else None
            if not vid and "watch?v=" in query:
                vid = query.split("watch?v=")[-1].split("&")[0]
            if vid:
                ext = ".mp4" if isVideo else ".webm"
                preferred_name = f"{vid}{ext}"

        # 1) try v2 first
        resp = None
        if prefer_v2:
            resp = await self.youtube_v2_download(query, isVideo=isVideo)
            if not resp:
                logger.debug("[FallenApi] v2 returned empty, trying v1 fallback")
                resp = await self.youtube_v1_download(query, isVideo=isVideo)
        else:
            resp = await self.youtube_v1_download(query, isVideo=isVideo)

        if resp is None:
            logger.warning("[FallenApi] No response for download request.")
            return None

        # debug snapshots
        try:
            if isinstance(resp, str):
                logger.debug("[FallenApi] Raw v2 response (string): %s", resp[:400])
            elif isinstance(resp, dict):
                logger.debug("[FallenApi] v2 response keys: %s", list(resp.keys()))
            elif isinstance(resp, list):
                logger.debug("[FallenApi] v2 response list len: %s", len(resp))
        except Exception:
            pass

        dl_url = self._extract_candidate_from_obj(resp)
        job_id = None
        if isinstance(resp, dict):
            job_id = resp.get("job_id") or resp.get("job")

        # Poll if job_id present and no dl_url yet
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
                    logger.info("[FallenApi] Job completed on attempt %s/%s - url found.", attempt, self.job_poll_attempts)
                    break
                else:
                    logger.debug("[FallenApi] jobStatus attempt %s/%s: still processing.", attempt, self.job_poll_attempts)
            if not dl_url:
                logger.warning("[FallenApi] job did not complete within polling window (job_id=%s).", job_id)
                return None

        if not dl_url:
            logger.warning("[FallenApi] No downloadable URL found in API response.")
            return None

        # If it's a telegram link, use pyrogram downloader
        if isinstance(dl_url, str) and (dl_url.startswith("https://t.me/") or _TG_RE.match(dl_url)):
            tg_path = await self._download_telegram_media(dl_url)
            return tg_path

        # Otherwise download CDN; pass preferred_name so file is saved as {video_id}.webm when possible
        file_path = await self._download_cdn_to_file(dl_url, preferred_name=preferred_name)
        if not file_path:
            logger.warning("[FallenApi] Failed to download CDN file from %s", dl_url)
            return None

        # final validation
        try:
            p = Path(file_path)
            if not p.exists():
                logger.warning("[FallenApi] Downloaded file path does not exist: %s", file_path)
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

        logger.info("[FallenApi] Returning downloaded file: %s", file_path)
        return file_path

# module-level client
client = FallenApi()
