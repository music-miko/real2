# fallenapi.py
# Full FallenApi client:
#  - requests /youtube/v2/download (with search-term or URL)
#  - polls /youtube/jobStatus when needed
#  - downloads CDN or t.me files, validates them, converts to mp3 if needed
#  - sends audio to Telegram chat via Pyrogram (app.send_audio)
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

# project imports - expected in your codebase
try:
    from anony import logger, config, app  # app must be your async pyrogram.Client
except Exception:
    # lightweight fallback logger + fake config when running standalone
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

# Regex helpers
_YT_ID_RE = re.compile(
    r"""(?x)
    (?:v=|\/)([A-Za-z0-9_-]{11})
    |youtu\.be\/([A-Za-z0-9_-]{11})
    """
)
_TG_RE = re.compile(r"https?://t\.me/([^/]+)/(\d+)", re.IGNORECASE)

# Minimal pydantic model for track metadata
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
        """GET with retries. Returns parsed JSON, list, or raw string."""
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
                            # fallback: raw text (sometimes direct URL)
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
        """/youtube/v2/download - supports search term or youtube url/id"""
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
        """/youtube/jobStatus"""
        return await self._request_json("youtube/jobStatus", params={"job_id": job_id})

    # optional v1/search/spotify wrappers if you need them
    async def youtube_v1_download(self, query: str, isVideo: bool = False) -> Optional[Any]:
        return await self._request_json("youtube/v1/download", params={"query": query, "isVideo": str(isVideo).lower()})

    async def youtube_search(self, query: str) -> Optional[Any]:
        return await self._request_json("youtube/search", params={"query": query})

    async def spotify_get_name(self, link: str) -> Optional[Any]:
        return await self._request_json("spotify/name", params={"link": link})

    async def spotify_download(self, link: str, isVideo: bool = False) -> Optional[Any]:
        return await self._request_json("spotify/download", params={"link": link, "isVideo": str(isVideo).lower()})

    # --- response parsing ---
    def _extract_candidate_from_obj(self, obj: Any) -> Optional[str]:
        """Given API response (str/list/dict) return first plausible cdn/download url or None."""
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

    # --- download helpers ---
    def _choose_extension_from_ct(self, content_type: Optional[str]) -> str:
        if not content_type:
            return ".mp3"
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
        return ".mp3"

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
                logger.warning("[FallenApi] ffmpeg conversion failed (code %s): %s", res.returncode, res.stderr.decode()[:500])
                return False
        except Exception as e:
            logger.exception("[FallenApi] Exception while running ffmpeg: %s", e)
            return False

    async def _download_cdn_to_file(self, cdn_url: str) -> Optional[str]:
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
                        if cd:
                            match = re.findall('filename="?([^";]+)"?', cd)
                            if match:
                                filename = match[0]
                        if not filename:
                            filename = os.path.basename(cdn_url.split("?")[0]) or f"{uuid.uuid4().hex[:8]}"
                            ext = self._choose_extension_from_ct(content_type)
                            if not filename.endswith(ext):
                                filename = f"{filename}{ext}"
                        save_path = self.download_dir / filename
                        with open(save_path, "wb") as f:
                            async for chunk in resp.content.iter_chunked(16 * 1024):
                                if chunk:
                                    f.write(chunk)
                        size = save_path.stat().st_size if save_path.exists() else 0
                        logger.info("[FallenApi] Download saved to %s (%s bytes). Content-Type=%s", save_path, size, content_type)
                        if size < self.min_valid_size_bytes:
                            logger.warning("[FallenApi] Downloaded file too small (%s bytes). Deleting and retrying.", size)
                            try:
                                save_path.unlink(missing_ok=True)
                            except Exception:
                                pass
                            continue
                        ext = save_path.suffix.lower()
                        # if not common audio ext, try to convert to mp3
                        if ext not in (".mp3", ".m4a", ".aac", ".ogg", ".wav", ".flac"):
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
                        # convert to mp3 optionally if not mp3 and ffmpeg present for consistent playback
                        if ext != ".mp3" and self.ffmpeg_path:
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
                logger.warning("[FallenApi] Telegram media small (%s bytes).", size)
            return file_path
        except Exception as e:
            if hasattr(pyrogram_errors, "FloodWait") and isinstance(e, getattr(pyrogram_errors, "FloodWait")):
                wait = getattr(e, "value", 5)
                logger.warning("[FallenApi] FloodWait %s - sleeping then retrying.", wait)
                await asyncio.sleep(wait)
                return await self._download_telegram_media(tme_url)
            logger.warning("[FallenApi] Error downloading telegram media %s: %s", tme_url, e)
            return None

    # --- High-level: request v2 -> poll jobStatus -> download file path ---
    async def download_track_v2(self, query: str, isVideo: bool = False, prefer_v2: bool = True) -> Optional[str]:
        """
        Request v2 (search-term or URL). If v2 returns job_id, poll jobStatus until cdnurl found.
        Return local downloaded file path (converted if needed) or None.
        """
        logger.info("[FallenApi] download_track_v2: requesting %s (prefer_v2=%s)", query, prefer_v2)
        resp = None
        if prefer_v2:
            resp = await self.youtube_v2_download(query, isVideo=isVideo)
            # fallback to v1 if v2 gave nothing
            if not resp:
                logger.info("[FallenApi] v2 returned empty; falling back to v1")
                resp = await self.youtube_v1_download(query, isVideo=isVideo)
        else:
            resp = await self.youtube_v1_download(query, isVideo=isVideo)

        if resp is None:
            logger.warning("[FallenApi] No response from API for download request.")
            return None

        # debug log short snapshot
        try:
            if isinstance(resp, str):
                logger.debug("[FallenApi] v2 raw string response: %s", resp[:500])
            elif isinstance(resp, dict):
                logger.debug("[FallenApi] v2 response keys: %s", list(resp.keys()))
            elif isinstance(resp, list):
                logger.debug("[FallenApi] v2 response list length: %s", len(resp))
        except Exception:
            pass

        dl_url = self._extract_candidate_from_obj(resp)
        job_id = None
        if isinstance(resp, dict):
            job_id = resp.get("job_id") or resp.get("job")

        # Poll job status when job_id present and dl_url not yet available
        if job_id and not dl_url:
            logger.info("[FallenApi] v2 returned job_id %s - polling jobStatus", job_id)
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
                    logger.info("[FallenApi] job completed on attempt %s/%s - url found.", attempt, self.job_poll_attempts)
                    break
                else:
                    logger.debug("[FallenApi] jobStatus attempt %s/%s: still processing.", attempt, self.job_poll_attempts)
            if not dl_url:
                logger.warning("[FallenApi] job didn't finish within polling window (job_id=%s).", job_id)
                return None

        if not dl_url:
            logger.warning("[FallenApi] No downloadable url found in v2 response.")
            return None

        # If telegram link, download via app
        if isinstance(dl_url, str) and (dl_url.startswith("https://t.me/") or _TG_RE.match(dl_url)):
            tg_path = await self._download_telegram_media(dl_url)
            if tg_path:
                return tg_path

        # Download CDN file
        file_path = await self._download_cdn_to_file(dl_url)
        if not file_path:
            logger.warning("[FallenApi] Failed to download file from %s", dl_url)
            return None

        # Validate file size again
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

        logger.info("[FallenApi] download_track_v2 completed -> %s", file_path)
        return file_path

    # --- Convenience: download with v2 then send to a Telegram chat (plays) ---
    async def download_and_send_v2(self, query: str, chat_id: int, caption: Optional[str] = None, isVideo: bool = False):
        """
        Performs the full flow:
          - Request /youtube/v2/download?q=query
          - Poll jobStatus if necessary
          - Download the file
          - Send audio to `chat_id` (via app.send_audio)
        Returns message object from send_audio on success, or None on failure.
        """
        if not app:
            logger.warning("[FallenApi] Pyrogram app not available; cannot send audio.")
            return None

        # 1) download track (v2 preferred)
        local_path = await self.download_track_v2(query, isVideo=isVideo, prefer_v2=True)
        if not local_path:
            logger.warning("[FallenApi] download_and_send_v2: failed to obtain audio for query=%s", query)
            try:
                await app.send_message(chat_id, f"Failed to download: {query}")
            except Exception:
                pass
            return None

        # 2) send as audio to Telegram (send_audio to enable inline player)
        try:
            # optionally extract title/artist from the last API resp if you saved it; here we just use caption if provided
            logger.info("[FallenApi] Sending audio to chat %s -> %s", chat_id, local_path)
            msg = await app.send_audio(chat_id=chat_id, audio=local_path, caption=caption or "")
            logger.info("[FallenApi] Sent audio message id=%s", getattr(msg, "message_id", None))
            return msg
        except Exception as e:
            logger.exception("[FallenApi] Failed to send_audio: %s", e)
            try:
                # fallback: send as document if audio send fails
                msg = await app.send_document(chat_id=chat_id, document=local_path, caption=caption or "")
                return msg
            except Exception as e2:
                logger.exception("[FallenApi] Also failed to send_document: %s", e2)
                return None

# module-level client
client = FallenApi()
