# fallenapi.py
# FallenApi with improved download validation and optional ffmpeg conversion for playable audio.

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

# project imports (fallback stubs)
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
        timeout: int = 20,
        download_dir: str = "downloads",
        job_poll_attempts: int = 20,
        job_poll_interval: float = 2.0,
        min_valid_size_bytes: int = 1024 * 5,  # 5 KB
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
                            stripped = text.strip()
                            logger.debug("[FallenApi] Non-JSON response from %s: %s", url, stripped[:300])
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

    async def youtube_search(self, query: str) -> Optional[Any]:
        return await self._request_json("youtube/search", params={"query": query})

    async def youtube_v1_download(self, query: str, isVideo: bool = False) -> Optional[Any]:
        return await self._request_json("youtube/v1/download", params={"query": query, "isVideo": str(isVideo).lower()})

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

    async def spotify_get_name(self, link: str) -> Optional[Any]:
        return await self._request_json("spotify/name", params={"link": link})

    async def spotify_download(self, link: str, isVideo: bool = False) -> Optional[Any]:
        return await self._request_json("spotify/download", params={"link": link, "isVideo": str(isVideo).lower()})

    # -------------------------
    # Helpers for file validation and conversion
    # -------------------------
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
        # default
        return ".mp3"

    async def _convert_to_mp3(self, src_path: Path, dest_path: Path) -> bool:
        """
        Convert src_path to MP3 at dest_path using ffmpeg. Uses executor so it doesn't block event loop.
        Returns True on success.
        """
        if not self.ffmpeg_path:
            logger.warning("[FallenApi] ffmpeg not found; cannot convert to mp3.")
            return False

        loop = asyncio.get_event_loop()
        cmd = [self.ffmpeg_path, "-y", "-i", str(src_path), "-vn", "-acodec", "libmp3lame", "-q:a", "4", str(dest_path)]
        logger.info("[FallenApi] Converting to mp3: %s", " ".join(cmd))
        try:
            # run in thread pool
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
    # Download helpers
    # -------------------------
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
                            # attach extension guessed from content-type
                            ext = self._choose_extension_from_ct(content_type)
                            if not filename.endswith(ext):
                                filename = f"{filename}{ext}"

                        save_path = self.download_dir / filename

                        # stream-write
                        with open(save_path, "wb") as f:
                            async for chunk in resp.content.iter_chunked(16 * 1024):
                                if chunk:
                                    f.write(chunk)

                        # validate size
                        size = save_path.stat().st_size if save_path.exists() else 0
                        logger.info("[FallenApi] Download saved to %s (%s bytes). Content-Type=%s", save_path, size, content_type)

                        if size < self.min_valid_size_bytes:
                            logger.warning("[FallenApi] Downloaded file too small (%s bytes) -> deleting and retrying.", size)
                            try:
                                save_path.unlink(missing_ok=True)
                            except Exception:
                                pass
                            # if last attempt, fall-through to return None
                            continue

                        # If not an mp3 (or likely not playable), try to convert to mp3 for compatibility
                        ext = save_path.suffix.lower()
                        if ext not in (".mp3", ".m4a", ".aac", ".ogg", ".wav", ".flac"):
                            # create mp3 target path
                            dest = save_path.with_suffix(".mp3")
                            ok = await self._convert_to_mp3(save_path, dest)
                            if ok:
                                try:
                                    save_path.unlink(missing_ok=True)
                                except Exception:
                                    pass
                                return str(dest)
                            else:
                                # conversion failed; still return original if it's large enough
                                return str(save_path)

                        # if consumer expects .mp3, convert some containers if desired
                        if ext != ".mp3" and self.ffmpeg_path:
                            # prefer to provide mp3 for consistent playback, optional
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
            parts = tme_url.rstrip("/").split("/")
            if len(parts) >= 2 and parts[-1].isdigit():
                chat = parts[-2]
                msg_id = parts[-1]
            else:
                return None

        try:
            msg = await app.get_messages(chat_id=chat, message_ids=int(msg_id))
            file_path = await msg.download(file_name=str(self.download_dir))
            # validate size
            size = Path(file_path).stat().st_size if Path(file_path).exists() else 0
            if size < self.min_valid_size_bytes:
                logger.warning("[FallenApi] Telegram media too small (%s bytes).", size)
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

    # -------------------------
    # Response parsing + download flow
    # -------------------------
    def _extract_candidate_from_obj(self, obj: Any) -> Optional[str]:
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

    async def download_track(self, query: str, prefer_v2: bool = True, isVideo: bool = False) -> Optional[str]:
        logger.info("[FallenApi] Requesting download for %s (prefer_v2=%s)", query, prefer_v2)

        resp = None
        if prefer_v2:
            resp = await self.youtube_v2_download(query, isVideo=isVideo)
            if not resp:
                logger.info("[FallenApi] youtube_v2_download returned empty, falling back to v1")
                resp = await self.youtube_v1_download(query, isVideo=isVideo)
        else:
            resp = await self.youtube_v1_download(query, isVideo=isVideo)

        # debug: log raw response snapshot (first 500 chars for string, or keys for dict)
        try:
            if isinstance(resp, str):
                logger.debug("[FallenApi] Raw string response: %s", resp[:500])
            elif isinstance(resp, dict):
                logger.debug("[FallenApi] Response keys: %s", list(resp.keys()))
            elif isinstance(resp, list):
                logger.debug("[FallenApi] Response is list of length %s", len(resp))
        except Exception:
            pass

        if resp is None:
            logger.warning("[FallenApi] No response from API for download request.")
            return None

        dl_url = self._extract_candidate_from_obj(resp)
        job_id = resp.get("job_id") if isinstance(resp, dict) else None
        if not job_id:
            job_id = resp.get("job") if isinstance(resp, dict) else None

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

        # If it's a telegram link
        if isinstance(dl_url, str) and (dl_url.startswith("https://t.me/") or _TG_RE.match(dl_url)):
            tg_path = await self._download_telegram_media(dl_url)
            if tg_path:
                return tg_path

        # Otherwise try CDN download and validation/conversion
        file_path = await self._download_cdn_to_file(dl_url)
        if not file_path:
            logger.warning("[FallenApi] Failed to download file from %s", dl_url)
            return None

        # final sanity check
        try:
            p = Path(file_path)
            if not p.exists():
                logger.warning("[FallenApi] Download returned path that does not exist: %s", file_path)
                return None
            size = p.stat().st_size
            if size < self.min_valid_size_bytes:
                logger.warning("[FallenApi] Final downloaded file too small (%s bytes): %s", size, file_path)
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
