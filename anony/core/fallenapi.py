# fallenapi.py
"""
FallenApi - v2-only arcapi client
Uses only:
  - /youtube/v2/download
  - /youtube/jobStatus
  - /search  (fallback to /youtube/search)
Downloads returned audio/video URLs into ./downloads/

Requires:
  - anony.config.API_URL
  - anony.config.API_KEY
  - aiohttp
  - (optional) pyrogram client available as anony.app to download t.me links
"""
import asyncio
import os
import re
import uuid
import mimetypes
import urllib.parse
from pathlib import Path
from typing import Dict, Optional, Any

import aiohttp

# optional pyrogram support for t.me downloads
try:
    from pyrogram import errors
    from anony import app  # pyrogram client in your project
except Exception:
    app = None
    errors = None

from anony import config, logger

mimetypes.init()
mimetypes.add_type("audio/mp4", ".m4a")
mimetypes.add_type("audio/mpeg", ".mp3")
mimetypes.add_type("video/mp4", ".mp4")
mimetypes.add_type("audio/webm", ".weba")
mimetypes.add_type("video/webm", ".webm")


class FallenApi:
    """v2-only FallenApi client"""

    def __init__(self, retries: int = 3, timeout: int = 30, download_dir: str = "downloads"):
        self.api_url = (getattr(config, "API_URL", "") or "").rstrip("/") + "/"
        self.api_key = getattr(config, "API_KEY", "") or ""
        self.retries = retries
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.download_dir = Path(download_dir)
        self.download_dir.mkdir(parents=True, exist_ok=True)

    def _headers(self) -> Dict[str, str]:
        return {"Accept": "application/json", "X-API-Key": self.api_key or ""}

    async def _get_json(self, path: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        params = params or {}
        if "api_key" not in params and self.api_key:
            params["api_key"] = self.api_key
        url = urllib.parse.urljoin(self.api_url, path.lstrip("/"))
        for attempt in range(1, self.retries + 1):
            try:
                async with aiohttp.ClientSession(timeout=self.timeout) as session:
                    async with session.get(url, headers=self._headers(), params=params) as resp:
                        data = await resp.json(content_type=None)
                        if resp.status in (200, 201):
                            return data
                        logger.warning(f"[fallenapi v2] GET {url} status={resp.status} body={data}")
                        return data
            except asyncio.TimeoutError:
                logger.warning(f"[fallenapi v2] GET timeout {url} (attempt {attempt})")
            except aiohttp.ClientError as e:
                logger.warning(f"[fallenapi v2] GET network error {url} (attempt {attempt}) -> {e}")
            except Exception as e:
                logger.exception(f"[fallenapi v2] GET unexpected {url} -> {e}")
            await asyncio.sleep(1)
        logger.warning(f"[fallenapi v2] GET failed after {self.retries} attempts: {url}")
        return None

    async def youtube_v2_download(self, query: str, isVideo: bool = False) -> Optional[Dict[str, Any]]:
        return await self._get_json("/youtube/v2/download", {"query": query, "isVideo": str(isVideo).lower()})

    async def youtube_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        return await self._get_json("/youtube/jobStatus", {"job_id": job_id})

    async def youtube_search(self, query: str) -> Optional[Dict[str, Any]]:
        # prefer /search then fallback to /youtube/search
        res = await self._get_json("/search", {"query": query})
        if res:
            return res
        return await self._get_json("/youtube/search", {"query": query})

    # Extract likely download URLs from typical v2 response shapes
    def _extract_urls(self, resp: Dict[str, Any]) -> Dict[str, str]:
        urls: Dict[str, str] = {}
        if not isinstance(resp, dict):
            return urls
        for k in ("cdnurl", "download_url", "download", "url", "file", "stream", "downloadUrl"):
            v = resp.get(k)
            if isinstance(v, str) and v.startswith("http"):
                urls[k] = v
        # nested audio/video structures
        for media in ("audio", "video", "result", "data"):
            node = resp.get(media)
            if isinstance(node, dict):
                for k, v in node.items():
                    if isinstance(v, str) and v.startswith("http"):
                        urls[f"{media}.{k}"] = v
        # formats/sources lists
        formats = resp.get("formats") or resp.get("sources")
        if isinstance(formats, list):
            for fmt in formats:
                if isinstance(fmt, dict):
                    u = fmt.get("url") or fmt.get("download_url")
                    if isinstance(u, str) and u.startswith("http"):
                        key = f"formats.{fmt.get('format_id','')}"
                        urls[key] = u
        return urls

    async def _download_to_file(self, url: str, title_hint: Optional[str] = None) -> Optional[str]:
        if not url:
            return None
        for attempt in range(1, self.retries + 1):
            try:
                async with aiohttp.ClientSession(timeout=self.timeout) as session:
                    async with session.get(url, headers=self._headers()) as resp:
                        if resp.status != 200:
                            logger.warning(f"[fallenapi v2] download {url} returned status {resp.status}")
                            return None
                        cd = resp.headers.get("Content-Disposition", "") or ""
                        filename = None
                        if cd:
                            m = re.findall('filename="?([^";]+)"?', cd)
                            if m:
                                filename = m[0]
                        if not filename and title_hint:
                            ext = ""
                            ctype = resp.headers.get("Content-Type", "")
                            if ctype:
                                ext = mimetypes.guess_extension(ctype.split(";")[0].strip()) or ""
                            filename = f"{self._sanitize(title_hint)}{ext}"
                        if not filename:
                            parsed = urllib.parse.urlparse(url)
                            filename = os.path.basename(parsed.path) or f"{uuid.uuid4().hex}"
                        name, ext = os.path.splitext(filename)
                        if not ext:
                            ctype = resp.headers.get("Content-Type", "")
                            ext = mimetypes.guess_extension(ctype.split(";")[0].strip()) or ""
                            filename = f"{name}{ext}"
                        save_path = self.download_dir / filename
                        if save_path.exists():
                            save_path = self.download_dir / f"{name}_{uuid.uuid4().hex[:8]}{ext}"
                        logger.info(f"[fallenapi v2] downloading {url} -> {save_path}")
                        with open(save_path, "wb") as f:
                            async for chunk in resp.content.iter_chunked(16 * 1024):
                                if chunk:
                                    f.write(chunk)
                        logger.info(f"[fallenapi v2] download finished -> {save_path}")
                        return str(save_path)
            except asyncio.TimeoutError:
                logger.warning(f"[fallenapi v2] download timeout {url} (attempt {attempt})")
            except aiohttp.ClientError as e:
                logger.warning(f"[fallenapi v2] download network error {url} (attempt {attempt}) -> {e}")
            except Exception as e:
                logger.exception(f"[fallenapi v2] download unexpected {url} -> {e}")
            await asyncio.sleep(1)
        logger.warning(f"[fallenapi v2] download failed after retries: {url}")
        return None

    def _sanitize(self, s: str) -> str:
        if not s:
            return "file"
        return re.sub(r"[^\w\-_\. ]", "_", s)[:200]

    async def download_from_youtube(self, query: str, prefer_video: bool = False, poll_timeout: int = 60) -> Optional[Dict[str, Any]]:
        """
        v2-only download flow:
          1. call /youtube/v2/download with query
          2. if it returns a job_id and queued/processing state -> poll /youtube/jobStatus
          3. once result is available, extract URLs and download audio/video (prefers audio unless prefer_video=True)
        Returns:
          { "source": "v2", "meta": <response>, "downloaded": {"audio": path, "video": path, ...} }
          or None on failure
        """
        v2 = await self.youtube_v2_download(query, isVideo=prefer_video)
        if not v2:
            logger.warning("[fallenapi v2] /youtube/v2/download returned no data")
            return None

        # check for job-style response
        status = (v2.get("status") or v2.get("state") or "").lower()
        job_id = v2.get("job_id") or v2.get("jobId") or v2.get("id")
        result = None

        if status in ("queued", "processing", "started") and job_id:
            logger.info(f"[fallenapi v2] job {job_id} started, polling up to {poll_timeout}s")
            for _ in range(poll_timeout):
                await asyncio.sleep(1)
                job = await self.youtube_job_status(job_id)
                if not job:
                    continue
                jstatus = (job.get("status") or job.get("state") or "").lower()
                if jstatus in ("done", "finished", "completed"):
                    # job result may be in job['result'] or job['data'] or the job object itself
                    result = job.get("result") or job.get("data") or job
                    break
                if jstatus in ("failed", "error"):
                    logger.warning(f"[fallenapi v2] job {job_id} failed -> {job}")
                    result = None
                    break
            else:
                logger.warning(f"[fallenapi v2] job {job_id} polling timed out")
                result = None
        else:
            result = v2

        if not result:
            logger.warning("[fallenapi v2] v2 produced no final result")
            return None

        urls = self._extract_urls(result)
        if not urls:
            logger.warning("[fallenapi v2] no downloadable URLs found in v2 result")
            return {"source": "v2", "meta": result, "downloaded": {}}

        downloaded: Dict[str, str] = {}

        # prefer audio unless prefer_video True
        # first look for explicit audio keys
        for k, u in urls.items():
            if not prefer_video and ("audio" in k or ".mp3" in u or ".m4a" in u or "audio" in k):
                p = await self._download_to_file(u, title_hint=result.get("title"))
                if p:
                    downloaded["audio"] = p
                    break

        # if prefer_video or no audio downloaded yet, look for video
        if (prefer_video and not downloaded) or ("video" in "".join(urls.keys())):
            for k, u in urls.items():
                if "video" in k or any(ext in u for ext in (".mp4", ".webm", ".mkv", ".mov")):
                    p = await self._download_to_file(u, title_hint=result.get("title"))
                    if p:
                        downloaded["video"] = p
                        break

        # fallback: download first available url if nothing downloaded yet
        if not downloaded and urls:
            _, u = next(iter(urls.items()))
            p = await self._download_to_file(u, title_hint=result.get("title"))
            if p:
                downloaded["file"] = p

        return {"source": "v2", "meta": result, "downloaded": downloaded}

    # compatibility convenience: map previous get_track -> search
    async def get_track(self, url: str) -> Optional[Dict[str, Any]]:
        return await self.youtube_search(url)

    async def download_track(self, url: str) -> Optional[str]:
        """
        compatibility wrapper: resolve via get_track (search) then download first found URL.
        supports t.me links via pyrogram if available, otherwise HTTP download.
        """
        meta = await self.get_track(url)
        if not meta:
            return None
        cdn = meta.get("cdnurl") or meta.get("url") or meta.get("download")
        if not cdn:
            urls = self._extract_urls(meta)
            if urls:
                cdn = next(iter(urls.values()))
        if not cdn:
            return None

        tg_match = re.match(r"https?://t\.me/([^/]+)/(\d+)", cdn)
        if tg_match and app:
            chat, msg_id = tg_match.groups()
            try:
                msg = await app.get_messages(chat_id=chat, message_ids=int(msg_id))
                path = await msg.download(file_name=str(self.download_dir))
                return path
            except errors.FloodWait as e:
                await asyncio.sleep(e.value)
                return await self.download_track(url)
            except Exception:
                pass
        return await self._download_to_file(cdn, title_hint=meta.get("title"))

