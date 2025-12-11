# fallenapi.py
# Rewritten to implement arcapi-style endpoints (youtube v1/v2, jobStatus, spotify)
# Async, aiohttp-based client with retries + CDN/Telegram downloading support.

import asyncio
import os
import re
import uuid
from pathlib import Path
from typing import Dict, Optional, Any

import aiohttp
from pydantic import BaseModel

from anony import logger, config, app
from pyrogram import errors

class MusicTrack(BaseModel):
    cdnurl: str
    key: Optional[str] = None
    name: Optional[str] = None
    artist: Optional[str] = None
    tc: Optional[str] = None
    # any extra fields from the API will be ignored by pydantic by default

class FallenApi:
    def __init__(self, retries: int = 3, timeout: int = 20):
        # Ensure API_URL does not end with slash for consistent endpoint building
        self.api_url = config.API_URL.rstrip("/") if getattr(config, "API_URL", None) else ""
        self.api_key = getattr(config, "API_KEY", "") or ""
        self.retries = retries
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.download_dir = Path("downloads")
        self.download_dir.mkdir(exist_ok=True)
        # regex for t.me message links (used by some API responses)
        self._tg_re = re.compile(r"https?://t\.me/([^/]+)/(\d+)")

    def _get_headers(self) -> Dict[str, str]:
        """
        Provide API key either via header. Some arcapi variants use `api_key` query param;
        we include header and still send param where arcapi example used it.
        """
        headers = {
            "Accept": "application/json",
        }
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        return headers

    async def _request_json(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Generic GET request with retries and JSON return. Logs and handles network/timeouts.
        """
        if not self.api_url:
            logger.warning("[FallenApi] API_URL not configured.")
            return None

        url = f"{self.api_url.rstrip('/')}/{endpoint.lstrip('/')}"
        # arcapi example sometimes uses api_key in params; include if present
        params = params or {}
        if self.api_key and "api_key" not in params:
            params["api_key"] = self.api_key

        last_exc = None
        for attempt in range(1, self.retries + 1):
            try:
                async with aiohttp.ClientSession(timeout=self.timeout) as session:
                    async with session.get(url, headers=self._get_headers(), params=params) as resp:
                        text = await resp.text()
                        # Try parse JSON robustly
                        try:
                            data = await resp.json(content_type=None)
                        except Exception:
                            # fallback: try to interpret text as JSON-like or return None
                            logger.warning("[FallenApi] Response not JSON, raw text: %s", text[:200])
                            return None

                        if resp.status == 200:
                            return data
                        else:
                            # log server-provided error message if present
                            err = data.get("error") or data.get("message") or f"HTTP {resp.status}"
                            logger.warning("[FallenApi] API returned error: %s (status %s) -- %s", err, resp.status, url)
                            return None

            except asyncio.TimeoutError as e:
                last_exc = e
                logger.warning("[FallenApi] Timeout attempt %s/%s calling %s", attempt, self.retries, url)
            except aiohttp.ClientError as e:
                last_exc = e
                logger.warning("[FallenApi] Network error attempt %s/%s calling %s: %s", attempt, self.retries, url, e)
            except Exception as e:
                last_exc = e
                logger.exception("[FallenApi] Unexpected error on attempt %s/%s calling %s: %s", attempt, self.retries, url, e)

            await asyncio.sleep(1)

        logger.warning("[FallenApi] All retry attempts exhausted for %s. Last error: %s", url, repr(last_exc))
        return None

    # --- Arc-like endpoints (async equivalents) --------------------------------

    async def youtube_search(self, query: str) -> Optional[Dict[str, Any]]:
        """
        Calls: GET {API_URL}/youtube/search?query=<query>&api_key=<key>
        Returns the parsed JSON response or None on failure.
        """
        return await self._request_json("youtube/search", params={"query": query})

    async def youtube_v1_download(self, query: str, isVideo: bool = False) -> Optional[Dict[str, Any]]:
        """
        Calls: GET {API_URL}/youtube/v1/download?query=<query>&isVideo=<true|false>&api_key=<key>
        Returns JSON describing the download result (job/cdnurl/metadata).
        """
        return await self._request_json("youtube/v1/download", params={"query": query, "isVideo": str(isVideo).lower()})

    async def youtube_v2_download(self, query: str, isVideo: bool = False) -> Optional[Dict[str, Any]]:
        """
        Calls: GET {API_URL}/youtube/v2/download?query=<query>&isVideo=<true|false>&api_key=<key>
        V2 may return improved metadata / job handling. This method supports the v2 route.
        """
        return await self._request_json("youtube/v2/download", params={"query": query, "isVideo": str(isVideo).lower()})

    async def youtube_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Calls: GET {API_URL}/youtube/jobStatus?job_id=<id>
        Useful if v2 returns a job id indicating background processing.
        """
        return await self._request_json("youtube/jobStatus", params={"job_id": job_id})

    async def spotify_get_name(self, link: str) -> Optional[Dict[str, Any]]:
        """
        Calls: GET {API_URL}/spotify/name?link=<link>&api_key=<key>
        Returns metadata for spotify link (name/artist/...).
        """
        return await self._request_json("spotify/name", params={"link": link})

    async def spotify_download(self, link: str, isVideo: bool = False) -> Optional[Dict[str, Any]]:
        """
        Calls: GET {API_URL}/spotify/download?link=<link>&isVideo=<true|false>&api_key=<key>
        """
        return await self._request_json("spotify/download", params={"link": link, "isVideo": str(isVideo).lower()})

    # --- CDN / tg download helper (kept from prior implementation) --------------

    async def _download_cdn_to_file(self, cdn_url: str) -> Optional[str]:
        """
        Download a file from an arbitrary CDN URL into downloads/ directory.
        Preserves filename from content-disposition if present; otherwise picks a uuid-based name.
        """
        for attempt in range(1, self.retries + 1):
            try:
                async with aiohttp.ClientSession(timeout=self.timeout) as session:
                    async with session.get(cdn_url) as resp:
                        if resp.status != 200:
                            logger.warning("[FallenApi] CDN responded %s for %s", resp.status, cdn_url)
                            return None

                        cd = resp.headers.get("Content-Disposition")
                        if cd:
                            import re as _re
                            match = _re.findall('filename="?([^";]+)"?', cd)
                            filename = match[0] if match else None
                        else:
                            filename = None

                        if not filename:
                            filename = os.path.basename(cdn_url.split("?")[0]) or f"{uuid.uuid4().hex[:8]}.mp3"

                        save_path = self.download_dir / filename

                        # Stream-write
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
        """
        If API returns a t.me link (chat/msg), download it using pyrogram client.
        """
        tg_match = self._tg_re.match(tme_url)
        if not tg_match:
            return None
        chat, msg_id = tg_match.groups()
        try:
            msg = await app.get_messages(chat_id=chat, message_ids=int(msg_id))
            file_path = await msg.download(file_name=self.download_dir)
            logger.info("[FallenApi] Telegram media downloaded: %s", file_path)
            return file_path
        except errors.FloodWait as e:
            logger.warning("[FallenApi] FloodWait %s, sleeping then retrying.", e.value)
            await asyncio.sleep(e.value)
            return await self._download_telegram_media(tme_url)
        except Exception as e:
            logger.warning("[FallenApi] Error downloading telegram media %s: %s", tme_url, e)
            return None

    # --- High-level download helper used by other parts of your bot --------------
    async def download_track(self, url: str) -> Optional[str]:
        """
        High-level function that: queries the API for track metadata (v2 preferred),
        then downloads the returned cdn/t.me resource to local disk and returns local path.

        This mirrors the previous project's behavior (YouTube.download called fallen.download_track(url)).
        """
        # Prefer v2 if available
        logger.info("[FallenApi] Requesting download for %s (using youtube_v2_download)", url)
        resp = await self.youtube_v2_download(url, isVideo=False)
        if not resp:
            logger.info("[FallenApi] v2 returned nothing, falling back to v1")
            resp = await self.youtube_v1_download(url, isVideo=False)

        if not resp:
            logger.warning("[FallenApi] No response from API for download request.")
            return None

        # Some APIs return job id when background processing is used
        job_id = resp.get("job_id") or resp.get("job")
        if job_id and not resp.get("cdnurl"):
            # user of jobStatus should poll for completion; we'll try one poll cycle here
            logger.info("[FallenApi] Received job_id %s, checking job status once.", job_id)
            status = await self.youtube_job_status(str(job_id))
            if status and status.get("cdnurl"):
                resp = status

        # If the response is the full track-like object, attempt to model it
        try:
            track = MusicTrack(**resp)
        except Exception:
            # Fallback: try to pick a cdnurl field from resp
            track = None

        dl_url = None
        if track and getattr(track, "cdnurl", None):
            dl_url = track.cdnurl
        else:
            dl_url = resp.get("cdnurl") or resp.get("url") or resp.get("download_url")

        if not dl_url:
            logger.warning("[FallenApi] No downloadable URL found in API response.")
            return None

        # If it's a telegram link, download via pyrogram
        if dl_url.startswith("https://t.me/") or self._tg_re.match(dl_url):
            tg_path = await self._download_telegram_media(dl_url)
            if tg_path:
                return tg_path

        # Otherwise, download from CDN
        file_path = await self._download_cdn_to_file(dl_url)
        return file_path
