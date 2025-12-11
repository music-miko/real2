# fallenapi.py (modified - v2-compatible api methods added)
import asyncio
import os
import re
import uuid
import urllib.parse
from pathlib import Path
from typing import Dict, Optional, Any

import aiohttp
from pydantic import BaseModel
from pyrogram import errors

from anony import config, logger, app


class MusicTrack(BaseModel):
    cdnurl: str
    key: str
    name: str
    artist: str
    tc: str
    # optional fields commented out in original:
    # cover: Optional[str] = None
    # lyrics: Optional[str] = None
    # album: Optional[str] = None
    # year: Optional[int] = None
    # duration: Optional[int] = None
    # platform: Optional[str] = None


class FallenApi:
    """
    Async FallenApi client.
    Implements v2-style endpoints similar to arcapi.py but keeping async semantics.
    """

    def __init__(self, retries: int = 3, timeout: int = 15):
        self.api_url = config.API_URL.rstrip("/")
        self.api_key = config.API_KEY
        self.retries = retries
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.download_dir = Path("downloads")
        self.download_dir.mkdir(exist_ok=True)

    def _get_headers(self) -> Dict[str, str]:
        # Keep X-API-Key header (original fallenapi used this) for compatibility
        return {
            "X-API-Key": self.api_key or "",
            "Accept": "application/json",
        }

    async def _get_json(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """Generic GET + JSON with retries and logging"""
        params = params or {}
        # also add api_key param for endpoints that expect it (arcapi uses api_key param)
        if "api_key" not in params and self.api_key:
            params["api_key"] = self.api_key

        for attempt in range(1, self.retries + 1):
            try:
                async with aiohttp.ClientSession(timeout=self.timeout) as session:
                    async with session.get(endpoint, headers=self._get_headers(), params=params) as resp:
                        # allow content_type None to avoid strict content-type checks
                        data = await resp.json(content_type=None)
                        if resp.status == 200:
                            return data
                        # if non-200, log and return the JSON (which may contain error)
                        error_msg = data.get("error") or data.get("message")
                        status = data.get("status", resp.status)
                        logger.warning(f"[API ERROR] {endpoint} -> {error_msg or 'Unexpected error'} (status {status})")
                        return data
            except aiohttp.ClientError as e:
                logger.warning(f"[NETWORK ERROR] Attempt {attempt}/{self.retries} for {endpoint} failed: {e}")
            except asyncio.TimeoutError:
                logger.warning(f"[TIMEOUT] Attempt {attempt}/{self.retries} for {endpoint} exceeded timeout.")
            except Exception as e:
                logger.warning(f"[UNEXPECTED ERROR] {endpoint} -> {e}")

            await asyncio.sleep(1)

        logger.warning(f"[FAILED] All retry attempts exhausted for {endpoint}.")
        return None

    # --------------------------
    # ArcAPI-like endpoints (async)
    # --------------------------
    async def youtube_search(self, query: str) -> Optional[Dict[str, Any]]:
        """Calls /youtube/search (v2-style)"""
        endpoint = f"{self.api_url}/youtube/search"
        params = {"query": query}
        return await self._get_json(endpoint, params=params)

    async def youtube_v1_download(self, query: str, isVideo: bool = False) -> Optional[Dict[str, Any]]:
        """Calls /youtube/v1/download"""
        endpoint = f"{self.api_url}/youtube/v1/download"
        params = {"query": query, "isVideo": str(isVideo).lower()}
        return await self._get_json(endpoint, params=params)

    async def youtube_v2_download(self, query: str, isVideo: bool = False) -> Optional[Dict[str, Any]]:
        """Calls /youtube/v2/download (preferred v2)"""
        endpoint = f"{self.api_url}/youtube/v2/download"
        params = {"query": query, "isVideo": str(isVideo).lower()}
        return await self._get_json(endpoint, params=params)

    async def youtube_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Calls /youtube/jobStatus"""
        endpoint = f"{self.api_url}/youtube/jobStatus"
        params = {"job_id": job_id}
        return await self._get_json(endpoint, params=params)

    async def spotify_get_name(self, link: str) -> Optional[Dict[str, Any]]:
        """Calls /spotify/name"""
        endpoint = f"{self.api_url}/spotify/name"
        params = {"link": link}
        return await self._get_json(endpoint, params=params)

    async def spotify_download(self, link: str, isVideo: bool = False) -> Optional[Dict[str, Any]]:
        """Calls /spotify/download"""
        endpoint = f"{self.api_url}/spotify/download"
        params = {"link": link, "isVideo": str(isVideo).lower()}
        return await self._get_json(endpoint, params=params)

    # --------------------------
    # Existing FallenApi behavior (unchanged, but kept in same class)
    # --------------------------
    async def get_track(self, url: str) -> Optional[MusicTrack]:
        """
        Original behavior: /track?url=...
        If API returns 200 and a track-like JSON, returns MusicTrack.
        """
        endpoint = f"{self.api_url}/track?url={urllib.parse.quote(url)}"

        for attempt in range(1, self.retries + 1):
            try:
                async with aiohttp.ClientSession(timeout=self.timeout) as session:
                    async with session.get(endpoint, headers=self._get_headers()) as resp:
                        data = await resp.json(content_type=None)
                        if resp.status == 200:
                            # Pydantic will validate/convert
                            return MusicTrack(**data)

                        error_msg = data.get("error") or data.get("message")
                        status = data.get("status", resp.status)
                        logger.warning(f"[API ERROR] {error_msg or 'Unexpected error'} (status {status})")
                        return None

            except aiohttp.ClientError as e:
                logger.warning(f"[NETWORK ERROR] Attempt {attempt}/{self.retries} failed: {e}")
            except asyncio.TimeoutError:
                logger.warning(f"[TIMEOUT] Attempt {attempt}/{self.retries} exceeded timeout.")
            except Exception as e:
                logger.warning(f"[UNEXPECTED ERROR] {e}")

            await asyncio.sleep(1)

        logger.warning("[FAILED] All retry attempts exhausted.")
        return None

    async def download_cdn(self, cdn_url: str) -> Optional[str]:
        """Download file from a CDN URL to downloads/ directory"""
        for attempt in range(1, self.retries + 1):
            try:
                async with aiohttp.ClientSession(timeout=self.timeout) as session:
                    async with session.get(cdn_url) as resp:
                        if resp.status != 200:
                            logger.warning(f"[HTTP {resp.status}] Failed to download from {cdn_url}")
                            return None

                        cd = resp.headers.get("Content-Disposition")
                        if cd:
                            match = re.findall('filename="?([^";]+)"?', cd)
                            filename = match[0] if match else None
                        else:
                            filename = None

                        if not filename:
                            filename = os.path.basename(cdn_url.split("?")[0]) or f"{uuid.uuid4().hex[:8]}.mp3"

                        save_path = self.download_dir / filename

                        with open(save_path, "wb") as f:
                            async for chunk in resp.content.iter_chunked(16 * 1024):
                                if chunk:
                                    f.write(chunk)

                        logger.info(f"Download complete: {save_path}")
                        return str(save_path)

            except aiohttp.ClientError as e:
                logger.warning(f"[NETWORK ERROR] Attempt {attempt}/{self.retries} failed: {e}")
            except asyncio.TimeoutError:
                logger.warning(f"[TIMEOUT] Attempt {attempt}/{self.retries} exceeded timeout.")
            except Exception as e:
                logger.warning(f"[UNEXPECTED ERROR] {e}")

            await asyncio.sleep(1)

        logger.warning("[FAILED] CDN download attempts exhausted.")
        return None

    async def download_track(self, url: str) -> Optional[str]:
        """
        High-level: call get_track (existing /track endpoint),
        if cdnurl points to a Telegram message link, download via pyrogram,
        otherwise download from cdnurl.
        """
        track = await self.get_track(url)
        if not track:
            logger.warning("[❌] No track metadata found.")
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
                return await self.download_track(url)
            except Exception as e:
                logger.warning(f"[TG DOWNLOAD ERROR] {e}")
                return None

        return await self.download_cdn(dl_url)


# Example async usage:
# import asyncio
# from fallenapi import FallenApi
#
# async def main():
#     api = FallenApi()
#     res = await api.youtube_search("alan walker faded")
#     print(res)
#
# asyncio.run(main())
