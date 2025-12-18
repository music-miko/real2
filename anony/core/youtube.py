# Copyright (c) 2025 AnonymousX1025
# Licensed under the MIT License.
# This file is part of AnonXMusic

import os
import re
import random
import aiohttp
from typing import Optional, Union

from pyrogram import enums, types
from py_yt import Playlist, VideosSearch

from anony import logger, config
from anony.helpers import Track, utils

# ✅ V2-only downloader client (downloader.py style)
# expects fallenapi.py to expose: client = V2Api(download_dir="downloads")
from .fallenapi import client as v2_client


class YouTube:
    def __init__(self):
        self.base = "https://www.youtube.com/watch?v="

        # Cookie helpers can stay if your bot uses them elsewhere (NOT used for download now)
        self.cookies = []
        self.checked = False
        self.warned = False

        self.regex = re.compile(
            r"(https?://)?(www\.|m\.|music\.)?"
            r"(youtube\.com/(watch\?v=|shorts/|playlist\?list=)|youtu\.be/)"
            r"([A-Za-z0-9_-]{11}|PL[A-Za-z0-9_-]+)([&?][^\s]*)?"
        )

    # -----------------------
    # Cookie helpers (optional)
    # -----------------------
    def get_cookies(self):
        """
        Kept only because your old file had it and you may use it elsewhere.
        NOT used in download() anymore.
        """
        if not self.checked:
            try:
                for file in os.listdir("anony/cookies"):
                    if file.endswith(".txt"):
                        self.cookies.append(file)
            except Exception:
                pass
            self.checked = True

        if not self.cookies:
            if not self.warned:
                self.warned = True
                logger.warning("Cookies are missing; yt-dlp is disabled so this does not affect V2 downloads.")
            return None

        return f"anony/cookies/{random.choice(self.cookies)}"

    async def save_cookies(self, urls: list[str]) -> None:
        """
        Kept for compatibility if admins upload cookies via tg links.
        Not used by V2 downloader.
        """
        logger.info("Saving cookies from urls...")
        for url in urls:
            path = f"anony/cookies/cookie{random.randint(10000, 99999)}.txt"
            link = url.replace("me/", "me/raw/")
            async with aiohttp.ClientSession() as session:
                async with session.get(link) as resp:
                    with open(path, "wb") as fw:
                        fw.write(await resp.read())
        logger.info("Cookies saved.")

    # -----------------------
    # URL helpers
    # -----------------------
    def valid(self, url: str) -> bool:
        return bool(re.match(self.regex, url))

    def url(self, message_1: types.Message) -> Union[str, None]:
        messages = [message_1]
        link = None

        if message_1.reply_to_message:
            messages.append(message_1.reply_to_message)

        for message in messages:
            text = message.text or message.caption or ""

            if message.entities:
                for entity in message.entities:
                    if entity.type == enums.MessageEntityType.URL:
                        link = text[entity.offset : entity.offset + entity.length]
                        break

            if not link and message.caption_entities:
                for entity in message.caption_entities:
                    if entity.type == enums.MessageEntityType.TEXT_LINK:
                        link = entity.url
                        break

            if link:
                break

        if link:
            return link.split("&si")[0].split("?si")[0]
        return None

    # -----------------------
    # Search / Playlist
    # -----------------------
    async def search(self, query: str, m_id: int, video: bool = False) -> Track | None:
        _search = VideosSearch(query, limit=1)
        results = await _search.next()
        if results and results.get("result"):
            data = results["result"][0]
            return Track(
                id=data.get("id"),
                channel_name=data.get("channel", {}).get("name"),
                duration=data.get("duration"),
                duration_sec=utils.to_seconds(data.get("duration")),
                message_id=m_id,
                title=(data.get("title") or "")[:25],
                thumbnail=(data.get("thumbnails", [{}])[-1].get("url") or "").split("?")[0],
                url=data.get("link"),
                view_count=data.get("viewCount", {}).get("short"),
                video=video,
            )
        return None

    async def playlist(self, limit: int, user: str, url: str, video: bool) -> list[Track]:
        plist = await Playlist.get(url)
        tracks = []

        for data in plist.get("videos", [])[:limit]:
            tracks.append(
                Track(
                    id=data.get("id"),
                    channel_name=data.get("channel", {}).get("name", ""),
                    duration=data.get("duration"),
                    duration_sec=utils.to_seconds(data.get("duration")),
                    title=(data.get("title") or "")[:25],
                    thumbnail=(data.get("thumbnails") or [{}])[-1].get("url", "").split("?")[0],
                    url=(data.get("link") or "").split("&list=")[0],
                    user=user,
                    view_count="",
                    video=video,
                )
            )

        return tracks

    # -----------------------
    # Download (V2 ONLY)
    # -----------------------
    async def download(self, video_id: str, video: bool = False) -> Optional[str]:
        """
        ✅ V2 ONLY download (aligned with earlier fallenapi.py / downloader.py style)
        - No yt-dlp
        - No cookies used here
        - Retries + CDN + telegram handled inside fallenapi.py client
        - Files saved into old downloads/ directory by fallenapi.py
        """
        if not (config.API_URL and config.API_KEY):
            logger.warning("V2_DOWNLOAD_FAILED type=%s title='%s' query='%s' (API_URL/API_KEY missing)",
                           "video" if video else "audio", video_id, video_id)
            return None

        url = self.base + video_id

        try:
            # fallenapi.py client already logs only SUCCESS/FAILED internally
            return await v2_client.download(url, isVideo=video, title=video_id)
        except Exception as e:
            # keep it minimal (match your “2 logs only” preference)
            logger.warning(
                "V2_DOWNLOAD_FAILED type=%s title='%s' query='%s' err=%s",
                "video" if video else "audio",
                video_id,
                url,
                e,
            )
            return None
