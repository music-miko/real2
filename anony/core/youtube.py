# Copyright (c) 2025 AnonymousX1025
# Licensed under the MIT License.
# This file is part of AnonXMusic

import re
from typing import Optional, Union

from pyrogram import enums, types
from py_yt import Playlist, VideosSearch

from anony import logger
from anony.helpers import Track, utils

from .fallenapi import FallenApi


class YouTube:
    def __init__(self):
        self.base = "https://www.youtube.com/watch?v="

        # ✅ V2-only downloader (downloads will be stored in old downloads/ folder)
        self.fallen = FallenApi(download_dir="downloads")

        self.regex = re.compile(
            r"(https?://)?(www\.|m\.|music\.)?"
            r"(youtube\.com/(watch\?v=|shorts/|playlist\?list=)|youtu\.be/)"
            r"([A-Za-z0-9_-]{11}|PL[A-Za-z0-9_-]+)([&?][^\s]*)?"
        )

    def valid(self, url: str) -> bool:
        return bool(re.match(self.regex, url))

    def url(self, message_1: types.Message) -> Union[str, None]:
        messages = [message_1]
        link = None

        if message_1.reply_to_message:
            messages.append(message_1.reply_to_message)

        for message in messages:
            # text entities
            if message.entities:
                for entity in message.entities:
                    if entity.type == enums.MessageEntityType.URL:
                        try:
                            link = message.text[entity.offset : entity.offset + entity.length]
                            break
                        except Exception:
                            continue
                    if entity.type == enums.MessageEntityType.TEXT_LINK:
                        link = entity.url
                        break

            # caption entities
            if not link and message.caption_entities:
                for entity in message.caption_entities:
                    if entity.type == enums.MessageEntityType.URL:
                        try:
                            link = message.caption[entity.offset : entity.offset + entity.length]
                            break
                        except Exception:
                            continue
                    if entity.type == enums.MessageEntityType.TEXT_LINK:
                        link = entity.url
                        break

            if link:
                break

        if link:
            # remove extra tracking params like &si / ?si
            return link.split("&si")[0].split("?si")[0]
        return None

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
        tracks: list[Track] = []
        for data in plist.get("videos", [])[:limit]:
            track = Track(
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
            tracks.append(track)
        return tracks

    async def download(self, video_id: str, video: bool = False) -> Optional[str]:
        """
        ✅ V2 ONLY download (no yt-dlp, no cookies, no proxies here)
        - video=False -> audio (.mp3)
        - video=True  -> video (.mp4)
        - stored in downloads/ folder (old path)
        """
        url = self.base + video_id

        try:
            file_path = await self.fallen.download(url, isVideo=video, title=video_id)
            return file_path
        except Exception as e:
            logger.warning(
                "V2_DOWNLOAD_FAILED type=%s id=%s err=%s",
                "video" if video else "audio",
                video_id,
                e,
            )
            return None
