# Copyright (c) 2025 AnonymousX1025
# Licensed under the MIT License.
# This file is part of AnonXMusic

import os
import re
import yt_dlp
import random
import asyncio
import aiohttp
from pathlib import Path
from typing import Optional, Union

from pyrogram import enums, types
from py_yt import Playlist, VideosSearch

from anony import logger, config
from anony.helpers import Track, utils

from .fallenapi import client as FallenApi


class YouTube:
    def __init__(self):
        self.base = "https://www.youtube.com/watch?v="
        self.cookies = []
        self.checked = False
        self.warned = False

        # kept as in your file (API path)
        self.fallen = FallenApi()

        self.regex = re.compile(
            r"(https?://)?(www\.|m\.|music\.)?"
            r"(youtube\.com/(watch\?v=|shorts/|playlist\?list=)|youtu\.be/)"
            r"([A-Za-z0-9_-]{11}|PL[A-Za-z0-9_-]+)([&?][^\s]*)?"
        )

    # -----------------------
    # Cookies / Proxy helpers
    # -----------------------
    def get_cookies(self) -> Optional[str]:
        """
        Return a random cookie file path (or None).
        Safe even if folder doesn't exist.
        """
        if not self.checked:
            self.checked = True
            try:
                if os.path.isdir("anony/cookies"):
                    for file in os.listdir("anony/cookies"):
                        if file.endswith(".txt"):
                            self.cookies.append(file)
            except Exception:
                pass

        if not self.cookies:
            if not self.warned:
                self.warned = True
                logger.warning("Cookies are missing; yt-dlp fallbacks might fail.")
            return None

        return f"anony/cookies/{random.choice(self.cookies)}"

    def pick_proxy(self) -> Optional[str]:
        """
        Optional proxy support for yt-dlp fallbacks.
        Looks for config.PROXIES (list) or config.PROXY (string).
        """
        try:
            proxies = getattr(config, "PROXIES", None)
            if proxies and isinstance(proxies, (list, tuple)) and len(proxies) > 0:
                return random.choice(list(proxies))
        except Exception:
            pass

        try:
            p = getattr(config, "PROXY", None)
            if p and isinstance(p, str) and p.strip():
                return p.strip()
        except Exception:
            pass

        return None

    async def save_cookies(self, urls: list[str]) -> None:
        logger.info("Saving cookies from urls...")
        os.makedirs("anony/cookies", exist_ok=True)
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
                    if entity.type == enums.MessageEntityType.TEXT_LINK:
                        link = entity.url
                        break

            if not link and message.caption_entities:
                for entity in message.caption_entities:
                    if entity.type == enums.MessageEntityType.TEXT_LINK:
                        link = entity.url
                        break

        if link:
            return link.split("&si")[0].split("?si")[0]
        return None

    # -----------------------
    # Search: py_yt primary, yt-dlp fallback (cookies/proxy)
    # -----------------------
    async def _search_with_ytdlp(self, query: str) -> Optional[dict]:
        """
        yt-dlp metadata-only search (no download).
        Tries: proxy -> cookies -> proxy+cookies -> plain.
        """
        proxy = self.pick_proxy()
        cookie = self.get_cookies()

        def _extract(opts: dict):
            with yt_dlp.YoutubeDL(opts) as ydl:
                return ydl.extract_info(f"ytsearch1:{query}", download=False)

        # 1) proxy only
        if proxy:
            try:
                opts = {
                    "quiet": True,
                    "no_warnings": True,
                    "skip_download": True,
                    "extract_flat": True,
                    "proxy": proxy,
                    "nocheckcertificate": True,
                }
                return await asyncio.to_thread(_extract, opts)
            except Exception:
                pass

        # 2) cookies only
        if cookie:
            try:
                opts = {
                    "quiet": True,
                    "no_warnings": True,
                    "skip_download": True,
                    "extract_flat": True,
                    "cookiefile": cookie,
                    "nocheckcertificate": True,
                }
                return await asyncio.to_thread(_extract, opts)
            except Exception:
                pass

        # 3) proxy + cookies
        if proxy and cookie:
            try:
                opts = {
                    "quiet": True,
                    "no_warnings": True,
                    "skip_download": True,
                    "extract_flat": True,
                    "proxy": proxy,
                    "cookiefile": cookie,
                    "nocheckcertificate": True,
                }
                return await asyncio.to_thread(_extract, opts)
            except Exception:
                pass

        # 4) plain
        try:
            opts = {
                "quiet": True,
                "no_warnings": True,
                "skip_download": True,
                "extract_flat": True,
                "nocheckcertificate": True,
            }
            return await asyncio.to_thread(_extract, opts)
        except Exception:
            return None

    async def search(self, query: str, m_id: int, video: bool = False) -> Track | None:
        # Primary: py_yt
        try:
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
        except Exception as e:
            # This is where your "HTTP 403 Sorry..." was killing the bot
            logger.warning("VideosSearch failed (likely 403). Falling back to yt-dlp search. err=%s", e)

        # Fallback: yt-dlp search
        info = await self._search_with_ytdlp(query)
        if not info or not isinstance(info, dict):
            return None

        entries = info.get("entries") or []
        if not entries:
            return None

        first = entries[0] or {}
        vid = first.get("id") or ""
        title = (first.get("title") or "")[:25]
        channel = first.get("uploader") or first.get("channel") or ""
        thumb = (first.get("thumbnail") or "").split("?")[0]

        return Track(
            id=vid,
            channel_name=channel,
            duration=None,
            duration_sec=0,
            message_id=m_id,
            title=title,
            thumbnail=thumb,
            url=(self.base + vid) if vid else "",
            view_count="",
            video=video,
        )

    async def playlist(self, limit: int, user: str, url: str, video: bool) -> list[Track]:
        plist = await Playlist.get(url)
        tracks = []
        for data in plist["videos"][:limit]:
            track = Track(
                id=data.get("id"),
                channel_name=data.get("channel", {}).get("name", ""),
                duration=data.get("duration"),
                duration_sec=utils.to_seconds(data.get("duration")),
                title=(data.get("title") or "")[:25],
                thumbnail=(data.get("thumbnails")[-1].get("url") or "").split("?")[0],
                url=(data.get("link") or "").split("&list=")[0],
                user=user,
                view_count="",
                video=video,
            )
            tracks.append(track)
        return tracks

    # -----------------------
    # Download: API first, yt-dlp+cookies fallback
    # -----------------------
    async def download(self, video_id: str, video: bool = False) -> Optional[str]:
        url = self.base + video_id

        # 1) API path (as your file already does)
        if config.API_URL and config.API_KEY:
            try:
                file_path = await self.fallen.download(url, isVideo=video)
                if file_path:
                    return file_path
            except Exception as e:
                logger.warning("[API] download failed, switching to yt-dlp fallback: %s", e)

        # 2) yt-dlp fallback with cookie rotation
        ext = "mp4" if video else "webm"
        filename = f"downloads/{video_id}.{ext}"
        Path("downloads").mkdir(parents=True, exist_ok=True)

        if Path(filename).exists():
            return filename

        base_opts = {
            "outtmpl": "downloads/%(id)s.%(ext)s",
            "quiet": True,
            "noplaylist": True,
            "geo_bypass": True,
            "no_warnings": True,
            "overwrites": False,
            "nocheckcertificate": True,
        }

        proxy = self.pick_proxy()
        if proxy:
            base_opts["proxy"] = proxy

        if video:
            fmt_opts = {
                "format": "(bestvideo[height<=?720][width<=?1280][ext=mp4])+(bestaudio)",
                "merge_output_format": "mp4",
            }
        else:
            fmt_opts = {
                "format": "bestaudio[ext=webm][acodec=opus]",
            }

        async def _try_with_cookie(cookie_path: Optional[str]) -> Optional[str]:
            ydl_opts = dict(base_opts)
            ydl_opts.update(fmt_opts)
            if cookie_path:
                ydl_opts["cookiefile"] = cookie_path

            def _run():
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([url])
                return True

            try:
                await asyncio.to_thread(_run)
                return filename if Path(filename).exists() else filename
            except Exception:
                return None

        # try 1: cookie
        tried = 0
        max_cookie_tries = 1

        while tried < max_cookie_tries:
            cookie = self.get_cookies()
            tried += 1
            out = await _try_with_cookie(cookie)
            if out:
                return out

            # remove bad cookie from pool (best effort)
            try:
                if cookie and cookie.startswith("anony/cookies/"):
                    name = cookie.split("/")[-1]
                    if name in self.cookies:
                        self.cookies.remove(name)
            except Exception:
                pass

        # final try: no cookies
        return await _try_with_cookie(None)
