import os
import re
import random
import asyncio
from pathlib import Path
from typing import Optional, Union

import aiohttp
import yt_dlp
from pyrogram import enums, types
from py_yt import Playlist, VideosSearch

from anony import logger, config
from anony.helpers import Track, utils

# ✅ compatible with your attached fallenapi.py (exports `client = V2Api(...)`)
from .fallenapi import client as v2_client


class YouTube:
    def __init__(self):
        self.base = "https://www.youtube.com/watch?v="

        self.cookies = []
        self.checked = False
        self.warned = False

        self.regex = re.compile(
            r"(https?://)?(www\.|m\.|music\.)?"
            r"(youtube\.com/(watch\?v=|shorts/|playlist\?list=)|youtu\.be/)"
            r"([A-Za-z0-9_-]{11}|PL[A-Za-z0-9_-]+)([&?][^\s]*)?"
        )

        Path("downloads").mkdir(parents=True, exist_ok=True)

    # -----------------------
    # Cookies / Proxy helpers
    # -----------------------
    def get_cookies(self) -> Optional[str]:
        """Return random cookie file path from anony/cookies or None."""
        if not self.checked:
            self.checked = True
            try:
                if os.path.isdir("anony/cookies"):
                    for f in os.listdir("anony/cookies"):
                        if f.endswith(".txt"):
                            self.cookies.append(f)
            except Exception:
                pass

        if not self.cookies:
            if not self.warned:
                self.warned = True
                logger.warning("Cookie files not found in anony/cookies (yt-dlp fallbacks may fail).")
            return None

        return f"anony/cookies/{random.choice(self.cookies)}"

    def pick_proxy(self) -> Optional[str]:
        """
        Optional proxy support:
        - config.PROXIES = [..] OR config.PROXY = "http://user:pass@ip:port"
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
        """Kept for admin cookie uploads; used by yt-dlp fallback only."""
        os.makedirs("anony/cookies", exist_ok=True)
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
        if message_1.reply_to_message:
            messages.append(message_1.reply_to_message)

        for message in messages:
            text = message.text or message.caption or ""

            if message.entities:
                for entity in message.entities:
                    if entity.type == enums.MessageEntityType.URL:
                        try:
                            return text[entity.offset : entity.offset + entity.length].split("&si")[0].split("?si")[0]
                        except Exception:
                            pass
                    if entity.type == enums.MessageEntityType.TEXT_LINK:
                        return (entity.url or "").split("&si")[0].split("?si")[0]

            if message.caption_entities:
                for entity in message.caption_entities:
                    if entity.type == enums.MessageEntityType.TEXT_LINK:
                        return (entity.url or "").split("&si")[0].split("?si")[0]

        return None

    # -----------------------
    # yt-dlp SEARCH fallback (metadata only)
    # -----------------------
    async def _search_with_ytdlp(self, query: str) -> Optional[dict]:
        proxy = self.pick_proxy()
        cookie = self.get_cookies()

        def _extract(opts: dict):
            with yt_dlp.YoutubeDL(opts) as ydl:
                return ydl.extract_info(f"ytsearch1:{query}", download=False)

        # try: proxy -> cookies -> proxy+cookies -> plain
        attempts = []
        if proxy:
            attempts.append({"proxy": proxy})
        if cookie:
            attempts.append({"cookiefile": cookie})
        if proxy and cookie:
            attempts.append({"proxy": proxy, "cookiefile": cookie})
        attempts.append({})

        for extra in attempts:
            try:
                opts = {
                    "quiet": True,
                    "no_warnings": True,
                    "skip_download": True,
                    "extract_flat": True,
                    "nocheckcertificate": True,
                }
                opts.update(extra)
                return await asyncio.to_thread(_extract, opts)
            except Exception:
                continue

        return None

    # -----------------------
    # Search / Playlist
    # -----------------------
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
            # This is your 403 case
            logger.warning("VideosSearch failed (likely 403). Falling back to yt-dlp search. err=%s", e)

        # Fallback: yt-dlp metadata search
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
        tracks: list[Track] = []
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
    # Download: V2 first, yt-dlp fallback
    # -----------------------
    async def download(self, video_id: str, video: bool = False) -> Optional[str]:
        url = self.base + video_id

        # cache hit (your v2 saves .mp3 / .mp4 in downloads/)
        expected = f"downloads/{video_id}.mp4" if video else f"downloads/{video_id}.mp3"
        if Path(expected).exists():
            return expected

        # 1) V2 API (your attached fallenapi.py client)
        if getattr(config, "API_URL", None) and getattr(config, "API_KEY", None):
            try:
                out = await v2_client.download(url, isVideo=video, title=video_id)
                if out:
                    return out
            except Exception:
                pass

        # 2) yt-dlp fallback download (cookies/proxy) — best-effort
        proxy = self.pick_proxy()
        cookie = self.get_cookies()

        outtmpl = "downloads/%(id)s.%(ext)s"
        base_opts = {
            "outtmpl": outtmpl,
            "quiet": True,
            "no_warnings": True,
            "noplaylist": True,
            "geo_bypass": True,
            "overwrites": False,
            "nocheckcertificate": True,
        }

        if proxy:
            base_opts["proxy"] = proxy

        if video:
            # mp4 best effort
            fmt_opts = {
                "format": "(bestvideo[ext=mp4]/bestvideo)+(bestaudio/best)",
                "merge_output_format": "mp4",
            }
        else:
            # bestaudio; we won't convert (if it isn't mp3, your player may still handle it)
            fmt_opts = {"format": "bestaudio/best"}

        async def _dl(extra: dict) -> Optional[str]:
            opts = dict(base_opts)
            opts.update(fmt_opts)
            opts.update(extra)

            def _run():
                with yt_dlp.YoutubeDL(opts) as ydl:
                    ydl.download([url])

            try:
                await asyncio.to_thread(_run)
                # try to locate result
                for ext in (("mp4", "mkv", "webm") if video else ("mp3", "m4a", "webm", "opus")):
                    p = Path(f"downloads/{video_id}.{ext}")
                    if p.exists():
                        return str(p)
                return None
            except Exception:
                return None

        # cookie rotations (few tries)
        tries = []
        if cookie:
            tries.append({"cookiefile": cookie})
        tries.append({})  # no cookies

        for extra in tries:
            out = await _dl(extra)
            if out:
                return out

        return None
