# youtube.py (updated to use FallenApi.download_from_youtube v2-only)
import asyncio
import os
import time
from pathlib import Path
from typing import Optional

from anony import config, logger  # keep your project's config & logger
# Import FallenApi (v2-only) — make sure fallenapi.py is in your project and exports FallenApi
from fallenapi import FallenApi

# If your project used yt_dlp fallback, keep imports needed
try:
    import yt_dlp as ytdl
except Exception:
    ytdl = None

DOWNLOAD_DIR = Path("downloads")
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)


class YouTubeDownloader:
    def __init__(self):
        self.fallen = FallenApi()
        # optional: adjust prefer_video default settings or other prefs here
        self.default_prefer_video = False

    async def download(self, query: str, video: bool = False, timeout: int = 300) -> Optional[str]:
        """
        Primary entry point used by the bot:
         - Try FallenApi.download_from_youtube (v2-only) which downloads to ./downloads/
         - If fallenapi fails or returns nothing, fall back to local yt-dlp (if available)
        Returns:
         - local file path (str) on success
         - None on failure
        """
        logger.info(f"[youtube] Starting download for query/url: {query} (video={video})")

        # 1) Try the v2 arcapi route (high-level)
        try:
            # Timeout guard around the whole v2 operation
            coro = self.fallen.download_from_youtube(query, prefer_video=video, poll_timeout=60)
            res = await asyncio.wait_for(coro, timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("[youtube] FallenApi download_from_youtube timed out.")
            res = None
        except Exception as e:
            logger.exception(f"[youtube] FallenApi raised exception: {e}")
            res = None

        # If v2 returned a dict with downloaded files, choose file to return
        if isinstance(res, dict):
            downloaded = res.get("downloaded") or {}
            # Prefer explicit audio/video keys
            chosen_path = None
            if video and downloaded.get("video"):
                chosen_path = downloaded.get("video")
            elif not video and downloaded.get("audio"):
                chosen_path = downloaded.get("audio")
            # fallback ordering: file -> audio -> video
            if not chosen_path:
                for k in ("file", "audio", "video"):
                    if downloaded.get(k):
                        chosen_path = downloaded.get(k)
                        break

            if chosen_path:
                # verify file exists
                if os.path.exists(chosen_path):
                    logger.info(f"[youtube] FallenApi downloaded file found: {chosen_path}")
                    return chosen_path
                else:
                    logger.warning(f"[youtube] FallenApi reported file but it does not exist: {chosen_path}")

            # if response present but no files downloaded, treat as failure so fallback activates
            logger.info("[youtube] FallenApi returned response but no downloaded file found — falling back to yt-dlp if available.")

        # 2) Fallback: use local yt-dlp if available (keeps previous behavior)
        if ytdl is None:
            logger.warning("[youtube] yt-dlp not available; cannot fallback. Returning None.")
            return None

        logger.info("[youtube] Falling back to local yt-dlp download.")
        # Build yt-dlp options — match previous options in your original file if you had custom ones
        ytdl_opts = {
            "format": "bestaudio/best" if not video else "best",
            "outtmpl": str(DOWNLOAD_DIR / "%(id)s.%(ext)s"),
            "nocheckcertificate": True,
            "noplaylist": True,
            "quiet": True,
            "no_warnings": True,
            "ignoreerrors": True,
        }

        # optionally include cookies file if your project used it
        cookies_file = getattr(config, "YTDL_COOKIES", None)
        if cookies_file:
            ytdl_opts["cookiefile"] = cookies_file

        # Progress & download via yt-dlp
        try:
            with ytdl.YoutubeDL(ytdl_opts) as dl:
                info = dl.extract_info(query, download=True)
                # info may be a dict, or a playlist -> list
                if not info:
                    logger.warning("[youtube] yt-dlp returned no info.")
                    return None

                # If extracted info is a playlist, choose first entry
                if "entries" in info and isinstance(info["entries"], list) and info["entries"]:
                    info = info["entries"][0]

                # Determine filename from ytdl result
                filename = dl.prepare_filename(info)
                if os.path.exists(filename):
                    logger.info(f"[youtube] yt-dlp downloaded file: {filename}")
                    return filename
                else:
                    # Sometimes prepare_filename uses templates not matching actual; try to infer ext
                    ext = info.get("ext") or ( "mp4" if video else "webm" )
                    candidate = str(DOWNLOAD_DIR / f"{info.get('id')}.{ext}")
                    if os.path.exists(candidate):
                        return candidate
                    logger.warning(f"[youtube] yt-dlp did not leave expected file: {filename} / {candidate}")
                    return None
        except Exception as e:
            logger.exception(f"[youtube] yt-dlp fallback failed: {e}")
            return None

    # Convenience wrappers if your bot expects existing method names
    async def download_track(self, url: str) -> Optional[str]:
        """Compatibility wrapper that downloads an audio track for `url`"""
        return await self.download(url, video=False)

    async def download_video(self, url: str) -> Optional[str]:
        """Compatibility wrapper that downloads a video for `url`"""
        return await self.download(url, video=True)


# If you used top-level helpers from the old youtube.py, we expose them for backwards compatibility
_downloader = YouTubeDownloader()


async def download_track(url: str) -> Optional[str]:
    return await _downloader.download_track(url)


async def download_video(url: str) -> Optional[str]:
    return await _downloader.download_video(url)
