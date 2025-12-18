# fallenapi.py — V2 ONLY (audio+video) with 5 retries and forced .mp3/.mp4 filenames
import asyncio
import os
import re
import uuid
from pathlib import Path
from typing import Dict, Optional, Any
from urllib.parse import urlparse

import aiofiles
import aiohttp
from aiohttp import TCPConnector

# Project imports (expected in your repo)
try:
    from anony import logger, config, app  # app = pyrogram client
except Exception:
    import logging as _logging
    logger = _logging.getLogger("fallenapi")
    logger.setLevel(_logging.INFO)
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
    class _PE:
        class FloodWait(Exception):
            value = 5
    pyrogram_errors = _PE()


# -----------------------
# STATS (import & use)
# -----------------------
DOWNLOAD_STATS: Dict[str, int] = {
    "total": 0,
    "success": 0,
    "failed": 0,

    "success_audio": 0,
    "success_video": 0,
    "failed_audio": 0,
    "failed_video": 0,

    "hard_fail_401": 0,
    "hard_fail_403": 0,
    "hard_cycle_retries": 0,

    "api_fail_other_4xx": 0,
    "api_fail_5xx": 0,

    "network_fail": 0,
    "timeout_fail": 0,

    "no_candidate": 0,
    "tg_fail": 0,
    "cdn_fail": 0,
}

def get_download_stats() -> Dict[str, int]:
    return dict(DOWNLOAD_STATS)

def reset_download_stats() -> None:
    for k in list(DOWNLOAD_STATS.keys()):
        DOWNLOAD_STATS[k] = 0

def _inc(key: str, n: int = 1) -> None:
    DOWNLOAD_STATS[key] = DOWNLOAD_STATS.get(key, 0) + n


# -----------------------
# Settings (match downloader.py idea)
# -----------------------
V2_HTTP_RETRIES = 5              # network/timeout/5xx retries per request
V2_DOWNLOAD_CYCLES = 5           # full flow retries for any failure
HARD_RETRY_WAIT = 3              # wait before retry after 401/403

JOB_POLL_ATTEMPTS = 10
JOB_POLL_INTERVAL = 2.0
JOB_POLL_BACKOFF = 1.2

NO_CANDIDATE_WAIT = 4

CDN_RETRIES = 5
CDN_RETRY_DELAY = 2

CHUNK_SIZE = 1024 * 256  # 256KB


# -----------------------
# Regex / helpers
# -----------------------
_YT_ID_RE = re.compile(r"""(?x)(?:v=|\/)([A-Za-z0-9_-]{11})|youtu\.be\/([A-Za-z0-9_-]{11})""")
_TG_RE = re.compile(r"https?://t\.me/(?:(c)/(\d+)/(\d+)|([^/]+)/(\d+))", re.IGNORECASE)

_session: Optional[aiohttp.ClientSession] = None
_session_lock = asyncio.Lock()


class V2HardAPIError(Exception):
    def __init__(self, status: int, body_preview: str = ""):
        super().__init__(f"Hard API error status={status}")
        self.status = status
        self.body_preview = body_preview[:200]


def extract_video_id(link: str) -> str:
    if not link:
        return ""
    s = link.strip()
    m = _YT_ID_RE.search(s)
    if m:
        return m.group(1) or m.group(2) or ""
    if "watch?v=" in s:
        return s.split("watch?v=")[-1].split("&")[0]
    last = s.split("/")[-1].split("?")[0]
    return last if len(last) == 11 else ""


def _looks_like_status_text(s: Optional[str]) -> bool:
    if not s:
        return False
    low = s.lower()
    return any(x in low for x in ("download started", "background", "jobstatus", "job_id", "processing", "queued", "check status"))


def _extract_candidate(obj: Any) -> Optional[str]:
    if obj is None:
        return None
    if isinstance(obj, str):
        s = obj.strip()
        return s if s else None
    if isinstance(obj, list) and obj:
        return _extract_candidate(obj[0])
    if isinstance(obj, dict):
        job = obj.get("job")
        if isinstance(job, dict):
            res = job.get("result")
            if isinstance(res, dict):
                pub = res.get("public_url")
                if isinstance(pub, str) and pub.strip():
                    return pub.strip()
                for k in ("cdnurl", "download_url", "url", "tg_link", "telegram_link", "message_link", "file_path"):
                    v = res.get(k)
                    if isinstance(v, str) and v.strip():
                        return v.strip()

        for k in ("public_url", "cdnurl", "download_url", "url", "tg_link", "telegram_link", "message_link", "file_path"):
            v = obj.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()

        for wrap in ("result", "results", "data", "items", "payload", "message", "tracks"):
            v = obj.get(wrap)
            if v:
                c = _extract_candidate(v)
                if c:
                    return c
    return None


def _normalize_candidate_to_url(api_url: str, candidate: str) -> Optional[str]:
    if not candidate:
        return None
    c = candidate.strip()
    if c.startswith(("http://", "https://")):
        return c
    if c.startswith("/"):
        # ignore local file paths
        if c.startswith("/root") or c.startswith("/home"):
            return None
        return f"{api_url.rstrip('/')}{c}"
    return f"{api_url.rstrip('/')}/{c.lstrip('/')}"


def _as_download_dir(path: Path) -> str:
    p = str(path.resolve())
    if not p.endswith(os.sep):
        p += os.sep
    return p


def _resolve_if_dir(download_result: str) -> Optional[str]:
    if not download_result:
        return None
    p = Path(download_result)
    if p.exists() and p.is_file():
        return str(p)
    if p.exists() and p.is_dir():
        files = [x for x in p.iterdir() if x.is_file()]
        if not files:
            return None
        newest = max(files, key=lambda x: x.stat().st_mtime)
        return str(newest)
    return download_result


def _force_ext(path: str, forced_ext: str) -> str:
    """
    Rename file to forced extension (NO conversion).
    """
    if not path:
        return path
    p = Path(path)
    if not p.exists() or not p.is_file():
        return path

    forced_ext = forced_ext.lstrip(".")
    target = p.with_suffix(f".{forced_ext}")

    if str(target) == str(p):
        return str(p)

    try:
        # If target already exists, keep target (cache) and remove the new file
        if target.exists():
            try:
                p.unlink(missing_ok=True)  # py3.8+ safe
            except Exception:
                pass
            return str(target)

        p.rename(target)
        return str(target)
    except Exception:
        return str(p)


async def get_http_session() -> aiohttp.ClientSession:
    global _session
    if _session and not _session.closed:
        return _session
    async with _session_lock:
        if _session and not _session.closed:
            return _session
        timeout = aiohttp.ClientTimeout(total=600, sock_connect=20, sock_read=60)
        connector = TCPConnector(limit=0, ttl_dns_cache=300, enable_cleanup_closed=True)
        _session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        return _session


# -----------------------
# V2-only client
# -----------------------
class V2OnlyClient:
    def __init__(self, download_dir: str = "downloads"):
        self.api_url = (getattr(config, "API_URL", "") or "").rstrip("/")
        self.api_key = getattr(config, "API_KEY", "") or ""

        self.download_dir = Path(download_dir).resolve()
        self.download_dir.mkdir(parents=True, exist_ok=True)

    def _headers(self) -> Dict[str, str]:
        h = {"Accept": "application/json"}
        if self.api_key:
            h["X-API-Key"] = self.api_key
        return h

    async def _v2_request_json(self, endpoint: str, params: Dict[str, Any]) -> Optional[Any]:
        """
        Retries only on network/timeout/5xx.
        Raises V2HardAPIError on 401/403 (caller retries by cycle).
        """
        if not self.api_url:
            return None

        url = f"{self.api_url}/{endpoint.lstrip('/')}"
        params = dict(params or {})
        if self.api_key and "api_key" not in params:
            params["api_key"] = self.api_key

        for attempt in range(1, V2_HTTP_RETRIES + 1):
            try:
                session = await get_http_session()
                async with session.get(url, params=params, headers=self._headers()) as resp:
                    text = await resp.text()
                    try:
                        data = await resp.json(content_type=None)
                    except Exception:
                        data = None

                    if 200 <= resp.status < 300:
                        return data

                    if resp.status in (401, 403):
                        if resp.status == 401:
                            _inc("hard_fail_401")
                        else:
                            _inc("hard_fail_403")
                        raise V2HardAPIError(resp.status, text)

                    if 500 <= resp.status <= 599:
                        _inc("api_fail_5xx")
                        # retry
                    else:
                        _inc("api_fail_other_4xx")
                        return None

            except V2HardAPIError:
                raise
            except asyncio.TimeoutError:
                _inc("timeout_fail")
            except aiohttp.ClientError:
                _inc("network_fail")
            except Exception:
                _inc("network_fail")

            if attempt < V2_HTTP_RETRIES:
                await asyncio.sleep(1)

        return None

    async def _download_from_cdn(self, cdn_url: str, out_path: str) -> Optional[str]:
        for attempt in range(1, CDN_RETRIES + 1):
            try:
                session = await get_http_session()
                async with session.get(cdn_url) as resp:
                    if resp.status != 200:
                        if resp.status in (429, 500, 502, 503, 504) and attempt < CDN_RETRIES:
                            await asyncio.sleep(CDN_RETRY_DELAY)
                            continue
                        return None

                    async with aiofiles.open(out_path, "wb") as f:
                        async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                            if not chunk:
                                break
                            await f.write(chunk)

                return out_path if os.path.exists(out_path) else None

            except asyncio.TimeoutError:
                _inc("timeout_fail")
            except aiohttp.ClientError:
                _inc("network_fail")
            except Exception:
                _inc("network_fail")

            if attempt < CDN_RETRIES:
                await asyncio.sleep(CDN_RETRY_DELAY)

        return None

    async def _download_from_telegram(self, tme_url: str) -> Optional[str]:
        if not app:
            return None

        m = _TG_RE.match(tme_url)
        if not m:
            return None

        if m.group(1):  # /c/<id>/<msg>
            channel_id = m.group(2)
            msg_id = int(m.group(3))
            chat_id = int(f"-100{channel_id}")
        else:
            chat_id = m.group(4)
            msg_id = int(m.group(5))

        try:
            dl_dir = _as_download_dir(self.download_dir)
            msg = await app.get_messages(chat_id=chat_id, message_ids=msg_id)
            res = await msg.download(file_name=dl_dir)
            fixed = _resolve_if_dir(res)
            if fixed and Path(fixed).exists():
                return fixed
            return None

        except Exception as e:
            # floodwait retry (silent)
            if hasattr(pyrogram_errors, "FloodWait") and isinstance(e, getattr(pyrogram_errors, "FloodWait")):
                wait = getattr(e, "value", 5)
                await asyncio.sleep(wait)
                return await self._download_from_telegram(tme_url)
            return None

    async def download(self, query: str, isVideo: bool = False) -> Optional[str]:
        """
        V2 download with 5 full cycles.
        Enforces output extension:
          - video => .mp4
          - audio => .mp3
        """
        forced_ext = "mp4" if isVideo else "mp3"
        vid = extract_video_id(query)
        q = vid or query

        for cycle in range(1, V2_DOWNLOAD_CYCLES + 1):
            try:
                resp = await self._v2_request_json(
                    "youtube/v2/download",
                    {"query": q, "isVideo": str(bool(isVideo)).lower()},
                )
            except V2HardAPIError:
                _inc("hard_cycle_retries")
                if cycle < V2_DOWNLOAD_CYCLES:
                    await asyncio.sleep(HARD_RETRY_WAIT)
                    continue
                return None

            if not resp:
                if cycle < V2_DOWNLOAD_CYCLES:
                    await asyncio.sleep(1)
                    continue
                return None

            candidate = _extract_candidate(resp)
            if candidate and _looks_like_status_text(candidate):
                candidate = None

            job_id = None
            if isinstance(resp, dict):
                job_id = resp.get("job_id") or resp.get("job")
                if isinstance(job_id, dict) and "id" in job_id:
                    job_id = job_id.get("id")

            if job_id and not candidate:
                interval = JOB_POLL_INTERVAL
                for _ in range(1, JOB_POLL_ATTEMPTS + 1):
                    await asyncio.sleep(interval)
                    try:
                        status = await self._v2_request_json("youtube/jobStatus", {"job_id": str(job_id)})
                    except V2HardAPIError:
                        _inc("hard_cycle_retries")
                        candidate = None
                        break

                    candidate = _extract_candidate(status) if status else None
                    if candidate and _looks_like_status_text(candidate):
                        candidate = None
                    if candidate:
                        break
                    interval *= JOB_POLL_BACKOFF

            if not candidate:
                _inc("no_candidate")
                if cycle < V2_DOWNLOAD_CYCLES:
                    await asyncio.sleep(NO_CANDIDATE_WAIT)
                    continue
                return None

            # Build stable output name
            base_name = vid if vid else uuid.uuid4().hex[:10]
            out_path = str(self.download_dir / f"{base_name}.{forced_ext}")

            # Already cached
            if os.path.exists(out_path):
                return out_path

            # Telegram candidate
            if candidate.startswith(("http://t.me", "https://t.me")):
                raw = await self._download_from_telegram(candidate)
                if not raw:
                    _inc("tg_fail")
                    if cycle < V2_DOWNLOAD_CYCLES:
                        await asyncio.sleep(2)
                        continue
                    return None

                # force extension name (no conversion)
                final = _force_ext(raw, forced_ext)
                # also ensure our stable name exists (optional rename)
                try:
                    p_final = Path(final)
                    p_target = Path(out_path)
                    if p_final.exists() and p_final.is_file() and p_final != p_target:
                        if not p_target.exists():
                            p_final.rename(p_target)
                            return str(p_target)
                except Exception:
                    pass
                return final

            # CDN candidate
            normalized = _normalize_candidate_to_url(self.api_url, candidate)
            if not normalized:
                _inc("no_candidate")
                if cycle < V2_DOWNLOAD_CYCLES:
                    await asyncio.sleep(NO_CANDIDATE_WAIT)
                    continue
                return None

            # Download to forced extension path directly
            path = await self._download_from_cdn(normalized, out_path)
            if not path:
                _inc("cdn_fail")
                if cycle < V2_DOWNLOAD_CYCLES:
                    await asyncio.sleep(2)
                    continue
                return None

            # Ensure extension is forced (already is)
            return _force_ext(path, forced_ext)

        return None


# -----------------------
# Public wrapper (minimal logs)
# -----------------------
class FallenApi:
    """
    V2 ONLY.
    Logs only final SUCCESS/FAILED.
    """
    def __init__(self, download_dir: str = "downloads"):
        self.v2 = V2OnlyClient(download_dir=download_dir)

    async def download_audio(self, query: str, title: str = "") -> Optional[str]:
        _inc("total")
        path = await self.v2.download(query, isVideo=False)
        if path and os.path.exists(path):
            _inc("success"); _inc("success_audio")
            logger.info("V2_DOWNLOAD_SUCCESS type=audio title='%s' path='%s'", title or "Unknown", path)
            return path
        _inc("failed"); _inc("failed_audio")
        logger.warning("V2_DOWNLOAD_FAILED type=audio title='%s' query='%s'", title or "Unknown", query)
        return None

    async def download_video(self, query: str, title: str = "") -> Optional[str]:
        _inc("total")
        path = await self.v2.download(query, isVideo=True)
        if path and os.path.exists(path):
            _inc("success"); _inc("success_video")
            logger.info("V2_DOWNLOAD_SUCCESS type=video title='%s' path='%s'", title or "Unknown", path)
            return path
        _inc("failed"); _inc("failed_video")
        logger.warning("V2_DOWNLOAD_FAILED type=video title='%s' query='%s'", title or "Unknown", query)
        return None

    async def download(self, query: str, isVideo: bool = False, title: str = "") -> Optional[str]:
        return await (self.download_video(query, title=title) if isVideo else self.download_audio(query, title=title))


# module-level instance (same style as many repos)
client = FallenApi()
