# main.py — FastAPI YouTube proxy + Telegram (user session) with persistent index.json
import os
import re
import json
import time
import asyncio
import hashlib
import logging
import sqlite3
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

# Load .env early so env vars are available to os.environ
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import httpx

# Telegram (user session only; no bot token)
try:
    from pyrogram import Client
    from pyrogram.types import Message
    from pyrogram.errors import FloodWait
    TELEGRAM_AVAILABLE = True
except Exception:
    TELEGRAM_AVAILABLE = False
    Client = None
    Message = None
    FloodWait = Exception

# ---------------- Logging ----------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("yt-proxy-api")

# ---------------- Settings ----------------
APP_TITLE = "YouTube Streaming Proxy + Telegram"
APP_VERSION = "2.3.0"

# Rate-limit DB (SQLite only for rate limits)
DB_PATH = os.environ.get("YT_TG_DB", "youtube_rate.db")

# Persistent JSON index
INDEX_JSON_PATH = os.environ.get("INDEX_JSON_PATH", "index.json")

# Telegram user-session env
TELEGRAM_API_ID = os.environ.get("TELEGRAM_API_ID", "").strip()
TELEGRAM_API_HASH = os.environ.get("TELEGRAM_API_HASH", "").strip()
TELEGRAM_SESSION_STRING = os.environ.get("STRING_SESSION", "").strip()  # exported string session
TELEGRAM_SESSION = os.environ.get("TELEGRAM_SESSION", "youtube_user")            # file-name label
TELEGRAM_INDEX_CHANNEL = os.environ.get("TELEGRAM_INDEX_CHANNEL", "").strip()    # e.g. 
TELEGRAM_UPLOAD_CHANNEL = os.environ.get("TELEGRAM_UPLOAD_CHANNEL", "").strip()  # optional, unused here

# Rate limit settings
RATE_MAX = int(os.environ.get("RATE_MAX", "10000"))
RATE_WIN = int(os.environ.get("RATE_WIN", "60"))

# ---------------- DB Init (rate-limits only) ----------------
def init_db():
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS rate_limits (
          ip_hash TEXT PRIMARY KEY,
          request_count INTEGER,
          window_start INTEGER
        )
        """)
        conn.commit()
        conn.close()
    except Exception as e:
        log.error(f"DB init error: {e}")

# ---------------- Rate Limiter ----------------
class RateLimiter:
    def __init__(self, max_requests: int, window_seconds: int):
        self.max_requests = max_requests
        self.window_seconds = window_seconds

    def allow(self, client_ip: str) -> bool:
        try:
            ip_hash = hashlib.md5(client_ip.encode()).hexdigest()
            now = int(time.time())
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("SELECT request_count, window_start FROM rate_limits WHERE ip_hash = ?", (ip_hash,))
            row = cur.fetchone()
            if not row:
                cur.execute("INSERT INTO rate_limits (ip_hash, request_count, window_start) VALUES (?, 1, ?)", (ip_hash, now))
                conn.commit()
                conn.close()
                return True
            cnt, start = row
            if now - start >= self.window_seconds:
                cur.execute("UPDATE rate_limits SET request_count = 1, window_start = ? WHERE ip_hash = ?", (now, ip_hash))
                conn.commit()
                conn.close()
                return True
            if cnt >= self.max_requests:
                conn.close()
                return False
            cur.execute("UPDATE rate_limits SET request_count = request_count + 1 WHERE ip_hash = ?", (ip_hash,))
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            log.error(f"Rate limit error: {e}")
            return True

# ---------------- Persistent Index (index.json) ----------------
class IndexStore:
    def __init__(self, path: str = "index.json"):
        self.path = path
        self.data: Dict[str, Any] = {"channels": {}, "files": {}}
        self._load()

    def _load(self):
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                self.data = json.load(f)
        except FileNotFoundError:
            self.data = {"channels": {}, "files": {}}
        except Exception as e:
            log.error(f"index.json load failed: {e}")

    def _atomic_save(self):
        tmp = self.path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2, sort_keys=True)
            os.replace(tmp, self.path)  # atomic replacement
        except Exception as e:
            log.error(f"index.json save failed: {e}")

    def last_msg_id(self, channel: str) -> int:
        return int(self.data.get("channels", {}).get(channel, {}).get("last_msg_id", 0))

    def set_last_msg_id(self, channel: str, msg_id: int):
        ch = self.data.setdefault("channels", {}).setdefault(channel, {})
        if msg_id > int(ch.get("last_msg_id", 0)):
            ch["last_msg_id"] = int(msg_id)
            self._atomic_save()

    def upsert_from_message(self, channel: str, message: Any) -> bool:
        media = getattr(message, "audio", None) or getattr(message, "video", None) or getattr(message, "document", None)
        if not media:
            return False
        file_name = getattr(media, "file_name", "") or f"file_{message.id}"
        m = re.search(r"([A-Za-z0-9_-]{11})", file_name)
        video_id = m.group(1) if m else None
        if not video_id:
            return False
        entry = {
            "video_id": video_id,
            "title": message.caption or file_name,
            "file_name": file_name,
            "message_id": int(message.id),
            "channel_username": channel,
            "file_size": int(getattr(media, "file_size", 0) or 0),
            "duration": int(getattr(media, "duration", 0) or 0),
        }
        self.data.setdefault("files", {})[video_id] = entry
        self.set_last_msg_id(channel, int(message.id))
        self._atomic_save()
        return True

    def get_link(self, video_id: str) -> Optional[str]:
        v = self.data.get("files", {}).get(video_id)
        if v:
            return f"https://t.me/{v['channel_username']}/{v['message_id']}"
        return None

    def get_meta(self, video_id: str) -> Optional[Dict[str, Any]]:
        return self.data.get("files", {}).get(video_id)

    def search(self, q: str) -> List[Dict[str, Any]]:
        ql = q.lower()
        out: List[Dict[str, Any]] = []
        for v in self.data.get("files", {}).values():
            if ql in v.get("title", "").lower() or ql in v.get("file_name", "").lower():
                out.append({
                    "video_id": v["video_id"],
                    "title": v["title"],
                    "file_name": v["file_name"],
                    "telegram_url": f"https://t.me/{v['channel_username']}/{v['message_id']}",
                    "file_size": v["file_size"],
                    "duration": v["duration"]
                })
        # newest first by message id
        out.sort(key=lambda r: int(r["telegram_url"].split("/")[-1]), reverse=True)
        return out

index_store = IndexStore(INDEX_JSON_PATH)

# ---------------- Headers & Proxies ----------------
class HeaderManager:
    def __init__(self):
        self.video_headers = [
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
                "Accept": "video/webm,video/mp4,video/*;q=0.9,*/*;q=0.5",
                "Accept-Encoding": "identity",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive"
            }
        ]

    def pick_video(self) -> Dict[str, str]:
        import random
        return random.choice(self.video_headers).copy()

    @staticmethod
    def download_headers(filename: str) -> Dict[str, str]:
        return {
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache"
        }

class ProxyManager:
    def __init__(self):
        self._proxies: List[str] = []
        self._idx = 0

    def add(self, proxy_url: str):
        self._proxies.append(proxy_url)
        log.info(f"Added proxy: {proxy_url}")

    def pick(self) -> Optional[str]:
        if not self._proxies:
            return None
        p = self._proxies[self._idx % len(self._proxies)]
        self._idx += 1
        return p

# ---------------- YouTube API Client ----------------
class YouTubeAPI:
    def __init__(self):
        self.api_url = "https://utdqxiuahh.execute-api.ap-south-1.amazonaws.com/pro/fetch"
        self.headers = {
            "x-api-key": "fAtAyM17qm9pYmsaPlkAT8tRrDoHICBb2NnxcBPM",
            "User-Agent": "okhttp/4.12.0",
            "Accept-Encoding": "gzip"
        }

    @staticmethod
    def extract_video_id(inp: str) -> Optional[str]:
        if not inp:
            return None
        pats = [
            r"(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/embed/|youtube\.com/v/)([^&\n?#]+)",
            r"music\.youtube\.com/watch\?v=([^&\n?#]+)"
        ]
        for p in pats:
            m = re.search(p, inp)
            if m:
                return m.group(1)
        if re.fullmatch(r"[A-Za-z0-9_-]{11}", inp):
            return inp
        return None

    async def get_info(self, video_or_url: str) -> Dict[str, Any]:
        vid = self.extract_video_id(video_or_url)
        if not vid:
            raise HTTPException(status_code=400, detail="Invalid YouTube URL or video_id")
        yt_url = f"https://music.youtube.com/watch?v={vid}"
        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(30.0),
                headers=self.headers,
                follow_redirects=True
            ) as client:
                r = await client.get(self.api_url, params={"url": yt_url, "user_id": "h2"})
                if r.status_code != 200:
                    raise HTTPException(status_code=r.status_code, detail="YouTube API error")
                data = r.json()
                data["video_id"] = vid
                return data
        except httpx.TimeoutException:
            raise HTTPException(status_code=504, detail="YouTube API timeout")
        except Exception as e:
            log.error(f"YouTube API error: {e}")
            raise HTTPException(status_code=500, detail="YouTube API unavailable")

# ---------------- Telegram (User Session) ----------------
class TelegramManager:
    def __init__(self):
        self.client: Optional[Client] = None
        self.enabled = False
        self.index_channel = TELEGRAM_INDEX_CHANNEL

    async def start(self):
        if not TELEGRAM_AVAILABLE:
            log.warning("Pyrogram not installed; Telegram disabled.")
            return
        if not (TELEGRAM_API_ID and TELEGRAM_API_HASH):
            log.warning("Telegram API creds missing; Telegram disabled.")
            return
        try:
            api_id_val = int(TELEGRAM_API_ID) if TELEGRAM_API_ID.isdigit() else TELEGRAM_API_ID
            kwargs = {"name": TELEGRAM_SESSION, "api_id": api_id_val, "api_hash": TELEGRAM_API_HASH}
            if TELEGRAM_SESSION_STRING:
                kwargs["session_string"] = TELEGRAM_SESSION_STRING  # user session via string
            self.client = Client(**kwargs)  # user mode; no bot_token
            await self.client.start()
            self.enabled = True
            log.info("Telegram user session started")
            if self.index_channel:
                await self.index_channel_files(self.index_channel)
        except Exception as e:
            log.error(f"Telegram start failed: {e}")
            self.enabled = False

    async def stop(self):
        if self.client:
            try:
                await self.client.stop()
            except Exception as e:
                log.error(f"Telegram stop error: {e}")

    async def index_channel_files(self, channel_username: str):
        if not (self.enabled and self.client):
            return
        try:
            last_id = index_store.last_msg_id(channel_username)
            processed = 0
            # Newest first; stop when reaching last indexed
            async for msg in self.client.get_chat_history(channel_username):
                if last_id and int(msg.id) <= last_id:
                    break
                index_store.upsert_from_message(channel_username, msg)
                processed += 1
                if processed % 100 == 0:
                    log.info(f"Indexed {processed} new messages in {channel_username}")
                await asyncio.sleep(0.05)
            if processed:
                log.info(f"Indexed {processed} new messages in {channel_username}")
        except FloodWait as e:
            log.warning(f"FloodWait {e.value}s; retrying")
            await asyncio.sleep(e.value)
            await self.index_channel_files(channel_username)
        except Exception as e:
            log.error(f"Incremental indexing error: {e}")

    async def get_link(self, video_id: str) -> Optional[str]:
        return index_store.get_link(video_id)

    async def search(self, q: str) -> List[Dict[str, Any]]:
        return index_store.search(q)

# ---------------- Models ----------------
class VideoResponse(BaseModel):
    statusCode: int
    url: str
    title: str
    thumbnail: str
    duration: int
    video_id: str
    telegram_link: Optional[str] = None
    medias: List[Dict[str, Any]]
    source: Optional[str] = None

class SearchResponse(BaseModel):
    results: List[Dict[str, Any]]
    total: int

# ---------------- Utilities ----------------
rate_limiter = RateLimiter(RATE_MAX, RATE_WIN)
headers_mgr = HeaderManager()
proxy_mgr = ProxyManager()
yt_api = YouTubeAPI()
tg_mgr = TelegramManager()

def sanitize_filename(s: str, ext: str) -> str:
    base = re.sub(r"[^\w\s.-]", "", s).strip().replace(" ", "_")[:80] or "video"
    return f"{base}.{ext}"

def get_media_type(ext: str) -> str:
    mime = {
        "mp4": "video/mp4",
        "webm": "video/webm",
        "m4a": "audio/mp4",
        "mp3": "audio/mpeg",
        "aac": "audio/aac",
        "ogg": "audio/ogg",
    }
    return mime.get(ext.lower(), "application/octet-stream")

async def proxy_stream_media(
    request: Request,
    upstream_url: str,
    ext: str,
    filename: Optional[str] = None,
    force_download: bool = False
):
    # Forward Range for seeking
    range_header = request.headers.get("range")
    vid_headers = headers_mgr.pick_video()
    if range_header:
        vid_headers["Range"] = range_header

    proxy = proxy_mgr.pick()
    client_kwargs = {
        "timeout": httpx.Timeout(60.0),  # default applies to connect/read/write/pool
        "headers": vid_headers,
        "follow_redirects": True
    }
    if proxy:
        client_kwargs["proxy"] = proxy

    client = httpx.AsyncClient(**client_kwargs)
    try:
        resp = await client.stream("GET", upstream_url)
        if resp.status_code not in (200, 206):
            await resp.aclose()
            await client.aclose()
            raise HTTPException(status_code=resp.status_code, detail="Upstream unavailable")

        # Propagate headers
        out_headers = {"Accept-Ranges": "bytes"}
        if "Content-Length" in resp.headers:
            out_headers["Content-Length"] = resp.headers["Content-Length"]
        if "Content-Range" in resp.headers:
            out_headers["Content-Range"] = resp.headers["Content-Range"]
        status_code = 206 if resp.status_code == 206 else 200

        media_type = resp.headers.get("Content-Type", get_media_type(ext))
        if force_download:
            fname = filename or sanitize_filename("download", ext)
            out_headers.update(headers_mgr.download_headers(fname))
            media_type = "application/octet-stream"

        async def gen():
            try:
                async for chunk in resp.aiter_bytes(8192):
                    yield chunk
            finally:
                await resp.aclose()
                await client.aclose()

        return StreamingResponse(gen(), media_type=media_type, headers=out_headers, status_code=status_code)
    except Exception as e:
        try:
            await client.aclose()
        except Exception:
            pass
        log.error(f"Stream error: {e}")
        raise HTTPException(status_code=500, detail="Media stream unavailable")

# ---------------- Lifespan ----------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    await tg_mgr.start()
    yield
    await tg_mgr.stop()

app = FastAPI(title=APP_TITLE, version=APP_VERSION, lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"], allow_credentials=True
)

# ---------------- Middleware ----------------
@app.middleware("http")
async def rate_limit_mw(request: Request, call_next):
    ip = request.client.host if request.client else "0.0.0.0"
    if not rate_limiter.allow(ip):
        return JSONResponse(status_code=429, content={"error": "Rate limit exceeded"})
    return await call_next(request)

# ---------------- Endpoints ----------------
@app.get("/")
async def root():
    return {
        "name": APP_TITLE,
        "version": APP_VERSION,
        "telegram_enabled": tg_mgr.enabled,
        "index_file": INDEX_JSON_PATH,
        "endpoints": {
            "/extract": "Get video info (Telegram first)",
            "/formats": "List formats",
            "/stream/{video_id}": "Stream via this server",
            "/download/{video_id}/{format_id}": "Force download via this server",
            "/telegram/{video_id}": "Get Telegram link if indexed",
            "/search": "Search indexed Telegram files",
            "/index-new": "Index only new Telegram messages",
            "/proxy/add": "Add proxy",
            "/stats": "Stats"
        }
    }

@app.get("/extract", response_model=VideoResponse)
async def extract(url: Optional[str] = None, video_id: Optional[str] = None):
    inp = url or video_id
    if not inp:
        raise HTTPException(status_code=400, detail="Provide url or video_id")
    vid = yt_api.extract_video_id(inp)
    if not vid:
        raise HTTPException(status_code=400, detail="Invalid YouTube URL or video_id")

    tglink = await tg_mgr.get_link(vid)
    if tglink:
        meta = index_store.get_meta(vid)
        title = meta["title"] if meta else vid
        duration = int(meta["duration"]) if meta else 0
        return VideoResponse(
            statusCode=200,
            url=f"https://youtube.com/watch?v={vid}",
            title=title,
            thumbnail=f"https://i.ytimg.com/vi/{vid}/hq720.jpg",
            duration=duration or 0,
            video_id=vid,
            telegram_link=tglink,
            medias=[],
            source="telegram"
        )

    data = await yt_api.get_info(inp)
    data["telegram_link"] = None
    data["source"] = "youtube"
    return VideoResponse(**data)

@app.get("/formats")
async def get_formats(url: Optional[str] = None, video_id: Optional[str] = None):
    inp = url or video_id
    if not inp:
        raise HTTPException(status_code=400, detail="Provide url or video_id")
    data = await yt_api.get_info(inp)
    org = {"video": {}, "audio": [], "combined": []}
    for m in data.get("medias", []):
        info = {
            "formatId": m.get("formatId"),
            "ext": m.get("ext"),
            "quality": m.get("quality"),
            "bitrate": m.get("bitrate"),
            "stream_url": f"/stream/{data['video_id']}?format_id={m.get('formatId')}",
            "download_url": f"/download/{data['video_id']}/{m.get('formatId')}"
        }
        if m.get("is_audio"):
            org["audio"].append(info)
        elif m.get("type") == "video":
            label = m.get("label", "unknown")
            org["video"].setdefault(label, []).append(info)
        else:
            org["combined"].append(info)
    return {
        "video_id": data.get("video_id"),
        "title": data.get("title"),
        "telegram_link": await tg_mgr.get_link(data.get("video_id")),
        "formats": org
    }

@app.get("/stream/{video_id}")
async def stream_video(
    request: Request,
    video_id: str,
    quality: str = Query("720p"),
    format_type: str = Query("mp4"),
    format_id: Optional[int] = Query(None),
    download: bool = Query(False)
):
    tglink = await tg_mgr.get_link(video_id)
    if tglink:
        return RedirectResponse(url=tglink)

    data = await yt_api.get_info(video_id)
    medias = data.get("medias", [])
    if format_id is not None:
        chosen = next((m for m in medias if m.get("formatId") == format_id), None)
    else:
        cands = [
            m for m in medias
            if quality.lower() in m.get("label", "").lower()
            and format_type.lower() in m.get("ext", "").lower()
        ]
        if not cands:
            cands = [m for m in medias if quality.lower() in m.get("label", "").lower()]
        chosen = cands[0] if cands else None

    if not chosen:
        raise HTTPException(status_code=404, detail=f"No suitable format found for {quality} {format_type}")

    ext = chosen.get("ext", "mp4")
    name = sanitize_filename(f"{data.get('title', video_id)}_{video_id}", ext)
    return await proxy_stream_media(request, chosen["url"], ext, name, force_download=bool(download))

@app.get("/download/{video_id}/{format_id}")
async def download_video(
    request: Request,
    video_id: str,
    format_id: int,
    filename: Optional[str] = Query(None)
):
    tglink = await tg_mgr.get_link(video_id)
    if tglink:
        return RedirectResponse(url=tglink)

    data = await yt_api.get_info(video_id)
    chosen = next((m for m in data.get("medias", []) if m.get("formatId") == format_id), None)
    if not chosen:
        raise HTTPException(status_code=404, detail=f"Format ID {format_id} not found")
    ext = chosen.get("ext", "mp4")
    name = filename or sanitize_filename(f"{data.get('title', video_id)}_{video_id}", ext)
    return await proxy_stream_media(request, chosen["url"], ext, name, force_download=True)

@app.get("/telegram/{video_id}")
async def telegram_link(video_id: str):
    link = await tg_mgr.get_link(video_id)
    if not link:
        raise HTTPException(status_code=404, detail="Not found in Telegram index")
    return {"video_id": video_id, "telegram_url": link}

@app.get("/search", response_model=SearchResponse)
async def search(q: str = Query(..., description="Search query")):
    rows = await tg_mgr.search(q)
    return SearchResponse(results=rows, total=len(rows))

@app.post("/index-new")
async def index_new(channel: Optional[str] = Query(None, description="Channel username")):
    if not tg_mgr.enabled:
        raise HTTPException(status_code=503, detail="Telegram disabled")
    ch = channel or tg_mgr.index_channel
    if not ch:
        raise HTTPException(status_code=400, detail="No channel configured")
    await tg_mgr.index_channel_files(ch)
    return {
        "channel": ch,
        "last_msg_id": index_store.last_msg_id(ch),
        "total_indexed": len(index_store.data.get("files", {}))
    }

@app.post("/proxy/add")
async def add_proxy(proxy_url: str = Query(..., description="http://... or socks5://...")):
    proxy_mgr.add(proxy_url)
    return {"message": f"Added proxy: {proxy_url}", "count": len(proxy_mgr._proxies)}

@app.get("/proxy/status")
async def proxy_status():
    return {"proxies": len(proxy_mgr._proxies), "current_index": proxy_mgr._idx}

@app.get("/stats")
async def stats():
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM rate_limits")
        total_ips = cur.fetchone()[0]
        conn.close()
    except Exception:
        total_ips = 0
    return {
        "version": APP_VERSION,
        "telegram_enabled": tg_mgr.enabled,
        "indexed_files": len(index_store.data.get("files", {})),
        "channels": list(index_store.data.get("channels", {}).keys()),
        "rate_limit": f"{RATE_MAX}/{RATE_WIN}s",
        "unique_ips": total_ips
    }

@app.exception_handler(404)
async def not_found_handler(request, exc):
    return JSONResponse(status_code=404, content={"error": "Endpoint not found"})

@app.exception_handler(500)
async def internal_error_handler(request, exc):
    log.error(f"Internal error: {exc}")
    return JSONResponse(status_code=500, content={"error": "Internal server error"})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", "8000")), reload=False)
