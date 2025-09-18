# main.py - Fixed version with proper error handling
import os
import re
import json
import time
import asyncio
import hashlib
import logging
import sqlite3
from pathlib import Path
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks, Request
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from contextlib import asynccontextmanager

import httpx

# Optional Telegram imports - handle if missing
try:
    from pyrogram import Client
    from pyrogram.types import Message
    from pyrogram.errors import FloodWait
    TELEGRAM_AVAILABLE = True
except ImportError:
    TELEGRAM_AVAILABLE = False
    Client = None

# ------------- Logging -------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("yt-proxy-api")

# ------------- Settings -------------
APP_TITLE = "YouTube Streaming Proxy + Telegram"
APP_VERSION = "2.1.1"

DB_PATH = os.environ.get("YT_TG_DB", "youtube_telegram.db")

# Telegram creds - handle empty values gracefully
TELEGRAM_API_ID = os.environ.get("TELEGRAM_API_ID", "").strip()
TELEGRAM_API_HASH = os.environ.get("TELEGRAM_API_HASH", "").strip()
TELEGRAM_SESSION = os.environ.get("STRING_SESSION", "youtube_bot")
TELEGRAM_INDEX_CHANNEL = os.environ.get("TELEGRAM_INDEX_CHANNEL", "").strip()
TELEGRAM_UPLOAD_CHANNEL = os.environ.get("TELEGRAM_UPLOAD_CHANNEL", "").strip()

# Rate limit settings
RATE_MAX = int(os.environ.get("RATE_MAX", "10000"))
RATE_WIN = int(os.environ.get("RATE_WIN", "60"))

# ------------- DB Init -------------
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS indexed_files (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      video_id TEXT UNIQUE,
      title TEXT,
      file_name TEXT,
      message_id INTEGER,
      channel_username TEXT,
      file_size INTEGER,
      duration INTEGER,
      indexed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS rate_limits (
      ip_hash TEXT PRIMARY KEY,
      request_count INTEGER,
      window_start INTEGER
    )
    """)
    conn.commit()
    conn.close()

# ------------- Rate Limiter -------------
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
            return True  # Allow on error

# ------------- Header Rotation -------------
class HeaderManager:
    def __init__(self):
        self.browser_headers = [
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive"
            },
            {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive"
            }
        ]
        self.video_headers = [
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
                "Accept": "video/webm,video/mp4,video/*;q=0.9,*/*;q=0.5",
                "Accept-Encoding": "identity",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive"
            }
        ]

    def pick_browser(self) -> Dict[str, str]:
        import random
        return random.choice(self.browser_headers).copy()

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

# ------------- Proxy Manager -------------
class ProxyManager:
    def __init__(self):
        self._proxies: List[str] = []
        self._idx = 0

    def add(self, proxy_url: str):
        self._proxies.append(proxy_url)

    def pick(self) -> Optional[str]:
        if not self._proxies:
            return None
        p = self._proxies[self._idx % len(self._proxies)]
        self._idx += 1
        return p

# ------------- YouTube API client -------------
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
            async with httpx.AsyncClient(timeout=30.0, headers=self.headers, follow_redirects=True) as client:
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

# ------------- Telegram Manager -------------
class TelegramManager:
    def __init__(self):
        self.client: Optional[Client] = None
        self.enabled = False
        self.index_channel = TELEGRAM_INDEX_CHANNEL
        self.upload_channel = TELEGRAM_UPLOAD_CHANNEL

    async def start(self):
        if not TELEGRAM_AVAILABLE:
            log.warning("Pyrogram not installed. Telegram features disabled.")
            return
            
        if not (TELEGRAM_API_ID and TELEGRAM_API_HASH):
            log.warning("Telegram credentials not set. Telegram features disabled.")
            return
            
        if not TELEGRAM_API_ID.isdigit():
            log.warning("Invalid TELEGRAM_API_ID. Must be numeric.")
            return
            
        try:
            self.client = Client(
                TELEGRAM_SESSION, 
                api_id=int(TELEGRAM_API_ID), 
                api_hash=TELEGRAM_API_HASH
            )
            await self.client.start()
            self.enabled = True
            log.info("Telegram client started successfully")
            
            if self.index_channel:
                await self.index_channel_files(self.index_channel)
        except Exception as e:
            log.error(f"Telegram startup failed: {e}")
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
            log.info(f"Indexing channel: {channel_username}")
            count = 0
            async for msg in self.client.get_chat_history(channel_username):
                await self._index_message(channel_username, msg)
                count += 1
                if count % 100 == 0:
                    log.info(f"Indexed {count} messages...")
                await asyncio.sleep(0.1)
            log.info(f"Finished indexing {count} messages from {channel_username}")
        except FloodWait as e:
            log.warning(f"FloodWait: sleep {e.value}s")
            await asyncio.sleep(e.value)
            await self.index_channel_files(channel_username)
        except Exception as e:
            log.error(f"Channel indexing error: {e}")

    async def _index_message(self, channel_username: str, msg: Message):
        try:
            media = msg.audio or msg.video or msg.document
            if not media:
                return
            file_name = getattr(media, "file_name", "") or f"file_{msg.id}"
            m = re.search(r"([A-Za-z0-9_-]{11})", file_name)
            video_id = m.group(1) if m else None
            if not video_id:
                return
                
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("""
            INSERT OR REPLACE INTO indexed_files (video_id, title, file_name, message_id, channel_username, file_size, duration)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                video_id,
                msg.caption or file_name,
                file_name,
                msg.id,
                channel_username,
                getattr(media, "file_size", 0),
                getattr(media, "duration", 0)
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            log.error(f"Message indexing error: {e}")

    async def get_link(self, video_id: str) -> Optional[str]:
        try:
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("SELECT message_id, channel_username FROM indexed_files WHERE video_id = ?", (video_id,))
            row = cur.fetchone()
            conn.close()
            if row:
                mid, ch = row
                return f"https://t.me/{ch}/{mid}"
        except Exception as e:
            log.error(f"Get link error: {e}")
        return None

    async def search(self, q: str) -> List[Dict[str, Any]]:
        try:
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("""
            SELECT video_id, title, file_name, message_id, channel_username, file_size, duration
            FROM indexed_files WHERE title LIKE ? OR file_name LIKE ?
            ORDER BY indexed_at DESC LIMIT 50
            """, (f"%{q}%", f"%{q}%"))
            rows = cur.fetchall()
            conn.close()
            return [{
                "video_id": r[0],
                "title": r[1],
                "file_name": r[2],
                "telegram_url": f"<https://t.me/{r>[4]}/{r[3]}",
                "file_size": r[5],
                "duration": r[6]
            } for r in rows]
        except Exception as e:
            log.error(f"Search error: {e}")
            return []

# ------------- Models -------------
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

# ------------- Singletons -------------
rate_limiter = RateLimiter(RATE_MAX, RATE_WIN)
headers_mgr = HeaderManager()
proxy_mgr = ProxyManager()
yt_api = YouTubeAPI()
tg_mgr = TelegramManager()

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        init_db()
        await tg_mgr.start()
        log.info("Application started successfully")
        yield
    except Exception as e:
        log.error(f"Startup error: {e}")
        yield  # Continue anyway
    finally:
        await tg_mgr.stop()

app = FastAPI(title=APP_TITLE, version=APP_VERSION, lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_methods=["*"], 
    allow_headers=["*"], 
    allow_credentials=True
)

# ------------- Middleware: Rate Limit -------------
@app.middleware("http")
async def _rl(request: Request, call_next):
    ip = request.client.host if request.client else "0.0.0.0"
    if not rate_limiter.allow(ip):
        return JSONResponse(status_code=429, content={"error": "Rate limit exceeded"})
    return await call_next(request)

# ------------- Helpers -------------
def sanitize_filename(s: str, ext: str) -> str:
    base = re.sub(r"[^\w\s.-]", "", s).strip().replace(" ", "_")[:80] or "video"
    return f"{base}.{ext}"

async def safe_proxy_stream(request: Request, upstream_url: str, ext: str, filename: Optional[str] = None, force_download: bool = False):
    """Safe streaming with proper error handling"""
    range_header = request.headers.get("range")
    vid_headers = headers_mgr.pick_video()
    if range_header:
        vid_headers["Range"] = range_header

    proxy = proxy_mgr.pick()
    client_kwargs = {
        "timeout": httpx.Timeout(connect=10.0, read=60.0),
        "headers": vid_headers,
        "follow_redirects": True
    }
    if proxy:
        client_kwargs["proxy"] = proxy

    async def safe_generator():
        try:
            async with httpx.AsyncClient(**client_kwargs) as client:
                async with client.stream("GET", upstream_url) as resp:
                    if resp.status_code not in (200, 206):
                        return  # Stop generator on error
                    async for chunk in resp.aiter_bytes(8192):
                        yield chunk
        except Exception as e:
            log.error(f"Stream error: {e}")
            # Don't raise - just stop the generator
            return

    # Get headers first
    try:
        async with httpx.AsyncClient(**client_kwargs) as client:
            head = await client.head(upstream_url, headers=vid_headers)
            status = 206 if head.status_code == 206 else 200
            
            out_headers = {"Accept-Ranges": "bytes"}
            if "Content-Length" in head.headers:
                out_headers["Content-Length"] = head.headers["Content-Length"]
            if "Content-Range" in head.headers:
                out_headers["Content-Range"] = head.headers["Content-Range"]
            
            ctype = head.headers.get("Content-Type", f"video/{ext}")
            
            if force_download:
                fname = filename or sanitize_filename("download", ext)
                out_headers.update(headers_mgr.download_headers(fname))
                
            return StreamingResponse(
                safe_generator(), 
                media_type=ctype, 
                headers=out_headers, 
                status_code=status
            )
    except Exception as e:
        log.error(f"Stream setup error: {e}")
        raise HTTPException(status_code=500, detail="Stream unavailable")

# ------------- Routes -------------
@app.get("/")
async def root():
    return {
        "name": APP_TITLE,
        "version": APP_VERSION,
        "telegram_enabled": tg_mgr.enabled,
        "endpoints": {
            "/extract": "Get video metadata (Telegram priority)",
            "/stream/{video_id}": "Stream video via server",
            "/download/{video_id}/{format_id}": "Download video via server",
            "/search": "Search Telegram files",
            "/telegram/{video_id}": "Get Telegram link"
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

    # Check Telegram first
    tglink = await tg_mgr.get_link(vid)
    if tglink:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT title, duration FROM indexed_files WHERE video_id = ?", (vid,))
        row = cur.fetchone()
        conn.close()
        title = row[0] if row else vid
        duration = row[1] if row else 0
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

    # Get from YouTube
    data = await yt_api.get_info(inp)
    data["telegram_link"] = None
    data["source"] = "youtube"
    return VideoResponse(**data)

@app.get("/stream/{video_id}")
async def stream(
    request: Request,
    video_id: str,
    quality: str = Query("720p"),
    format_type: str = Query("mp4"),
    download: bool = Query(False)
):
    # Telegram first
    link = await tg_mgr.get_link(video_id)
    if link:
        return RedirectResponse(url=link)

    data = await yt_api.get_info(video_id)
    
    # Find best format
    cands = [m for m in data.get("medias", []) 
             if quality.lower() in m.get("label", "").lower() 
             and format_type.lower() in m.get("ext", "").lower()]
    
    if not cands:
        cands = [m for m in data.get("medias", []) if quality.lower() in m.get("label", "").lower()]
    
    if not cands:
        raise HTTPException(status_code=404, detail=f"No {quality} format found")

    chosen = cands[0]
    ext = chosen.get("ext", "mp4")
    fname = sanitize_filename(f'{data.get("title","")}_{video_id}', ext)
    
    return await safe_proxy_stream(
        request, chosen["url"], ext, 
        filename=fname, force_download=bool(download)
    )

@app.get("/download/{video_id}/{format_id}")
async def download(request: Request, video_id: str, format_id: int, filename: Optional[str] = None):
    # Telegram first
    link = await tg_mgr.get_link(video_id)
    if link:
        return RedirectResponse(url=link)

    data = await yt_api.get_info(video_id)
    m = next((m for m in data.get("medias", []) if int(m.get("formatId", -1)) == format_id), None)
    if not m:
        raise HTTPException(status_code=404, detail=f"Format {format_id} not found")
    
    ext = m.get("ext", "mp4")
    fname = filename or sanitize_filename(f'{data.get("title","")}_{video_id}', ext)
    
    return await safe_proxy_stream(request, m["url"], ext, filename=fname, force_download=True)

@app.get("/telegram/{video_id}")
async def telegram_link(video_id: str):
    link = await tg_mgr.get_link(video_id)
    if not link:
        raise HTTPException(status_code=404, detail="Not found in Telegram")
    return {"video_id": video_id, "telegram_url": link}

@app.get("/search", response_model=SearchResponse)
async def search(q: str = Query(..., description="Search query")):
    rows = await tg_mgr.search(q)
    return SearchResponse(results=rows, total=len(rows))

@app.post("/proxy/add")
async def proxy_add(proxy_url: str):
    proxy_mgr.add(proxy_url)
    return {"added": proxy_url}

@app.get("/proxy/status")
async def proxy_status():
    return {
        "count": len(proxy_mgr._proxies), 
        "current_index": proxy_mgr._idx,
        "telegram_enabled": tg_mgr.enabled
    }

@app.get("/stats")
async def stats():
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM indexed_files")
        total_files = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM rate_limits")
        total_ips = cur.fetchone()[0]
        conn.close()
        return {
            "indexed_files": total_files, 
            "unique_ips": total_ips, 
            "telegram_enabled": tg_mgr.enabled,
            "version": APP_VERSION
        }
    except Exception as e:
        return {"error": str(e), "version": APP_VERSION}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="72.60.108.192", port=int(os.environ.get("PORT", "8000")), reload=False)
