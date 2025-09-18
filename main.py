# main.py - Complete Fixed Version
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

# Load environment variables first
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks, Request
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from contextlib import asynccontextmanager

import httpx

# Optional Telegram imports
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
APP_VERSION = "2.1.2"

DB_PATH = os.environ.get("YT_TG_DB", "youtube_telegram.db")

# Telegram settings
TELEGRAM_API_ID = os.environ.get("TELEGRAM_API_ID", "").strip()
TELEGRAM_API_HASH = os.environ.get("TELEGRAM_API_HASH", "").strip()
TELEGRAM_SESSION_STRING = os.environ.get("STRING_SESSION", "").strip()
TELEGRAM_SESSION = os.environ.get("TELEGRAM_SESSION", "youtube_api")
TELEGRAM_INDEX_CHANNEL = os.environ.get("TELEGRAM_INDEX_CHANNEL", "").strip()
TELEGRAM_UPLOAD_CHANNEL = os.environ.get("TELEGRAM_UPLOAD_CHANNEL", "").strip()

# Rate limit settings
RATE_MAX = int(os.environ.get("RATE_MAX", "10000"))
RATE_WIN = int(os.environ.get("RATE_WIN", "60"))

# ------------- DB Init -------------
def init_db():
    try:
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
        log.info("Database initialized successfully")
    except Exception as e:
        log.error(f"Database init error: {e}")

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
            return True

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
        log.info(f"Added proxy: {proxy_url}")

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
        patterns = [
            r"(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/embed/|youtube\.com/v/)([^&\n?#]+)",
            r"music\.youtube\.com/watch\?v=([^&\n?#]+)"
        ]
        for pattern in patterns:
            match = re.search(pattern, inp)
            if match:
                return match.group(1)
        # Check if it's already a video ID
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
                response = await client.get(self.api_url, params={"url": yt_url, "user_id": "h2"})
                if response.status_code != 200:
                    raise HTTPException(status_code=response.status_code, detail="YouTube API error")
                data = response.json()
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
            log.info("Set TELEGRAM_API_ID and TELEGRAM_API_HASH environment variables.")
            return
            
        try:
            # Handle API ID
            api_id_val = int(TELEGRAM_API_ID) if TELEGRAM_API_ID.isdigit() else TELEGRAM_API_ID
            
            # Build client arguments
            client_kwargs = {
                "name": TELEGRAM_SESSION,
                "api_id": api_id_val,
                "api_hash": TELEGRAM_API_HASH
            }
            
            # Use session string if provided
            if TELEGRAM_SESSION_STRING:
                client_kwargs["session_string"] = TELEGRAM_SESSION_STRING
                log.info("Using Telegram session string")
            else:
                log.info("Using Telegram session file")
            
            self.client = Client(**client_kwargs)
            await self.client.start()
            self.enabled = True
            log.info("Telegram client started successfully")
            
            # Index channel if specified
            if self.index_channel:
                log.info(f"Starting to index channel: {self.index_channel}")
                await self.index_channel_files(self.index_channel)
                
        except Exception as e:
            log.error(f"Telegram startup failed: {e}")
            self.enabled = False

    async def stop(self):
        if self.client:
            try:
                await self.client.stop()
                log.info("Telegram client stopped")
            except Exception as e:
                log.error(f"Telegram stop error: {e}")

    async def index_channel_files(self, channel_username: str):
        if not (self.enabled and self.client):
            return
        try:
            count = 0
            async for msg in self.client.get_chat_history(channel_username):
                await self._index_message(channel_username, msg)
                count += 1
                if count % 100 == 0:
                    log.info(f"Indexed {count} messages from {channel_username}...")
                await asyncio.sleep(0.1)
            log.info(f"Finished indexing {count} messages from {channel_username}")
        except FloodWait as e:
            log.warning(f"FloodWait: sleeping {e.value}s")
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
            match = re.search(r"([A-Za-z0-9_-]{11})", file_name)
            video_id = match.group(1) if match else None
            
            if not video_id:
                return
                
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("""
            INSERT OR REPLACE INTO indexed_files 
            (video_id, title, file_name, message_id, channel_username, file_size, duration)
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
        yield
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

# ------------- Middleware -------------
@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    ip = request.client.host if request.client else "0.0.0.0"
    if not rate_limiter.allow(ip):
        return JSONResponse(status_code=429, content={"error": "Rate limit exceeded"})
    return await call_next(request)

# ------------- Helpers -------------
def sanitize_filename(s: str, ext: str) -> str:
    base = re.sub(r"[^\w\s.-]", "", s).strip().replace(" ", "_")[:80] or "video"
    return f"{base}.{ext}"

def get_media_type(ext: str) -> str:
    """Get proper MIME type for media files"""
    mime_types = {
        'mp4': 'video/mp4',
        'webm': 'video/webm',
        'm4a': 'audio/mp4',
        'mp3': 'audio/mpeg',
        'aac': 'audio/aac',
        'ogg': 'audio/ogg'
    }
    return mime_types.get(ext.lower(), 'application/octet-stream')

async def proxy_stream_media(
    request: Request, 
    upstream_url: str, 
    ext: str, 
    filename: Optional[str] = None, 
    force_download: bool = False
):
    """
    Proxy media stream from YouTube through this server
    - Streaming: Browser plays the media directly (video/webm, audio/mp4, etc.)
    - Download: Browser downloads the file (Content-Disposition: attachment)
    """
    
    # Get client range header for seeking support
    range_header = request.headers.get("range")
    vid_headers = headers_mgr.pick_video()
    if range_header:
        vid_headers["Range"] = range_header

    # Get proxy if available
    proxy = proxy_mgr.pick()
    
    # FIXED: Proper httpx.Timeout configuration
    client_kwargs = {
        "timeout": httpx.Timeout(timeout=60.0),  # Set default timeout
        "headers": vid_headers,
        "follow_redirects": True
    }
    
    if proxy:
        client_kwargs["proxy"] = proxy
        log.info(f"Using proxy: {proxy}")

    async def stream_generator():
        """Safe generator that doesn't raise exceptions after streaming starts"""
        try:
            async with httpx.AsyncClient(**client_kwargs) as client:
                async with client.stream("GET", upstream_url) as response:
                    if response.status_code not in (200, 206):
                        log.error(f"Upstream error: {response.status_code}")
                        return
                    
                    async for chunk in response.aiter_bytes(chunk_size=8192):
                        yield chunk
        except Exception as e:
            log.error(f"Stream error: {e}")
            return

    try:
        # Get response info first
        async with httpx.AsyncClient(**client_kwargs) as client:
            head_response = await client.head(upstream_url, headers=vid_headers)
            
            # Build response headers
            response_headers = {"Accept-Ranges": "bytes"}
            
            # Copy relevant headers from upstream
            if "Content-Length" in head_response.headers:
                response_headers["Content-Length"] = head_response.headers["Content-Length"]
            if "Content-Range" in head_response.headers:
                response_headers["Content-Range"] = head_response.headers["Content-Range"]
            
            # Determine status code
            status_code = 206 if head_response.status_code == 206 else 200
            
            # Set media type
            media_type = get_media_type(ext)
            
            # Handle download vs streaming
            if force_download:
                fname = filename or sanitize_filename("download", ext)
                response_headers.update(headers_mgr.download_headers(fname))
                media_type = "application/octet-stream"  # Force download
                log.info(f"Serving download: {fname}")
            else:
                log.info(f"Streaming {media_type}: {ext}")
            
            return StreamingResponse(
                stream_generator(),
                media_type=media_type,
                headers=response_headers,
                status_code=status_code
            )
            
    except Exception as e:
        log.error(f"Stream setup error: {e}")
        raise HTTPException(status_code=500, detail="Media stream unavailable")

# ------------- Routes -------------
@app.get("/")
async def root():
    return {
        "name": APP_TITLE,
        "version": APP_VERSION,
        "telegram_enabled": tg_mgr.enabled,
        "endpoints": {
            "/extract": "Get video info (Telegram link priority)",
            "/stream/{video_id}": "🎥 Stream video via this server (browser plays)",
            "/download/{video_id}/{format_id}": "📥 Download file via this server (forces save)",
            "/search": "🔍 Search indexed Telegram files",
            "/telegram/{video_id}": "📱 Get Telegram link if available",
            "/formats": "📋 List all available formats",
            "/proxy/add": "🔧 Add HTTP/SOCKS5 proxy",
            "/stats": "📊 API statistics"
        },
        "behavior": {
            "streaming": "Serves media for browser playback (MP4, WebM, M4A, MP3)",
            "download": "Forces file download with proper filename",
            "telegram_priority": "Returns Telegram link if video exists in indexed channel"
        }
    }

@app.get("/extract", response_model=VideoResponse)
async def extract(url: Optional[str] = None, video_id: Optional[str] = None):
    """Extract video information - Returns Telegram link if available, otherwise YouTube data"""
    inp = url or video_id
    if not inp:
        raise HTTPException(status_code=400, detail="Provide url or video_id parameter")
    
    vid = yt_api.extract_video_id(inp)
    if not vid:
        raise HTTPException(status_code=400, detail="Invalid YouTube URL or video_id")

    # 🔥 Priority 1: Check Telegram first
    telegram_link = await tg_mgr.get_link(vid)
    if telegram_link:
        # Return Telegram-based response
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT title, duration FROM indexed_files WHERE video_id = ?", (vid,))
        row = cur.fetchone()
        conn.close()
        
        title = row[0] if row else f"Video {vid}"
        duration = row[1] if row else 0
        
        return VideoResponse(
            statusCode=200,
            url=f"https://youtube.com/watch?v={vid}",
            title=title,
            thumbnail=f"https://i.ytimg.com/vi/{vid}/hq720.jpg",
            duration=duration or 0,
            video_id=vid,
            telegram_link=telegram_link,
            medias=[],
            source="telegram"
        )

    # 🎵 Priority 2: Get from YouTube API
    data = await yt_api.get_info(inp)
    data["telegram_link"] = None
    data["source"] = "youtube"
    
    return VideoResponse(**data)

@app.get("/formats")
async def get_formats(url: Optional[str] = None, video_id: Optional[str] = None):
    """List all available formats organized by type and quality"""
    inp = url or video_id
    if not inp:
        raise HTTPException(status_code=400, detail="Provide url or video_id parameter")
    
    data = await yt_api.get_info(inp)
    
    # Organize formats
    organized = {
        "video": {},
        "audio": [],
        "combined": []
    }
    
    for media in data.get("medias", []):
        format_info = {
            "formatId": media.get("formatId"),
            "ext": media.get("ext"),
            "quality": media.get("quality"),
            "bitrate": media.get("bitrate"),
            "size": media.get("file_size"),
            "stream_url": f"/stream/{data['video_id']}?format_id={media.get('formatId')}",
            "download_url": f"/download/{data['video_id']}/{media.get('formatId')}"
        }
        
        if media.get("is_audio"):
            organized["audio"].append(format_info)
        elif media.get("type") == "video" and not media.get("is_audio"):
            quality = media.get("label", "unknown")
            if quality not in organized["video"]:
                organized["video"][quality] = []
            organized["video"][quality].append(format_info)
        else:
            organized["combined"].append(format_info)
    
    return {
        "video_id": data.get("video_id"),
        "title": data.get("title"),
        "telegram_link": await tg_mgr.get_link(data.get("video_id")),
        "formats": organized
    }

@app.get("/stream/{video_id}")
async def stream_video(
    request: Request,
    video_id: str,
    quality: str = Query("720p", description="Video quality (240p, 360p, 480p, 720p, 1080p)"),
    format_type: str = Query("mp4", description="Format (mp4, webm, m4a)"),
    format_id: Optional[int] = Query(None, description="Specific format ID"),
    download: bool = Query(False, description="Force download instead of streaming")
):
    """
    🎥 Stream video through this server
    - Returns: Video/audio stream for browser playback
    - Supports: Range requests for seeking
    - Priority: Telegram link > YouTube stream
    """
    
    # Check Telegram first
    telegram_link = await tg_mgr.get_link(video_id)
    if telegram_link:
        return RedirectResponse(url=telegram_link)

    # Get video data
    data = await yt_api.get_info(video_id)
    medias = data.get("medias", [])
    
    # Find matching format
    if format_id:
        # Specific format requested
        chosen = next((m for m in medias if m.get("formatId") == format_id), None)
    else:
        # Quality-based selection
        candidates = [
            m for m in medias 
            if quality.lower() in m.get("label", "").lower() 
            and format_type.lower() in m.get("ext", "").lower()
        ]
        if not candidates:
            # Fallback to quality only
            candidates = [m for m in medias if quality.lower() in m.get("label", "").lower()]
        chosen = candidates[0] if candidates else None
    
    if not chosen:
        raise HTTPException(status_code=404, detail=f"No suitable format found for {quality} {format_type}")

    # Stream the media
    ext = chosen.get("ext", "mp4")
    title = data.get("title", video_id)
    filename = sanitize_filename(f"{title}_{video_id}", ext)
    
    return await proxy_stream_media(
        request=request,
        upstream_url=chosen["url"],
        ext=ext,
        filename=filename,
        force_download=download
    )

@app.get("/download/{video_id}/{format_id}")
async def download_video(
    request: Request, 
    video_id: str, 
    format_id: int,
    filename: Optional[str] = Query(None, description="Custom filename")
):
    """
    📥 Download video file through this server
    - Returns: File download (Content-Disposition: attachment)
    - Priority: Telegram link > YouTube download
    """
    
    # Check Telegram first
    telegram_link = await tg_mgr.get_link(video_id)
    if telegram_link:
        return RedirectResponse(url=telegram_link)

    # Get video data
    data = await yt_api.get_info(video_id)
    
    # Find specific format
    chosen = next(
        (m for m in data.get("medias", []) if m.get("formatId") == format_id), 
        None
    )
    
    if not chosen:
        raise HTTPException(status_code=404, detail=f"Format ID {format_id} not found")
    
    # Generate filename
    ext = chosen.get("ext", "mp4")
    if not filename:
        title = data.get("title", video_id)
        filename = sanitize_filename(f"{title}_{video_id}", ext)
    
    return await proxy_stream_media(
        request=request,
        upstream_url=chosen["url"],
        ext=ext,
        filename=filename,
        force_download=True  # Force download
    )

@app.get("/telegram/{video_id}")
async def get_telegram_link(video_id: str):
    """Get Telegram link for a video ID if it exists in indexed files"""
    link = await tg_mgr.get_link(video_id)
    if not link:
        raise HTTPException(status_code=404, detail="Video not found in Telegram index")
    return {"video_id": video_id, "telegram_url": link}

@app.get("/search", response_model=SearchResponse)
async def search_files(q: str = Query(..., description="Search query")):
    """Search indexed Telegram files by title or filename"""
    results = await tg_mgr.search(q)
    return SearchResponse(results=results, total=len(results))

@app.post("/proxy/add")
async def add_proxy(proxy_url: str = Query(..., description="Proxy URL (http:// or socks5://)")):
    """Add HTTP or SOCKS5 proxy for rotation"""
    proxy_mgr.add(proxy_url)
    return {"message": f"Added proxy: {proxy_url}", "total_proxies": len(proxy_mgr._proxies)}

@app.get("/proxy/status")
async def proxy_status():
    """Get proxy configuration status"""
    return {
        "total_proxies": len(proxy_mgr._proxies),
        "current_index": proxy_mgr._idx,
        "telegram_enabled": tg_mgr.enabled,
        "indexed_channel": tg_mgr.index_channel or "Not set"
    }

@app.get("/stats")
async def get_stats():
    """API statistics and configuration"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM indexed_files")
        total_files = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM rate_limits")
        total_ips = cur.fetchone()[0]
        conn.close()
        
        return {
            "version": APP_VERSION,
            "telegram_enabled": tg_mgr.enabled,
            "indexed_files": total_files,
            "unique_ips": total_ips,
            "rate_limit": f"{RATE_MAX} requests per {RATE_WIN} seconds",
            "proxies": len(proxy_mgr._proxies),
            "index_channel": tg_mgr.index_channel or "Not set",
            "upload_channel": tg_mgr.upload_channel or "Not set"
        }
    except Exception as e:
        return {"error": str(e), "version": APP_VERSION}

# ------------- Error Handlers -------------
@app.exception_handler(404)
async def not_found_handler(request, exc):
    return JSONResponse(status_code=404, content={"error": "Endpoint not found"})

@app.exception_handler(500)
async def internal_error_handler(request, exc):
    log.error(f"Internal error: {exc}")
    return JSONResponse(status_code=500, content={"error": "Internal server error"})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app", 
        host="72.60.108.92", 
        port=int(os.environ.get("PORT", "8000")), 
        reload=False,
        log_level="info"
    )
