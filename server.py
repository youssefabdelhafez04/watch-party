import asyncio
import atexit
import contextlib
import os
import re
import shutil
import subprocess
import tempfile
import time
import uuid
from typing import Optional
from urllib.parse import urljoin, quote

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Query
from fastapi.responses import FileResponse, Response, StreamingResponse
from fastapi.staticfiles import StaticFiles

FFMPEG   = shutil.which("ffmpeg")
FFPROBE  = shutil.which("ffprobe")

rooms: dict[str, dict] = {}


async def cleanup_room_media(room: dict):
    proc = room.get("proc")
    if proc and proc.returncode is None:
        proc.terminate()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(proc.wait(), timeout=3)
        if proc.returncode is None:
            proc.kill()
    tmpdir = room.pop("tmpdir", None)
    if tmpdir:
        shutil.rmtree(tmpdir, ignore_errors=True)
    room.pop("m3u8", None)
    room.pop("proc", None)
    room.pop("transcode_start", None)


async def set_room_stream(room_id: str, room: dict, url: str):
    await cleanup_room_media(room)
    room["stream_url"] = url
    room["duration"] = 0
    room["source_size"] = 0
    room["stream_version"] = room.get("stream_version", 0) + 1
    room["state"] = {"playing": False, "position": 0.0, "ts": time.time()}
    if FFMPEG:
        tmpdir, m3u8, proc = await start_transcode(room_id, url)
        room.update(tmpdir=tmpdir, m3u8=m3u8, proc=proc, transcode_start=time.time())
        asyncio.create_task(probe_source_info(room_id, url))


# ── ffmpeg ────────────────────────────────────────────────────────────────────

async def probe_source_info(room_id: str, url: str):
    # Get duration
    if FFPROBE:
        try:
            proc = await asyncio.create_subprocess_exec(
                FFPROBE, "-v", "quiet", "-show_entries", "format=duration",
                "-of", "csv=p=0", url,
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=20)
            if room_id in rooms:
                rooms[room_id]["duration"] = float(stdout.decode().strip())
        except Exception:
            pass
    # Get source file size for progress estimation
    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=10) as client:
            resp = await client.head(url)
            size = int(resp.headers.get("content-length", 0))
            if room_id in rooms:
                rooms[room_id]["source_size"] = size
    except Exception:
        pass


async def start_transcode(room_id: str, url: str):
    tmpdir  = tempfile.mkdtemp(prefix=f"wp_{room_id}_")
    m3u8 = os.path.join(tmpdir, "out.m3u8")
    proc = await asyncio.create_subprocess_exec(
        FFMPEG, "-hide_banner", "-loglevel", "warning",
        "-analyzeduration", "500000", "-probesize", "500000",
        "-i", url,
        "-map", "0:v:0", "-map", "0:a:0?",
        # Re-encode video to H.264 8-bit so every browser/phone can decode it.
        # (`-c:v copy` passed HEVC/10-bit/HDR straight through -> black on many devices.)
        # Use Apple's hardware encoder (VideoToolbox) so the CPU/fan stay calm;
        # `-allow_sw 1` falls back to software if hardware is unavailable.
        "-c:v", "h264_videotoolbox", "-allow_sw", "1", "-realtime", "1",
        "-profile:v", "high", "-b:v", "6000k", "-pix_fmt", "yuv420p",
        "-force_key_frames", "expr:gte(t,n_forced*3)",
        "-c:a", "aac", "-b:a", "192k", "-ac", "2",
        "-sn",
        "-f", "hls", "-hls_time", "3",
        "-hls_list_size", "0", "-hls_flags", "append_list",
        "-hls_segment_filename", os.path.join(tmpdir, "seg%05d.ts"),
        m3u8,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    return tmpdir, m3u8, proc


# ── App ───────────────────────────────────────────────────────────────────────

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
async def home():
    return FileResponse("static/index.html", headers={"cache-control": "no-store"})


@app.get("/watch/{room_id}")
async def watch(room_id: str):
    return FileResponse("static/index.html", headers={"cache-control": "no-store"})


# ── Room API ──────────────────────────────────────────────────────────────────

@app.post("/api/create")
async def create_room(request: Request):
    body = await request.json()
    url  = body.get("stream_url", "").strip()
    title = body.get("title", "").strip()
    if not url:
        return Response(status_code=400)
    room_id = uuid.uuid4().hex[:8]
    room: dict = {
        "stream_url": url,
        "sockets": set(),
        "state": {"playing": False, "position": 0.0, "ts": time.time()},
        "stream_version": 1,
        "title": title or "Watch Party",
    }
    if FFMPEG:
        tmpdir, m3u8, proc = await start_transcode(room_id, url)
        room.update(tmpdir=tmpdir, m3u8=m3u8, proc=proc, transcode_start=time.time())
        asyncio.create_task(probe_source_info(room_id, url))
    rooms[room_id] = room
    return {"room_id": room_id}


@app.get("/api/room/{room_id}")
async def room_info(room_id: str):
    r = rooms.get(room_id)
    if not r:
        return Response(status_code=404)
    st = r["state"].copy()
    if st["playing"]:
        st["position"] += time.time() - st["ts"]

    # Transcode progress
    transcode_done = False
    transcode_progress = 0.0
    if "m3u8" in r:
        proc = r["proc"]
        m3u8 = r["m3u8"]
        duration = r.get("duration", 0)
        if proc.returncode is not None:
            transcode_done = True
            transcode_progress = 100.0
        elif duration > 0 and r.get("transcode_start"):
            elapsed = time.time() - r["transcode_start"]
            # Estimate: 2x speed (video copy + aac transcode)
            estimated_progress = min(100.0, (elapsed / (duration / 2)) * 100)
            transcode_progress = estimated_progress
        elif os.path.exists(m3u8):
            transcode_progress = 5.0  # At least show something if m3u8 exists

    stream_url = f"/hls/{room_id}/out.m3u8?v={r.get('stream_version', 1)}" if "m3u8" in r else f"/stream/{room_id}?v={r.get('stream_version', 1)}"
    return {
        "room_id": room_id,
        "state": st,
        "stream_url": stream_url,
        "duration": r.get("duration", 0),
        "transcode_done": transcode_done,
        "transcode_progress": transcode_progress,
        "stream_version": r.get("stream_version", 1),
        "title": r.get("title", "Watch Party"),
    }


@app.get("/api/config")
async def get_config():
    return {"public_url": _public_url}


# ── WebSocket ─────────────────────────────────────────────────────────────────

async def broadcast(room: dict, msg: dict, exclude: Optional[WebSocket] = None):
    dead = set()
    for ws in list(room["sockets"]):
        if ws is exclude:
            continue
        try:
            await ws.send_json(msg)
        except Exception:
            dead.add(ws)
    room["sockets"] -= dead


@app.websocket("/ws/{room_id}")
async def ws_endpoint(websocket: WebSocket, room_id: str):
    r = rooms.get(room_id)
    if not r:
        await websocket.close(code=4004)
        return
    await websocket.accept()
    r["sockets"].add(websocket)
    st = r["state"]
    # Send the raw authoritative timeline (position sampled at `ts`) plus the
    # server clock. The client advances `position` itself using its clock offset,
    # which cancels out network latency.
    await websocket.send_json({
        "type": "sync",
        "playing": st["playing"],
        "position": st["position"],
        "ts": st["ts"],
        "server_time": time.time(),
    })
    try:
        while True:
            data = await websocket.receive_json()
            t   = data.get("type")
            if t == "ping":
                # NTP-style clock sync: echo the client's send time + our clock.
                await websocket.send_json({"type": "pong", "t0": data.get("t0"), "server": time.time()})
            elif t in ("play", "pause", "seek"):
                pos     = float(data.get("position", 0))
                playing = t == "play" or (t == "seek" and bool(data.get("playing", False)))
                now = time.time()
                r["state"].update(playing=playing, position=pos, ts=now)
                await broadcast(r, {"type": t, "position": pos, "playing": playing, "ts": now}, exclude=websocket)
            elif t == "change_video":
                url = str(data.get("stream_url", "")).strip()
                if not url:
                    continue
                title = str(data.get("title", "")).strip()
                if title:
                    r["title"] = title
                await set_room_stream(room_id, r, url)
                stream_url = f"/hls/{room_id}/out.m3u8?v={r.get('stream_version', 1)}" if "m3u8" in r else f"/stream/{room_id}?v={r.get('stream_version', 1)}"
                msg = {
                    "type": "change_video",
                    "stream_url": stream_url,
                    "stream_version": r.get("stream_version", 1),
                    "state": r["state"],
                    "title": r.get("title", "Watch Party"),
                }
                await websocket.send_json(msg)
                await broadcast(r, msg, exclude=websocket)
    except WebSocketDisconnect:
        r["sockets"].discard(websocket)
    except Exception:
        r["sockets"].discard(websocket)


# ── HLS serving ───────────────────────────────────────────────────────────

async def wait_for(path: str, timeout: float = 60.0) -> bool:
    for _ in range(int(timeout / 0.5)):
        if os.path.exists(path) and os.path.getsize(path) > 0:
            return True
        await asyncio.sleep(0.5)
    return False


@app.get("/hls/{room_id}/out.m3u8")
async def hls_manifest(room_id: str):
    r = rooms.get(room_id)
    if not r or "m3u8" not in r:
        return Response(status_code=404)
    if not await wait_for(r["m3u8"]):
        return Response(status_code=504)
    try:
        with open(r["m3u8"], "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
    except OSError:
        return Response(status_code=404)
    version = quote(str(r.get("stream_version", 1)), safe="")
    lines = []
    for line in content.splitlines():
        s = line.strip()
        if s and not s.startswith("#"):
            joiner = "&" if "?" in s else "?"
            lines.append(f"{s}{joiner}v={version}")
        else:
            lines.append(line)
    return Response("\n".join(lines).encode(), media_type="application/vnd.apple.mpegurl",
                    headers={"access-control-allow-origin": "*", "cache-control": "no-store"})


@app.get("/hls/{room_id}/{segment}")
async def hls_segment(room_id: str, segment: str):
    r = rooms.get(room_id)
    if not r or "tmpdir" not in r:
        return Response(status_code=404)
    path = os.path.join(r["tmpdir"], segment)
    if not await wait_for(path, timeout=30.0):
        return Response(status_code=404)
    return FileResponse(path, headers={"access-control-allow-origin": "*", "cache-control": "no-store"})


# ── Transcode stream (fallback) ───────────────────────────────────────────────

# ── Proxy (fallback) ──────────────────────────────────────────────────────────

def rewrite_m3u8(content: str, room_id: str, base_url: str) -> str:
    out = []
    for line in content.splitlines():
        s = line.strip()
        if s and not s.startswith("#"):
            abs_url = s if s.startswith(("http://", "https://")) else urljoin(base_url, s)
            out.append(f"/stream/{room_id}?url={quote(abs_url, safe='')}")
        else:
            out.append(line)
    return "\n".join(out)


TIMEOUT = httpx.Timeout(connect=10.0, read=None, write=10.0, pool=5.0)
FORWARD = {"range", "accept", "user-agent"}


@app.api_route("/stream/{room_id}", methods=["GET", "HEAD"])
async def proxy_stream(room_id: str, request: Request, url: Optional[str] = Query(None)):
    r = rooms.get(room_id)
    if not r:
        return Response(status_code=404)
    target = url if url else r["stream_url"]
    fwd    = {k: v for k, v in request.headers.items() if k.lower() in FORWARD}
    client = httpx.AsyncClient(timeout=TIMEOUT, follow_redirects=True)
    try:
        if request.method == "HEAD":
            resp = await client.head(target, headers=fwd)
            await client.aclose()
            return Response(status_code=resp.status_code, headers={
                "access-control-allow-origin": "*", "accept-ranges": "bytes",
                "content-type": resp.headers.get("content-type", "video/mp4"),
            })
        req  = client.build_request("GET", target, headers=fwd)
        resp = await client.send(req, stream=True)
        ct   = resp.headers.get("content-type", "")
        if "mpegurl" in ct.lower() or target.split("?")[0].endswith(".m3u8"):
            body = await resp.aread()
            await resp.aclose(); await client.aclose()
            return Response(content=rewrite_m3u8(body.decode(errors="replace"), room_id, target).encode(),
                            media_type="application/vnd.apple.mpegurl",
                            headers={"access-control-allow-origin": "*"})
        hdrs = {"access-control-allow-origin": "*", "content-type": ct or "video/mp4"}
        for h in ("content-length", "content-range", "accept-ranges"):
            if h in resp.headers:
                hdrs[h] = resp.headers[h]
        async def streamer():
            try:
                async for chunk in resp.aiter_bytes(65536):
                    yield chunk
            finally:
                await resp.aclose(); await client.aclose()
        return StreamingResponse(streamer(), status_code=resp.status_code, headers=hdrs)
    except Exception as e:
        try: await client.aclose()
        except: pass
        return Response(status_code=502, content=str(e).encode())


# ── Entry ─────────────────────────────────────────────────────────────────────

_public_url: Optional[str] = None
_tunnel_proc: Optional[subprocess.Popen] = None


def start_cloudflare_tunnel(port: int) -> Optional[str]:
    global _tunnel_proc
    cloudflared = shutil.which("cloudflared")
    if not cloudflared:
        return None
    _tunnel_proc = subprocess.Popen(
        [cloudflared, "tunnel", "--url", f"http://localhost:{port}", "--no-autoupdate"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    atexit.register(lambda: _tunnel_proc and _tunnel_proc.terminate())
    deadline = time.time() + 20
    while time.time() < deadline and _tunnel_proc.poll() is None:
        line = _tunnel_proc.stdout.readline() if _tunnel_proc.stdout else ""
        match = re.search(r"https://[a-zA-Z0-9-]+\.trycloudflare\.com", line)
        if match:
            return match.group(0)
    return None

if __name__ == "__main__":
    import uvicorn
    port = 8000
    tunnel_provider = None
    _public_url = start_cloudflare_tunnel(port)
    tunnel_provider = "Cloudflare" if _public_url else None

    sep = "=" * 54
    print(f"\n{sep}")
    print("  Watch Party — ready")
    print(f"  Local:  http://localhost:{port}")
    if _public_url:
        print(f"  Public: {_public_url} ({tunnel_provider})")
        print(f"\n  Send the Public URL to your friend")
    print(f"  ffmpeg: {'✓' if FFMPEG else 'NOT FOUND'}")
    print(f"{sep}\n")

    uvicorn.run(app, host="0.0.0.0", port=port, log_level="warning")
