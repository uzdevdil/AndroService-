"""
Remote Monitor — Relay Server
Render.com da ishlaydi (bepul)

Vazifasi:
  - Telefonlar ulanib, "onlayn" ro'yxatida turadi
  - EXE ulanganda onlayn telefonlar ro'yxatini ko'radi
  - EXE tanlagan telefon bilan ko'prik o'rnatadi
  - Ma'lumotlarni ikki tomonlama uzatadi
"""

import asyncio
import json
import logging
import os
import time
import uuid
import websockets
from websockets.server import WebSocketServerProtocol

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Ulanib turgan qurilmalar ──────────────────────────────────────────────────
# { device_id: { "ws": ws, "info": {...}, "viewer": ws | None, "connected_at": t } }
phones: dict[str, dict] = {}

# { viewer_ws: device_id }  — EXE qaysi telefonga ulanganini bilish uchun
viewers: dict[WebSocketServerProtocol, str] = {}

# ─────────────────────────────────────────────────────────────────────────────

async def handle(ws: WebSocketServerProtocol):
    """Har bir yangi ulanish shu funksiyaga tushadi."""
    role = None
    device_id = None

    try:
        # Birinchi xabar — kim ekanini aniqlaymiz
        raw = await asyncio.wait_for(ws.recv(), timeout=15)
        msg = json.loads(raw)
        role = msg.get("role")          # "phone" yoki "viewer"
        device_id = msg.get("device_id") or str(uuid.uuid4())

        if role == "phone":
            await handle_phone(ws, device_id, msg)
        elif role == "viewer":
            await handle_viewer(ws, device_id, msg)
        else:
            await ws.close(1008, "Noma'lum rol")

    except asyncio.TimeoutError:
        log.warning("Birinchi xabar kelmadi — ulanish yopildi")
    except Exception as e:
        log.error(f"handle() xatosi: {e}")
    finally:
        await cleanup(ws, role, device_id)


async def handle_phone(ws: WebSocketServerProtocol, device_id: str, info: dict):
    """Telefon ulanishi."""
    phones[device_id] = {
        "ws": ws,
        "info": {
            "device_id": device_id,
            "model": info.get("model", "Noma'lum qurilma"),
            "android": info.get("android", "?"),
            "sdk": info.get("sdk", 0),
            "battery": info.get("battery", -1),
        },
        "viewer": None,
        "connected_at": time.time(),
    }
    log.info(f"📱 Telefon ulandi: {info.get('model')} [{device_id}]")

    # Barcha viewer larga yangi telefon haqida xabar
    await broadcast_device_list()

    # Telefondan kelayotgan ma'lumotlarni viewerga uzatish
    try:
        async for message in ws:
            entry = phones.get(device_id)
            if entry and entry["viewer"]:
                try:
                    await entry["viewer"].send(message)
                except Exception:
                    entry["viewer"] = None


    except websockets.exceptions.ConnectionClosed:
        pass


async def handle_viewer(ws: WebSocketServerProtocol, viewer_id: str, msg: dict):
    """EXE (viewer) ulanishi."""
    log.info(f"🖥️  Viewer ulandi [{viewer_id}]")

    # Darhol qurilmalar ro'yxatini yuborish
    await send_device_list(ws)

    target_id = None

    try:
        async for raw in ws:
            # Viewer dan matnli buyruq
            if isinstance(raw, str):
                try:
                    data = json.loads(raw)
                    cmd = data.get("cmd")

                    if cmd == "connect_device":
                        # Telefonga ulanish so'rovi
                        tid = data.get("device_id")
                        entry = phones.get(tid)
                        if entry and entry["ws"]:
                            # Eski viewerni ajratish
                            if target_id and target_id in phones:
                                phones[target_id]["viewer"] = None

                            target_id = tid
                            entry["viewer"] = ws
                            viewers[ws] = tid
                            log.info(f"🔗 Viewer → Telefon [{tid}]")
                            await ws.send(json.dumps({
                                "type": "connected",
                                "device_id": tid,
                                "info": entry["info"]
                            }))
                        else:
                            await ws.send(json.dumps({
                                "type": "error",
                                "msg": "Telefon topilmadi yoki oflayn"
                            }))

                    elif cmd == "disconnect_device":
                        if target_id and target_id in phones:
                            phones[target_id]["viewer"] = None
                        target_id = None
                        viewers.pop(ws, None)

                    elif cmd == "get_devices":
                        await send_device_list(ws)

                    else:
                        # Boshqa buyruqlar telefonga uzatiladi
                        if target_id:
                            entry = phones.get(target_id)
                            if entry and entry["ws"]:
                                await entry["ws"].send(raw)

                except json.JSONDecodeError:
                    # JSON emas — to'g'ridan telefonga
                    if target_id:
                        entry = phones.get(target_id)
                        if entry and entry["ws"]:
                            await entry["ws"].send(raw)

            else:
                # Binary (video/audio) — telefonga uzatiladi
                if target_id:
                    entry = phones.get(target_id)
                    if entry and entry["ws"]:
                        await entry["ws"].send(raw)

    except websockets.exceptions.ConnectionClosed:
        pass


async def cleanup(ws, role, device_id):
    """Ulanish uzilganda tozalash."""
    if role == "phone" and device_id in phones:
        entry = phones.pop(device_id)
        # Agar viewer ulanib turgan bo'lsa — xabar berish
        if entry.get("viewer"):
            try:
                await entry["viewer"].send(json.dumps({
                    "type": "device_disconnected",
                    "device_id": device_id
                }))
            except Exception:
                pass
        viewers.pop(ws, None)
        log.info(f"📵 Telefon uzildi [{device_id}]")
        await broadcast_device_list()

    elif role == "viewer":
        tid = viewers.pop(ws, None)
        if tid and tid in phones:
            phones[tid]["viewer"] = None
        log.info(f"🖥️  Viewer uzildi")


async def send_device_list(ws: WebSocketServerProtocol):
    """Bitta viewer ga qurilmalar ro'yxatini yuborish."""
    devices = []
    now = time.time()
    for did, entry in phones.items():
        info = entry["info"].copy()
        info["online"] = True
        info["connected_seconds"] = int(now - entry["connected_at"])
        info["has_viewer"] = entry["viewer"] is not None
        devices.append(info)

    await ws.send(json.dumps({
        "type": "device_list",
        "devices": devices
    }))


async def broadcast_device_list():
    """Barcha viewer larga qurilmalar ro'yxatini yuborish."""
    if not viewers:
        return
    devices = []
    now = time.time()
    for did, entry in phones.items():
        info = entry["info"].copy()
        info["online"] = True
        info["connected_seconds"] = int(now - entry["connected_at"])
        info["has_viewer"] = entry["viewer"] is not None
        devices.append(info)

    msg = json.dumps({"type": "device_list", "devices": devices})
    dead = []
    for vws in viewers:
        try:
            await vws.send(msg)
        except Exception:
            dead.append(vws)
    for vws in dead:
        viewers.pop(vws, None)


async def main():
    port = int(os.environ.get("PORT", 8765))
    host = "0.0.0.0"
    log.info(f"🚀 Relay server ishga tushdi: {host}:{port}")
    async with websockets.serve(handle, host, port,
                                 max_size=10 * 1024 * 1024,   # 10MB (video chunk)
                                 ping_interval=20,
                                 ping_timeout=30):
        await asyncio.Future()  # doim ishlaydi


if __name__ == "__main__":
    asyncio.run(main())
