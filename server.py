"""
AndroService — Universal WebSocket Server
=========================================
Render.com Free tier da ishlaydi.

ROLLAR:
  phone  — APK uzatuvchi: video/audio yuboradi, buyruq qabul qiladi
  viewer — APK viewer / EXE / Terminal: buyruq yuboradi, media oladi

PROTOKOL:
  1. Ulanish (birinchi xabar):
       phone:  {"role":"phone",  "device_id":"...", "model":"...", "android":"...", "sdk":34}
       viewer: {"role":"viewer", "viewer_id":"..."}

  2. Ro'yxat (viewer → server):
       {"action":"list"}
       Javob: {"type":"device_list","devices":[{"id":"...","model":"..."},...]

  3. Qurilma tanlash (viewer → server):
       {"action":"connect","target_id":"..."}
       Javob viewer: {"type":"connected","target_id":"...","model":"..."}
       Javob phone:  {"type":"viewer_connected","viewer_id":"..."}

  4. Buyruq (viewer → phone, plain text):
       "SCREEN_ON" / "SCREEN_OFF" / "CAM_BACK_ON" / "MIC_ON" / ...

  5. Binary oqim (phone → barcha viewer):
       [0x01 ...] ekran H.264
       [0x02 ...] mikrofon AAC
       [0x03 ...] kamera H.264
       [0x04 ...] fayl chunk
       [0x05 ...] tizim ovozi AAC

  6. JSON oqim (phone → barcha viewer):
       {"type":"file_list",...} / {"type":"file_start",...} / ...

HEALTH:
  GET / yoki /health → {"status":"ok","phones":N,"viewers":N}
"""

import asyncio
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

from aiohttp import web, WSMsgType

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s,%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("AndroService")

# ── Sozlamalar ─────────────────────────────────────────────────────────────────
PORT         = int(os.environ.get("PORT", 10000))
MAX_PHONES   = 10
MAX_VIEWERS  = 20

# ── Ma'lumotlar ────────────────────────────────────────────────────────────────
@dataclass
class PhoneSession:
    ws:         web.WebSocketResponse
    device_id:  str
    model:      str
    android:    str = ""
    sdk:        int = 0
    joined_at:  float = field(default_factory=time.time)

@dataclass
class ViewerSession:
    ws:          web.WebSocketResponse
    viewer_id:   str
    target_id:   Optional[str] = None
    joined_at:   float = field(default_factory=time.time)

phones:  dict[str, PhoneSession]              = {}   # device_id → PhoneSession
viewers: dict[web.WebSocketResponse, ViewerSession] = {}   # ws → ViewerSession

# ── Yordamchi ──────────────────────────────────────────────────────────────────
async def tx(ws: web.WebSocketResponse, data) -> bool:
    """Xavfsiz yuborish."""
    try:
        if isinstance(data, (bytes, bytearray)):
            await ws.send_bytes(data)
        else:
            await ws.send_str(data)
        return True
    except Exception:
        return False

async def send_device_list(target_ws: Optional[web.WebSocketResponse] = None):
    msg = json.dumps({
        "type": "device_list",
        "devices": [
            {"id": p.device_id, "model": p.model,
             "android": p.android, "sdk": p.sdk}
            for p in phones.values()
        ],
    })
    if target_ws:
        await tx(target_ws, msg)
    else:
        for v in list(viewers.values()):
            await tx(v.ws, msg)

def viewers_for(device_id: str) -> list[ViewerSession]:
    return [v for v in viewers.values() if v.target_id == device_id]

# ── Phone handler ──────────────────────────────────────────────────────────────
async def handle_phone(ws: web.WebSocketResponse, init: dict):
    device_id = init.get("device_id") or str(uuid.uuid4())[:8]
    model     = init.get("model", "Unknown Device")
    android   = init.get("android", "")
    sdk       = int(init.get("sdk", 0))

    if len(phones) >= MAX_PHONES:
        await tx(ws, json.dumps({"type":"error","msg":"Maksimum qurilma soni to'ldi"}))
        return

    session = PhoneSession(ws=ws, device_id=device_id,
                           model=model, android=android, sdk=sdk)
    phones[device_id] = session
    log.info(f"📱 Phone ulandi: {model} [{device_id}]")

    await tx(ws, json.dumps({"type":"registered","device_id":device_id}))
    await send_device_list()   # Barcha viewerlarga yangi ro'yxat

    async for msg in ws:
        if msg.type == WSMsgType.BINARY:
            # Video/audio → barcha ulangan viewerlarga
            for v in viewers_for(device_id):
                await tx(v.ws, msg.data)

        elif msg.type == WSMsgType.TEXT:
            # JSON xabar (file_list, file_start, file_end, error) → viewerlarga
            for v in viewers_for(device_id):
                await tx(v.ws, msg.data)

        elif msg.type in (WSMsgType.CLOSE, WSMsgType.ERROR):
            break

    phones.pop(device_id, None)
    log.info(f"📱 Phone uzildi: {model} [{device_id}]")

    disc = json.dumps({"type":"phone_disconnected","device_id":device_id})
    for v in list(viewers.values()):
        if v.target_id == device_id:
            v.target_id = None
            await tx(v.ws, disc)
    await send_device_list()

# ── Viewer handler ─────────────────────────────────────────────────────────────
async def handle_viewer(ws: web.WebSocketResponse, init: dict):
    vid = init.get("viewer_id") or str(uuid.uuid4())[:8]

    if len(viewers) >= MAX_VIEWERS:
        await tx(ws, json.dumps({"type":"error","msg":"Maksimum viewer soni to'ldi"}))
        return

    session = ViewerSession(ws=ws, viewer_id=vid)
    viewers[ws] = session
    log.info(f"🖥️  Viewer ulandi: {vid}")

    # Darhol ro'yxat yuborish
    await send_device_list(ws)

    async for msg in ws:
        if msg.type == WSMsgType.TEXT:
            text = msg.data.strip()

            # JSON action?
            try:
                obj = json.loads(text)
                action = obj.get("action")
            except json.JSONDecodeError:
                obj = {}
                action = None

            # ── Ro'yxat ──────────────────────────────────────────────────
            if action == "list":
                await send_device_list(ws)
                continue

            # ── Qurilma tanlash ───────────────────────────────────────────
            if action == "connect":
                t_id = obj.get("target_id")
                phone = phones.get(t_id) if t_id else None

                if not phone:
                    await tx(ws, json.dumps({
                        "type":"error",
                        "msg": f"Qurilma topilmadi: {t_id}"
                    }))
                    continue

                # Avvalgi phone dan uzish
                if session.target_id and session.target_id != t_id:
                    old = phones.get(session.target_id)
                    if old:
                        await tx(old.ws, json.dumps({
                            "type":"viewer_disconnected","viewer_id":vid
                        }))

                session.target_id = t_id
                await tx(ws, json.dumps({
                    "type":"connected","target_id":t_id,"model":phone.model
                }))
                await tx(phone.ws, json.dumps({
                    "type":"viewer_connected","viewer_id":vid
                }))
                log.info(f"🔗 {vid} → {t_id} ({phone.model})")
                continue

            # ── Buyruq (plain text yoki JSON) → phone ga ──────────────────
            if session.target_id:
                phone = phones.get(session.target_id)
                if phone:
                    await tx(phone.ws, text)
                    log.debug(f"  CMD {vid}→{session.target_id}: {text[:80]}")
                else:
                    await tx(ws, json.dumps({"type":"error","msg":"Qurilma offline"}))
            else:
                await tx(ws, json.dumps({
                    "type":"error",
                    "msg":'Avval qurilma tanlang: {"action":"connect","target_id":"..."}'
                }))

        elif msg.type in (WSMsgType.CLOSE, WSMsgType.ERROR):
            break

    # Tozalash
    if session.target_id:
        phone = phones.get(session.target_id)
        if phone:
            await tx(phone.ws, json.dumps({
                "type":"viewer_disconnected","viewer_id":vid
            }))
    viewers.pop(ws, None)
    log.info(f"🖥️  Viewer uzildi: {vid}")

# ── HTTP + WebSocket birlashgan handler ───────────────────────────────────────
async def ws_handler(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse(
        heartbeat=20,
        max_msg_size=0,     # Cheksiz hajm (video kadrlar uchun)
    )
    await ws.prepare(request)

    # Birinchi xabar — rol aniqlash
    try:
        first = await asyncio.wait_for(ws.receive(), timeout=15)
    except asyncio.TimeoutError:
        await ws.close()
        return ws

    if first.type != WSMsgType.TEXT:
        await ws.close()
        return ws

    try:
        data = json.loads(first.data)
    except json.JSONDecodeError:
        await ws.close()
        return ws

    role = data.get("role", "viewer")

    if role == "phone":
        await handle_phone(ws, data)
    else:
        await handle_viewer(ws, data)

    return ws

async def health_handler(request: web.Request) -> web.Response:
    return web.Response(
        content_type="application/json",
        text=json.dumps({
            "status":  "ok",
            "phones":  len(phones),
            "viewers": len(viewers),
            "devices": [
                {"id": p.device_id, "model": p.model, "android": p.android}
                for p in phones.values()
            ],
        }),
    )

# ── Ishga tushirish ────────────────────────────────────────────────────────────
def main():
    app = web.Application()
    app.router.add_get("/",        health_handler)
    app.router.add_get("/health",  health_handler)
    app.router.add_get("/ws",      ws_handler)

    log.info(f"🚀 AndroService ishga tushmoqda: port {PORT}")
    web.run_app(app, host="0.0.0.0", port=PORT, access_log=None)

if __name__ == "__main__":
    main()
