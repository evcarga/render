import os
import json
import random
import hashlib
import asyncio
import datetime
import http.server
import threading
from urllib.request import Request, urlopen
from telethon.sync import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.phone import RequestCallRequest, DiscardCallRequest
from telethon.tl.types import PhoneCallProtocol, PhoneCallDiscardReasonDisconnect

# Variables de entorno en Render
PORT = int(os.environ.get("PORT", 5000))
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://oxzhmeeyiesflhhhehpa.supabase.co")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

def supabase_request(endpoint, method="GET", payload=None):
    """Petición REST autenticada con Service Role Key de Supabase"""
    url = f"{SUPABASE_URL}/rest/v1/{endpoint}"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }
    data_bytes = json.dumps(payload).encode("utf-8") if payload else None
    req = Request(url, data=data_bytes, headers=headers, method=method)
    try:
        with urlopen(req, timeout=10) as resp:
            content = resp.read().decode("utf-8")
            return json.loads(content) if content else {}
    except Exception as e:
        print(f"[Supabase REST Error] {endpoint}: {e}")
        return None

async def ejecutar_llamada_y_mensaje(api_id, api_hash, session_string, numero_destino, mensaje_texto):
    """Llama y envía mensaje directo mediante Telethon MTProto"""
    client = TelegramClient(StringSession(session_string), int(api_id), str(api_hash))
    await client.connect()
    resultados = {}
    try:
        entity = await client.get_input_entity(numero_destino)

        # 1. Enviar mensaje de auxilio con ubicación
        if mensaje_texto:
            await client.send_message(entity, mensaje_texto)
            resultados['mensaje'] = 'enviado'
            print(f"[Telethon] Mensaje SOS entregado a {numero_destino}")

        # 2. Iniciar Timbrado VoIP
        g_a = bytes([random.randint(0, 255) for _ in range(256)])
        g_a_hash = hashlib.sha256(g_a).digest()

        call_result = await client(RequestCallRequest(
            user_id=entity,
            random_id=random.randint(0, 0x7fffffff),
            g_a_hash=g_a_hash,
            protocol=PhoneCallProtocol(
                udp_p2p=True, udp_reflector=True,
                min_layer=92, max_layer=92,
                library_versions=['1.0.0']
            ),
            video=False
        ))
        resultados['llamada'] = 'timbrando'
        print(f"[Telethon] Timbrando VoIP a {numero_destino}...")

        # Timbrar durante 12 segundos para alertar
        await asyncio.sleep(12)
        try:
            await client(DiscardCallRequest(
                peer=call_result.phone_call,
                duration=0,
                reason=PhoneCallDiscardReasonDisconnect(),
                connection_id=0
            ))
            print(f"[Telethon] Timbrado finalizado a {numero_destino}")
        except Exception:
            pass

    except Exception as err:
        print(f"[Telethon Error Contacto {numero_destino}]: {err}")
        resultados['error'] = str(err)
    finally:
        await client.disconnect()

    return resultados

async def despachar_alerta_para_usuario(timer):
    """Consulta credenciales seguras de profiles con Service Role y dispara Telegram"""
    user_id = timer.get("user_id")
    # 1. Obtener credenciales privadas de profiles
    profiles = supabase_request(f"profiles?id=eq.{user_id}&select=*")
    if not profiles or len(profiles) == 0:
        print(f"Perfil no encontrado para user {user_id}")
        return

    profile = profiles[0]
    api_id = profile.get("telegram_api_id")
    api_hash = profile.get("telegram_api_hash")
    session = profile.get("telegram_session")
    first_name = profile.get("first_name", "Usuario")
    phone = profile.get("telegram_phone", "")

    # 2. Obtener contactos de emergencia del usuario
    contacts = supabase_request(f"emergency_contacts?user_id=eq.{user_id}&select=*&order=priority_order.asc&limit=5") or []

    lat = timer.get("last_latitude")
    lon = timer.get("last_longitude")
    maps_url = f"https://maps.google.com/?q={lat},{lon}" if lat and lon else "Ubicación no disponible"

    sos_msg = (
        f"🚨 ALERTA TEMPRANA DE EMERGENCIA -> CONTACTA A LA PERSONA SINO CONTESTA LLAMA AL 911 🚨\n\n"
        f"De: {first_name} ({phone})\n"
        f"Motivo: ⏱️ CRONÓMETRO DE SEGURIDAD VENCIDO SIN RESPUESTA\n\n"
        f"📍 Mi última ubicación GPS exacta:\n{maps_url}\n\n"
        f"¡Por favor verifica mi estado o alerta a las autoridades de inmediato!"
    )

    logs = []
    for c in contacts:
        dest_num = c.get("phone_number")
        if dest_num and api_id and api_hash and session:
            res = await ejecutar_llamada_y_mensaje(api_id, api_hash, session, dest_num, sos_msg)
            logs.append({"contact": dest_num, "result": res})

    # 3. Guardar log en Supabase
    supabase_request("alert_logs", method="POST", payload={
        "user_id": user_id,
        "trigger_type": "timer_expired_10s_poller",
        "latitude": lat,
        "longitude": lon,
        "contacts_notified": logs,
        "status": "dispatched"
    })

def revisar_y_despachar_cronometros_vencidos():
    """Consulta Supabase buscando cronómetros vencidos y despacha alertas por Telegram"""
    if not SUPABASE_SERVICE_ROLE_KEY:
        print("[Poller] SUPABASE_SERVICE_ROLE_KEY no configurada.")
        return {"error": "SUPABASE_SERVICE_ROLE_KEY no configurada", "procesados": 0}

    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()
    endpoint = f"active_timers?status=eq.running&alert_triggered=eq.false&expires_at=lte.{now_iso}&select=*"
    expired = supabase_request(endpoint) or []
    procesados = []

    for timer in expired:
        timer_id = timer.get("id")
        user_id = timer.get("user_id")
        print(f"⏱️ Cronómetro vencido detectado: {timer_id} para usuario {user_id}")
        # Marcar inmediatamente como vencido para no repetir llamadas
        supabase_request(f"active_timers?id=eq.{timer_id}", method="PATCH", payload={
            "status": "expired",
            "alert_triggered": True,
            "alert_triggered_at": now_iso
        })
        # Despachar llamadas y mensajes SOS
        asyncio.run(despachar_alerta_para_usuario(timer))
        procesados.append(timer_id)

    return {
        "success": True,
        "cronometros_vencidos": len(procesados),
        "timers_procesados": procesados,
        "timestamp": now_iso
    }

def loop_revision_10_segundos():
    """Hilo en segundo plano: revisa continuamente cada 10 segundos cronómetros vencidos"""
    print("[Sentinel Poller] Iniciado: comprobación continua cada 10 segundos.")
    while True:
        try:
            revisar_y_despachar_cronometros_vencidos()
        except Exception as e:
            print(f"[Poller Error]: {e}")

        asyncio.run(asyncio.sleep(10))

class RenderWebServer(http.server.BaseHTTPRequestHandler):
    """Servidor HTTP para recibir Keep-Alive y llamadas bajo demanda con soporte CORS"""
    def send_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization, x-client-info, apikey")

    def do_OPTIONS(self):
        # Manejo de la solicitud Preflight CORS del navegador
        self.send_response(200)
        self.send_cors_headers()
        self.end_headers()

    def do_GET(self):
        clean_path = self.path.split("?")[0].rstrip("/")
        # Endpoint de salud y Keep-Alive
        if clean_path in ["", "/health"]:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_cors_headers()
            self.end_headers()
            resp = {
                "status": "online",
                "service": "Sentinel Telethon Bridge",
                "time": datetime.datetime.now(datetime.timezone.utc).isoformat()
            }
            self.wfile.write(json.dumps(resp).encode("utf-8"))
        # Endpoint directo activable por navegador o Cron externo (ej: cron-job.org)
        elif clean_path in ["/dispatch-immediate", "/cron", "/check-timers"]:
            try:
                res = revisar_y_despachar_cronometros_vencidos()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_cors_headers()
                self.end_headers()
                self.wfile.write(json.dumps(res).encode("utf-8"))
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.send_cors_headers()
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode("utf-8"))
        else:
            self.send_response(404)
            self.send_cors_headers()
            self.end_headers()

    def do_POST(self):
        clean_path = self.path.split("?")[0].rstrip("/")
        if clean_path == "/dispatch-immediate":
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length).decode("utf-8") if length > 0 else ""
            data = {}
            if body:
                try:
                    data = json.loads(body)
                except Exception:
                    data = {}

            user_id = data.get("user_id")
            try:
                if user_id:
                    # Despacho forzado bajo demanda (ej: PIN de coacción desde la aplicación web)
                    lat = data.get("latitude")
                    lon = data.get("longitude")
                    timer_fake = {"user_id": user_id, "last_latitude": lat, "last_longitude": lon}
                    asyncio.run(despachar_alerta_para_usuario(timer_fake))
                    res = {"success": True, "mode": "immediate_user_alert", "user_id": user_id}
                else:
                    # Llamada desde Cron externo vía POST sin user_id específico: revisar base de datos
                    res = revisar_y_despachar_cronometros_vencidos()

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_cors_headers()
                self.end_headers()
                self.wfile.write(json.dumps(res).encode("utf-8"))
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.send_cors_headers()
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode("utf-8"))
        else:
            self.send_response(404)
            self.send_cors_headers()
            self.end_headers()

if __name__ == "__main__":
    # 1. Iniciar el hilo de revisión cada 10 segundos
    poller_thread = threading.Thread(target=loop_revision_10_segundos, daemon=True)
    poller_thread.start()

    # 2. Iniciar servidor web en el puerto asignado por Render
    server = http.server.HTTPServer(("0.0.0.0", PORT), RenderWebServer)
    print(f"🚀 Sentinel Render Server escuchando en puerto {PORT}")
    server.serve_forever()
