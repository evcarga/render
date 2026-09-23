"""Sentinel - puente Telethon en Render.

Cambios respecto a la version anterior:
  1. El mensaje SOS incluye el RASTRO de ubicaciones (tabla location_history),
     no solo el ultimo punto.
  2. El PIN de coaccion ya no manda el mismo texto que un vencimiento normal.
     Antes, un aviso por coaccion le decia a los contactos "contacta a la
     persona", que es justo lo que NO deben hacer si alguien la esta reteniendo
     y mirando su telefono.
  3. El POST /dispatch-immediate responde al instante y despacha en segundo
     plano. Antes bloqueaba el servidor 12 segundos por cada contacto.
"""

import os
import json
import random
import hashlib
import asyncio
import datetime
import time
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

# Cuantos puntos del rastro se incluyen en el mensaje. La app guarda uno cada
# 10 minutos, asi que 12 son las ultimas 2 horas.
RASTRO_PUNTOS = int(os.environ.get("RASTRO_PUNTOS", "12"))

# Una sola sesion de Telethon por usuario a la vez: dos despachos simultaneos
# con la misma string session se pisan y Telegram cierra la conexion.
_locks_por_usuario = {}
_lock_maestro = threading.Lock()


def lock_de_usuario(user_id):
    with _lock_maestro:
        if user_id not in _locks_por_usuario:
            _locks_por_usuario[user_id] = threading.Lock()
        return _locks_por_usuario[user_id]


def supabase_request(endpoint, method="GET", payload=None):
    """Peticion REST autenticada con Service Role Key de Supabase"""
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


# ---------------------------------------------------------------------------
# Rastro de ubicaciones
# ---------------------------------------------------------------------------

def obtener_rastro(user_id, limite=RASTRO_PUNTOS):
    """Ultimos puntos guardados por la app, del mas reciente al mas antiguo."""
    endpoint = (
        f"location_history?user_id=eq.{user_id}"
        "&select=latitude,longitude,accuracy,recorded_at"
        "&order=recorded_at.desc"
        f"&limit={limite}"
    )
    puntos = supabase_request(endpoint)
    if not isinstance(puntos, list):
        return []
    # Solo los que traen coordenadas utiles.
    return [p for p in puntos
            if p.get("latitude") is not None and p.get("longitude") is not None]


def url_mapa(lat, lon):
    return f"https://maps.google.com/?q={lat},{lon}"


def hora_legible(iso_texto):
    try:
        t = datetime.datetime.fromisoformat(str(iso_texto).replace("Z", "+00:00"))
        return t.strftime("%d/%m %H:%M UTC")
    except Exception:
        return str(iso_texto or "?")


def formatear_rastro(puntos):
    if not puntos:
        return "(sin puntos previos registrados)"
    lineas = []
    for p in puntos:
        marca = hora_legible(p.get("recorded_at"))
        precision = p.get("accuracy")
        sufijo = f" (+/-{round(precision)} m)" if precision is not None else ""
        lineas.append(f"- {marca}{sufijo}\n  {url_mapa(p['latitude'], p['longitude'])}")
    return "\n".join(lineas)


# ---------------------------------------------------------------------------
# Mensaje SOS
# ---------------------------------------------------------------------------

def construir_mensaje(first_name, phone, lat, lon, puntos, reason):
    coaccion = (reason == "duress_pin_coaccion")

    # Si el disparo no trajo coordenadas, se usa el punto mas reciente del
    # rastro en vez de mandar "Ubicacion no disponible".
    if (lat is None or lon is None) and puntos:
        lat = puntos[0]["latitude"]
        lon = puntos[0]["longitude"]

    ubicacion = url_mapa(lat, lon) if lat is not None and lon is not None \
        else "Ubicacion no disponible"

    if coaccion:
        encabezado = (
            "ALERTA DE COACCION - ESTA PERSONA ESTA SIENDO OBLIGADA\n\n"
            f"De: {first_name} ({phone})\n"
            "Motivo: activo su PIN de emergencia bajo amenaza.\n\n"
            "NO la llames ni le escribas: quien la retiene podria estar\n"
            "mirando su telefono y eso la pondria en mas peligro.\n"
            "Llama al 911 y entrega la ubicacion de abajo."
        )
    else:
        encabezado = (
            "ALERTA TEMPRANA DE EMERGENCIA\n\n"
            f"De: {first_name} ({phone})\n"
            "Motivo: cronometro de seguridad vencido sin respuesta.\n\n"
            "Contacta a la persona. Si no contesta, llama al 911."
        )

    return (
        f"{encabezado}\n\n"
        f"Ultima ubicacion conocida:\n{ubicacion}\n\n"
        f"Recorrido de las ultimas horas (lo mas reciente primero):\n"
        f"{formatear_rastro(puntos)}"
    )


# ---------------------------------------------------------------------------
# Telethon
# ---------------------------------------------------------------------------

async def ejecutar_llamada_y_mensaje(api_id, api_hash, session_string,
                                     numero_destino, mensaje_texto):
    """Llama y envia mensaje directo mediante Telethon MTProto"""
    client = TelegramClient(StringSession(session_string), int(api_id), str(api_hash))
    await client.connect()
    resultados = {}
    try:
        entity = await client.get_input_entity(numero_destino)

        # 1. Enviar mensaje de auxilio con ubicacion y rastro
        if mensaje_texto:
            await client.send_message(entity, mensaje_texto)
            resultados['mensaje'] = 'enviado'
            print(f"[Telethon] Mensaje SOS entregado a {numero_destino}")

        # 2. Iniciar timbrado VoIP
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


async def despachar_alerta_para_usuario(timer, reason="timer_expired"):
    """Consulta credenciales de profiles con Service Role y dispara Telegram"""
    user_id = timer.get("user_id")

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

    contacts = supabase_request(
        f"emergency_contacts?user_id=eq.{user_id}&select=*"
        "&order=priority_order.asc&limit=5"
    ) or []

    lat = timer.get("last_latitude")
    lon = timer.get("last_longitude")
    puntos = obtener_rastro(user_id)

    sos_msg = construir_mensaje(first_name, phone, lat, lon, puntos, reason)

    logs = []
    for c in contacts:
        dest_num = c.get("phone_number")
        if dest_num and api_id and api_hash and session:
            res = await ejecutar_llamada_y_mensaje(
                api_id, api_hash, session, dest_num, sos_msg)
            logs.append({"contact": dest_num, "result": res})

    supabase_request("alert_logs", method="POST", payload={
        "user_id": user_id,
        "trigger_type": reason,
        "latitude": lat,
        "longitude": lon,
        "contacts_notified": logs,
        "status": "dispatched"
    })


def despachar_sincrono(timer, reason):
    """Envoltorio con candado por usuario, para llamar desde un hilo."""
    user_id = timer.get("user_id")
    with lock_de_usuario(user_id):
        asyncio.run(despachar_alerta_para_usuario(timer, reason))


# ---------------------------------------------------------------------------
# Poller
# ---------------------------------------------------------------------------

def revisar_y_despachar_cronometros_vencidos():
    """Busca cronometros vencidos en Supabase y despacha alertas"""
    if not SUPABASE_SERVICE_ROLE_KEY:
        print("[Poller] SUPABASE_SERVICE_ROLE_KEY no configurada.")
        return {"error": "SUPABASE_SERVICE_ROLE_KEY no configurada", "procesados": 0}

    now_iso = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    endpoint = (f"active_timers?status=eq.running&alert_triggered=eq.false"
                f"&expires_at=lte.{now_iso}&select=*")
    expired = supabase_request(endpoint) or []
    procesados = []

    for timer in expired:
        timer_id = timer.get("id")
        user_id = timer.get("user_id")
        print(f"Cronometro vencido detectado: {timer_id} para usuario {user_id}")
        # Marcar de inmediato para no repetir llamadas
        supabase_request(f"active_timers?id=eq.{timer_id}", method="PATCH", payload={
            "status": "expired",
            "alert_triggered": True,
            "alert_triggered_at": now_iso
        })
        despachar_sincrono(timer, "timer_expired")
        procesados.append(timer_id)

    return {
        "success": True,
        "cronometros_vencidos": len(procesados),
        "timers_procesados": procesados,
        "timestamp": now_iso
    }


def loop_revision_10_segundos():
    """Hilo en segundo plano: revisa cronometros vencidos cada 10 segundos"""
    print("[Sentinel Poller] Iniciado: comprobacion continua cada 10 segundos.")
    while True:
        try:
            revisar_y_despachar_cronometros_vencidos()
        except Exception as e:
            print(f"[Poller Error]: {e}")
        time.sleep(10)


# ---------------------------------------------------------------------------
# Servidor HTTP
# ---------------------------------------------------------------------------

class RenderWebServer(http.server.BaseHTTPRequestHandler):
    """Keep-alive y despacho bajo demanda, con CORS"""

    def send_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers",
                         "Content-Type, Authorization, x-client-info, apikey")

    def responder_json(self, codigo, cuerpo):
        self.send_response(codigo)
        self.send_header("Content-Type", "application/json")
        self.send_cors_headers()
        self.end_headers()
        self.wfile.write(json.dumps(cuerpo).encode("utf-8"))

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_cors_headers()
        self.end_headers()

    def do_GET(self):
        clean_path = self.path.split("?")[0].rstrip("/")
        if clean_path in ["", "/health"]:
            self.responder_json(200, {
                "status": "online",
                "service": "Sentinel Telethon Bridge",
                "time": datetime.datetime.now(datetime.timezone.utc).isoformat()
            })
        elif clean_path in ["/dispatch-immediate", "/cron", "/check-timers"]:
            try:
                self.responder_json(200, revisar_y_despachar_cronometros_vencidos())
            except Exception as e:
                self.responder_json(500, {"error": str(e)})
        else:
            self.send_response(404)
            self.send_cors_headers()
            self.end_headers()

    def do_POST(self):
        clean_path = self.path.split("?")[0].rstrip("/")
        if clean_path != "/dispatch-immediate":
            self.send_response(404)
            self.send_cors_headers()
            self.end_headers()
            return

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
                reason = data.get("reason") or "timer_expired"
                timer_fake = {
                    "user_id": user_id,
                    "last_latitude": data.get("latitude"),
                    "last_longitude": data.get("longitude"),
                }
                # Se responde YA y se despacha en segundo plano: cada contacto
                # tarda 12 s en timbrar, y la app no puede quedarse esperando
                # un minuto entero con el telefono en la mano de quien sea.
                threading.Thread(
                    target=despachar_sincrono,
                    args=(timer_fake, reason),
                    daemon=True
                ).start()
                self.responder_json(202, {
                    "success": True,
                    "mode": "immediate_user_alert",
                    "reason": reason,
                    "user_id": user_id
                })
            else:
                self.responder_json(200, revisar_y_despachar_cronometros_vencidos())
        except Exception as e:
            self.responder_json(500, {"error": str(e)})

    def log_message(self, fmt, *args):
        # El log por defecto imprime una linea por peticion; el keep-alive no
        # aporta nada al log de Render.
        pass


if __name__ == "__main__":
    poller_thread = threading.Thread(target=loop_revision_10_segundos, daemon=True)
    poller_thread.start()

    # ThreadingHTTPServer: con el servidor de un solo hilo, un despacho en
    # curso dejaba al keep-alive sin respuesta y Render podia marcar el
    # servicio como caido.
    server = http.server.ThreadingHTTPServer(("0.0.0.0", PORT), RenderWebServer)
    print(f"Sentinel Render Server escuchando en puerto {PORT}")
    server.serve_forever()
