"""Sentinel - puente Telethon en Render.

Cambios respecto a la version anterior:
  1. El rastro del mensaje SOLO trae los puntos de la guardia en curso (desde
     que se activo el cronometro), no ubicaciones de dias anteriores.
  2. Cada alerta lleva un enlace de seguimiento en vivo (pagina en GitHub
     Pages). El token del enlace solo existe en el mensaje: en la base se
     guarda su SHA-256. El enlace muere a los 3 dias de activar el cronometro.
  3. Cada hora se purgan los puntos de mas de 3 dias y las sesiones vencidas.
"""

import os
import re
import json
import random
import hashlib
import secrets
import asyncio
import datetime
import time
import http.server
import threading
from urllib.request import Request, urlopen

from telethon import TelegramClient, events, errors
from telethon.sessions import StringSession
from telethon.tl.functions.phone import RequestCallRequest, DiscardCallRequest
from telethon.tl.functions.contacts import GetContactsRequest, ImportContactsRequest
from telethon.tl.functions.updates import GetStateRequest
from telethon.tl.types import (
    PhoneCallProtocol, PhoneCallDiscardReasonDisconnect, InputPeerUser,
    InputPhoneCall, InputPhoneContact, UpdatePhoneCall, PhoneCallWaiting,
    PhoneCallAccepted, PhoneCallDiscarded, MessageActionPhoneCall,
)

# Se muestra en /health para saber que version esta desplegada en Render.
VERSION = "2026-09-24.4-llamada"

# Variables de entorno en Render
PORT = int(os.environ.get("PORT", 5000))
SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://oxzhmeeyiesflhhhehpa.supabase.co")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")

# Cuantos puntos del rastro se escriben en el mensaje. El recorrido completo
# esta en el enlace de seguimiento; aqui van solo los mas recientes.
RASTRO_PUNTOS = int(os.environ.get("RASTRO_PUNTOS", "6"))

# Pagina de seguimiento (GitHub Pages). El token va en el fragmento (#t=...),
# que el navegador nunca envia a ningun servidor ni en el Referer.
TRACKING_BASE_URL = os.environ.get(
    "TRACKING_BASE_URL", "https://evcarga.github.io/seguimiento/")

# Zona horaria de las horas del mensaje (los contactos leen hora local).
ZONA_HORARIA = os.environ.get("ZONA_HORARIA", "America/Bogota")

# Una sesion cerrada hace menos que esto todavia se considera "la de esta
# alerta" (carrera entre el PIN de coaccion y el cierre de la guardia).
MARGEN_SESION = datetime.timedelta(minutes=10)

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
    data_bytes = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = Request(url, data=data_bytes, headers=headers, method=method)
    try:
        with urlopen(req, timeout=10) as resp:
            content = resp.read().decode("utf-8")
            return json.loads(content) if content else {}
    except Exception as e:
        print(f"[Supabase REST Error] {endpoint}: {e}")
        return None


def ahora_utc():
    return datetime.datetime.now(datetime.timezone.utc)


def iso(t):
    return t.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


_FRACCION = re.compile(r"\.(\d+)")


def parse_iso(texto):
    if not texto:
        return None
    txt = str(texto).strip().replace("Z", "+00:00").replace(" ", "T", 1)
    # Postgres recorta los ceros finales de los microsegundos (".27776") y el
    # fromisoformat de Python 3.10 solo acepta 3 o 6 cifras: se completan a 6.
    txt = _FRACCION.sub(lambda m: "." + (m.group(1) + "000000")[:6], txt, 1)
    txt = re.sub(r"([+-]\d{2})$", r"\g<1>:00", txt)  # "+00" -> "+00:00"
    try:
        t = datetime.datetime.fromisoformat(txt)
    except Exception:
        return None
    return t if t.tzinfo else t.replace(tzinfo=datetime.timezone.utc)


# ---------------------------------------------------------------------------
# Sesion de guardia y enlace de seguimiento
# ---------------------------------------------------------------------------

def inicio_cronometro(user_id, timer):
    """Cuando se activo el cronometro de esta alerta (None si no se sabe)."""
    inicio = parse_iso(timer.get("started_at"))
    if inicio is None:
        cron = supabase_request(
            f"active_timers?user_id=eq.{user_id}&select=started_at&limit=1")
        if isinstance(cron, list) and cron:
            inicio = parse_iso(cron[0].get("started_at"))
    return inicio


def obtener_sesion(user_id, timer):
    """La sesion de la guardia que disparo esta alerta.

    La crea la app al activar el cronometro. Si no existe (app vieja o sin red
    en ese momento) se crea aqui, empezando en el started_at del cronometro,
    para que el rastro igual quede limitado a esta guardia.
    """
    inicio = inicio_cronometro(user_id, timer)

    filas = supabase_request(
        f"guard_sessions?user_id=eq.{user_id}"
        "&select=id,started_at,stopped_at,expires_at"
        "&order=started_at.desc&limit=1"
    )
    if isinstance(filas, list) and filas:
        s = filas[0]
        empieza = parse_iso(s.get("started_at"))
        parada = parse_iso(s.get("stopped_at"))
        vence = parse_iso(s.get("expires_at"))
        vigente = vence is None or vence > ahora_utc()
        abierta = parada is None or parada > ahora_utc() - MARGEN_SESION
        # Una sesion que empezo bastante antes que este cronometro es de otra
        # guardia (una app vieja nunca las cierra): no se reutiliza.
        misma_guardia = (inicio is None or empieza is None or
                         empieza >= inicio - MARGEN_SESION)
        if vigente and abierta and misma_guardia:
            return s

    if inicio is None or inicio < ahora_utc() - datetime.timedelta(days=3):
        inicio = ahora_utc()

    creada = supabase_request("guard_sessions", method="POST", payload={
        "user_id": user_id,
        "started_at": iso(inicio),
        "expires_at": iso(inicio + datetime.timedelta(days=3)),
    })
    if isinstance(creada, list) and creada:
        return creada[0]
    return None


def crear_enlace(sesion):
    """Genera un token nuevo para esta alerta y devuelve la URL, o None."""
    if not sesion or not sesion.get("id"):
        return None
    token = secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
    ok = supabase_request("tracking_links", method="POST", payload={
        "token_hash": token_hash,
        "session_id": sesion["id"],
    })
    if ok is None:
        return None
    return f"{TRACKING_BASE_URL}#t={token}"


# ---------------------------------------------------------------------------
# Rastro de ubicaciones
# ---------------------------------------------------------------------------

def obtener_rastro(user_id, desde, limite=RASTRO_PUNTOS):
    """Puntos de esta guardia (desde que se activo el cronometro), del mas
    reciente al mas antiguo. Nunca devuelve puntos de guardias anteriores."""
    if desde is None:
        return []
    # Mismo margen de 1 minuto que usa get_tracking en la base.
    desde_iso = iso(desde - datetime.timedelta(minutes=1))
    endpoint = (
        f"location_history?user_id=eq.{user_id}"
        f"&recorded_at=gte.{desde_iso}"
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


# Colombia no tiene horario de verano: si el servidor no trae la base de zonas
# horarias, UTC-5 fijo da exactamente la misma hora.
_COLOMBIA_FIJA = datetime.timezone(datetime.timedelta(hours=-5), "COT")


def zona_local():
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(ZONA_HORARIA)
    except Exception:
        return _COLOMBIA_FIJA


def fecha_colombia(t, con_anio=False):
    """'24/09 10:15 a. m.' en hora de Colombia (sin depender del idioma del
    servidor para el a. m. / p. m.)."""
    if t is None:
        return "?"
    local = t.astimezone(zona_local())
    fecha = local.strftime("%d/%m/%Y" if con_anio else "%d/%m")
    hora12 = local.hour % 12 or 12
    sufijo = "a. m." if local.hour < 12 else "p. m."
    return f"{fecha} {hora12:02d}:{local.minute:02d} {sufijo}"


def hora_legible(iso_texto):
    t = parse_iso(iso_texto)
    if t is None:
        return str(iso_texto or "?")
    return fecha_colombia(t)


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

def construir_mensaje(first_name, phone, lat, lon, puntos, reason, enlace=None,
                      inicio_guardia=None):
    coaccion = (reason == "duress_pin_coaccion")
    prueba = (reason == "prueba")
    boton = (reason == "boton_emergencia")

    # Si el disparo no trajo coordenadas, se usa el punto mas reciente del
    # rastro en vez de mandar "Ubicacion no disponible".
    if (lat is None or lon is None) and puntos:
        lat = puntos[0]["latitude"]
        lon = puntos[0]["longitude"]

    ubicacion = url_mapa(lat, lon) if lat is not None and lon is not None \
        else "Ubicacion no disponible"

    # Todas las horas del mensaje van en hora de Colombia.
    horas = f"Hora de la alerta: {fecha_colombia(ahora_utc(), True)} (hora Colombia)"
    if inicio_guardia is not None:
        horas += (f"\nGuardia activada: {fecha_colombia(inicio_guardia, True)}"
                  " (hora Colombia)")

    if prueba:
        encabezado = (
            "PRUEBA DEL SISTEMA SENTINEL - NO ES UNA EMERGENCIA\n\n"
            f"De: {first_name} ({phone})\n"
            "Esto es solo una prueba del mensaje, la llamada y el enlace de\n"
            "seguimiento. No tienes que hacer nada.\n"
            f"{horas}"
        )
    elif coaccion:
        encabezado = (
            "ALERTA DE COACCION - ESTA PERSONA ESTA SIENDO OBLIGADA\n\n"
            f"De: {first_name} ({phone})\n"
            "Motivo: activo su PIN de emergencia bajo amenaza.\n"
            f"{horas}\n\n"
            "NO la llames ni le escribas: quien la retiene podria estar\n"
            "mirando su telefono y eso la pondria en mas peligro.\n"
            "Llama al 911 y entrega la ubicacion de abajo."
        )
    elif boton:
        encabezado = (
            "ALERTA DE EMERGENCIA - PULSO EL BOTON DE AUXILIO\n\n"
            f"De: {first_name} ({phone})\n"
            "Motivo: activo manualmente el boton de emergencia de Sentinel.\n"
            f"{horas}\n\n"
            "Puede estar en peligro y con alguien mirando su telefono: antes\n"
            "de llamarla, revisa el enlace de seguimiento. Si no puedes\n"
            "confirmar que esta bien, llama al 911 y entrega la ubicacion."
        )
    else:
        encabezado = (
            "ALERTA TEMPRANA DE EMERGENCIA\n\n"
            f"De: {first_name} ({phone})\n"
            "Motivo: cronometro de seguridad vencido sin respuesta.\n"
            f"{horas}\n\n"
            "Contacta a la persona. Si no contesta, llama al 911."
        )

    vence_enlace = "El enlace deja de funcionar a los 3 dias. "
    if inicio_guardia is not None:
        vence_enlace = (
            "El enlace deja de funcionar el "
            f"{fecha_colombia(inicio_guardia + datetime.timedelta(days=3), True)}"
            " (hora Colombia). ")

    seguimiento = ""
    if enlace:
        seguimiento = (
            "SEGUIMIENTO EN VIVO (mapa con el recorrido, se actualiza solo):\n"
            f"{enlace}\n"
            f"{vence_enlace}No lo compartas.\n\n"
        )

    return (
        f"{encabezado}\n\n"
        f"{seguimiento}"
        f"Ultima ubicacion conocida:\n{ubicacion}\n\n"
        f"Recorrido desde que activo la guardia (hora Colombia, lo mas "
        f"reciente primero):\n"
        f"{formatear_rastro(puntos)}"
    )


# ---------------------------------------------------------------------------
# Telethon
# ---------------------------------------------------------------------------

# Versiones del protocolo de llamadas que usan las apps actuales de Telegram.
# Con la version vieja ('1.0.0', capa 92 fija) el servidor aceptaba la llamada
# pero el telefono del contacto podia no llegar a timbrar.
PROTOCOLO_LLAMADA = PhoneCallProtocol(
    udp_p2p=True, udp_reflector=True,
    min_layer=65, max_layer=92,
    library_versions=["2.4.4", "2.7.7", "5.0.0", "6.0.0", "7.0.0",
                      "8.0.0", "9.0.0", "10.0.0", "11.0.0"],
)

# Cuanto timbra como maximo (Telegram corta solo a los ~45 s).
SEGUNDOS_TIMBRE = int(os.environ.get("SEGUNDOS_TIMBRE", "30"))

# Esperas por limite de Telegram (FloodWait) que se aguantan en vez de fallar.
ESPERA_MAXIMA_FLOOD = 30


def solo_digitos(numero):
    return re.sub(r"\D", "", str(numero or ""))


def guardar_cache_contacto(contacto, usuario):
    """Guarda id y access_hash de Telegram del contacto: los proximos despachos
    ya no tienen que buscarlo (esa busqueda es la que Telegram limita)."""
    cid = contacto.get("id")
    if not cid:
        return
    supabase_request(f"emergency_contacts?id=eq.{cid}", method="PATCH", payload={
        "telegram_user_id": f"{usuario.id}:{usuario.access_hash}"})


def borrar_cache_contacto(contacto):
    cid = contacto.get("id")
    if cid:
        supabase_request(f"emergency_contacts?id=eq.{cid}", method="PATCH",
                         payload={"telegram_user_id": None})


async def llamar_con_espera(client, peticion):
    """Ejecuta la peticion; si Telegram pide esperar poco, espera y reintenta."""
    try:
        return await client(peticion)
    except errors.FloodWaitError as e:
        if e.seconds > ESPERA_MAXIMA_FLOOD:
            raise
        print(f"[Telethon] FloodWait {e.seconds}s, esperando...")
        await asyncio.sleep(e.seconds + 1)
        return await client(peticion)


class Resolutor:
    """Encuentra al contacto en Telegram gastando lo menos posible.

    1. id guardado en emergency_contacts.telegram_user_id (sin pedir nada).
    2. Lista de contactos de la cuenta, UNA vez por despacho.
    3. Importar el numero como contacto (si no estaba en la lista).
    """

    def __init__(self, client):
        self.client = client
        self._por_telefono = None

    async def _lista(self):
        if self._por_telefono is None:
            self._por_telefono = {}
            res = await llamar_con_espera(self.client, GetContactsRequest(hash=0))
            for u in getattr(res, "users", []):
                if getattr(u, "phone", None):
                    self._por_telefono[solo_digitos(u.phone)] = u
        return self._por_telefono

    async def resolver(self, contacto, ignorar_cache=False):
        cache = (contacto.get("telegram_user_id") or "").strip()
        if not ignorar_cache and re.fullmatch(r"\d+:-?\d+", cache):
            uid, ah = cache.split(":")
            return InputPeerUser(int(uid), int(ah)), "cache"

        numero = solo_digitos(contacto.get("phone_number"))
        usuario = None
        try:
            usuario = (await self._lista()).get(numero)
        except errors.FloodWaitError as e:
            print(f"[Telethon] Lista de contactos limitada ({e.seconds}s); "
                  "se intenta importar el numero.")

        origen = "lista_contactos"
        if usuario is None:
            res = await llamar_con_espera(self.client, ImportContactsRequest([
                InputPhoneContact(client_id=random.randint(1, 2**31),
                                  phone="+" + numero,
                                  first_name=contacto.get("name") or "Contacto",
                                  last_name="")]))
            usuario = res.users[0] if res.users else None
            origen = "importado"

        if usuario is None:
            raise ValueError(f"El numero {numero} no tiene Telegram o no se "
                             "pudo encontrar.")
        guardar_cache_contacto(contacto, usuario)
        return InputPeerUser(usuario.id, usuario.access_hash), origen


async def timbrar(client, peer, resultados):
    """Llama al contacto y registra si el telefono de verdad recibio la
    llamada (Telegram lo confirma con receive_date)."""
    estado = {"id": None, "recibida": False, "contestada": False, "fin": None}
    terminado = asyncio.Event()
    t0 = time.monotonic()
    eventos = []  # (segundos desde la solicitud, que paso)

    def anotar(que):
        eventos.append(f"{time.monotonic() - t0:.1f}s {que}")

    async def al_actualizar(update):
        if not isinstance(update, UpdatePhoneCall):
            return
        pc = update.phone_call
        if estado["id"] is not None and getattr(pc, "id", None) != estado["id"]:
            return
        detalle = type(pc).__name__
        if isinstance(pc, PhoneCallWaiting):
            detalle += " recibida" if pc.receive_date else " sin_recibir"
        if isinstance(pc, PhoneCallDiscarded) and pc.reason:
            detalle += ":" + type(pc.reason).__name__
        anotar(detalle)
        if isinstance(pc, PhoneCallWaiting) and pc.receive_date:
            estado["recibida"] = True
        elif isinstance(pc, PhoneCallAccepted):
            estado["recibida"] = True
            estado["contestada"] = True
            terminado.set()
        elif isinstance(pc, PhoneCallDiscarded):
            estado["fin"] = type(pc.reason).__name__ if pc.reason else "sin_motivo"
            terminado.set()

    client.add_event_handler(al_actualizar, events.Raw)
    try:
        g_a = secrets.token_bytes(256)
        res = await llamar_con_espera(client, RequestCallRequest(
            user_id=peer,
            random_id=random.randint(0, 0x7fffffff),
            g_a_hash=hashlib.sha256(g_a).digest(),
            protocol=PROTOCOLO_LLAMADA,
            video=False,
        ))
        llamada = res.phone_call
        estado["id"] = llamada.id
        anotar("solicitada " + type(llamada).__name__)
        if isinstance(llamada, PhoneCallWaiting) and llamada.receive_date:
            estado["recibida"] = True
        resultados["llamada"] = "solicitada"
        print("[Telethon] Llamada solicitada, esperando que timbre...")

        try:
            await asyncio.wait_for(terminado.wait(), timeout=SEGUNDOS_TIMBRE)
        except asyncio.TimeoutError:
            pass

        if estado["fin"] is None:
            try:
                await client(DiscardCallRequest(
                    peer=InputPhoneCall(id=llamada.id,
                                        access_hash=llamada.access_hash),
                    duration=0,
                    reason=PhoneCallDiscardReasonDisconnect(),
                    connection_id=0,
                ))
            except Exception as e:
                print(f"[Telethon] No se pudo colgar: {e}")
    finally:
        client.remove_event_handler(al_actualizar, events.Raw)

    if estado["contestada"]:
        resultados["llamada"] = "contestada"
    elif estado["fin"] == "PhoneCallDiscardReasonBusy":
        resultados["llamada"] = "rechazada_por_el_contacto"
    elif estado["recibida"]:
        resultados["llamada"] = "timbro_en_el_telefono"
    else:
        resultados["llamada"] = "sin_confirmacion_de_timbre"
    if estado["fin"]:
        resultados["llamada_fin"] = estado["fin"]
    resultados["llamada_recibida_por_el_telefono"] = estado["recibida"]
    resultados["llamada_eventos"] = eventos


async def verificar_historial(client, peer, resultados):
    """Busca en el chat la llamada perdida que deja Telegram: prueba de que la
    llamada quedo registrada en el telefono del contacto."""
    try:
        for m in await client.get_messages(peer, limit=5):
            accion = getattr(m, "action", None)
            if isinstance(accion, MessageActionPhoneCall):
                motivo = type(accion.reason).__name__ if accion.reason else "?"
                resultados["registro_llamada_en_chat"] = motivo
                return
        resultados["registro_llamada_en_chat"] = "no_encontrado"
    except Exception as e:
        resultados["registro_llamada_en_chat"] = f"error: {e}"


async def notificar_contactos(api_id, api_hash, session_string, contactos,
                              mensaje_texto):
    """Una sola conexion de Telegram para todos los contactos del despacho."""
    logs = []
    client = TelegramClient(StringSession(session_string), int(api_id),
                            str(api_hash))
    await client.connect()
    try:
        if not await client.is_user_authorized():
            return [{"contact": c.get("phone_number"),
                     "result": {"error": "La sesion de Telegram del usuario ya "
                                         "no es valida: hay que volver a "
                                         "generarla."}} for c in contactos]
        # Para recibir los eventos de la llamada (timbro, contesto, colgo).
        try:
            await client(GetStateRequest())
        except Exception:
            pass

        resolutor = Resolutor(client)
        for c in contactos:
            numero = c.get("phone_number")
            resultados = {}
            try:
                peer, origen = await resolutor.resolver(c)
                resultados["contacto_encontrado_por"] = origen
                try:
                    if mensaje_texto:
                        msg = await client.send_message(peer, mensaje_texto)
                except (errors.PeerIdInvalidError, errors.UserIdInvalidError,
                        ValueError):
                    # El id guardado ya no sirve (otra cuenta/sesion): se
                    # busca de nuevo.
                    borrar_cache_contacto(c)
                    peer, origen = await resolutor.resolver(c, ignorar_cache=True)
                    resultados["contacto_encontrado_por"] = origen
                    msg = await client.send_message(peer, mensaje_texto)
                resultados["mensaje"] = "enviado"
                resultados["mensaje_id"] = msg.id
                print(f"[Telethon] Mensaje entregado a {numero}")

                try:
                    await timbrar(client, peer, resultados)
                except errors.UserPrivacyRestrictedError:
                    resultados["llamada"] = ("bloqueada_por_privacidad: el "
                                             "contacto no acepta llamadas de "
                                             "esta cuenta")
                except Exception as e:
                    resultados["llamada"] = f"error: {e}"

                await verificar_historial(client, peer, resultados)
            except Exception as err:
                print(f"[Telethon Error Contacto {numero}]: {err}")
                resultados["error"] = str(err)
            logs.append({"contact": numero, "result": resultados})
    finally:
        await client.disconnect()
    return logs


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
    contacts = [c for c in contacts if c.get("phone_number")]

    lat = timer.get("last_latitude")
    lon = timer.get("last_longitude")

    sesion = obtener_sesion(user_id, timer)
    desde = parse_iso(sesion.get("started_at")) if sesion else \
        parse_iso(timer.get("started_at"))
    puntos = obtener_rastro(user_id, desde)
    enlace = crear_enlace(sesion)

    sos_msg = construir_mensaje(first_name, phone, lat, lon, puntos, reason,
                                enlace, inicio_guardia=desde)

    if not (api_id and api_hash and session):
        logs = [{"contact": c.get("phone_number"),
                 "result": {"error": "El perfil no tiene configurada la cuenta "
                                     "de Telegram (api_id, api_hash o sesion)."}}
                for c in contacts]
    else:
        try:
            logs = await notificar_contactos(api_id, api_hash, session,
                                             contacts, sos_msg)
        except Exception as e:
            print(f"[Telethon Error general]: {e}")
            logs = [{"contact": c.get("phone_number"),
                     "result": {"error": str(e)}} for c in contacts]

    enviados = sum(1 for l in logs if l["result"].get("mensaje") == "enviado")
    supabase_request("alert_logs", method="POST", payload={
        "user_id": user_id,
        "trigger_type": reason,
        "latitude": lat,
        "longitude": lon,
        "contacts_notified": logs,
        "status": "dispatched" if enviados else "failed",
        "details": f"{enviados}/{len(logs)} contactos con mensaje entregado",
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


def purgar_datos_vencidos():
    """Borra puntos de mas de 3 dias y sesiones (y enlaces) vencidas. Es un
    respaldo del pg_cron horario de la base."""
    if SUPABASE_SERVICE_ROLE_KEY:
        supabase_request("rpc/purge_old_location_history", method="POST",
                         payload={})


def loop_revision_10_segundos():
    """Hilo en segundo plano: revisa cronometros vencidos cada 10 segundos"""
    print("[Sentinel Poller] Iniciado: comprobacion continua cada 10 segundos.")
    ultima_purga = 0.0
    while True:
        try:
            revisar_y_despachar_cronometros_vencidos()
        except Exception as e:
            print(f"[Poller Error]: {e}")
        if time.time() - ultima_purga > 3600:
            ultima_purga = time.time()
            try:
                purgar_datos_vencidos()
            except Exception as e:
                print(f"[Purga Error]: {e}")
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
                "version": VERSION,
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
                    "started_at": data.get("started_at"),
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
