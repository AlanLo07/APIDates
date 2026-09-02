"""
spotify_handler.py — Integracion de Spotify Web API

Flujo de autenticacion:
- Catalogo publico (search, tracks, artists, albums, playlists): Client Credentials,
  token cacheado en memoria del contenedor Lambda.
- Player (reproduccion en tiempo real): Authorization Code flow por usuario. El usuario
  vincula su cuenta via /spotify/login -> /spotify/callback y los tokens (access +
  refresh) se guardan por userId (sub del JWT) en DynamoDB.

Endpoints expuestos:
- GET  /spotify/search?q=...&type=track&limit=10&market=CO
- GET  /spotify/tracks/{id}?market=CO
- GET  /spotify/artists/{id}
- GET  /spotify/artists/{id}/top-tracks?market=CO
- GET  /spotify/artists/{id}/albums?market=CO
- GET  /spotify/albums/{id}?market=CO
- GET  /spotify/albums/{id}/tracks?market=CO
- GET  /spotify/playlists/{id}?market=CO
- GET  /spotify/playlists/{id}/tracks?market=CO
- GET  /spotify/login                          (requiere JWT, devuelve authUrl)
- GET  /spotify/callback                       (publico, redirect de Spotify)
- GET  /spotify/player?market=CO
- GET  /spotify/player/currently-playing?market=CO
- GET  /spotify/player/devices
- PUT  /spotify/player/play
- PUT  /spotify/player/pause
- PUT  /spotify/player/volume?volume_percent=50
- POST /spotify/player/next
- POST /spotify/player/previous

Nota: recommendations/moods/genres se eliminaron porque dependen de endpoints
de Spotify marcados como deprecated (recommendations family).
"""

import base64
import hashlib
import hmac
import json
import logging
import os
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import boto3

from common.utils import build_response

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SPOTIFY_ACCOUNTS_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_AUTHORIZE_URL = "https://accounts.spotify.com/authorize"
SPOTIFY_API_BASE = "https://api.spotify.com/v1"
DEFAULT_MARKET = os.environ.get("SPOTIFY_MARKET", "CO")
SPOTIFY_REDIRECT_URI = os.environ.get("SPOTIFY_REDIRECT_URI", "")
SPOTIFY_STATE_SECRET = os.environ.get("SPOTIFY_STATE_SECRET", "")
SPOTIFY_TOKENS_TABLE = os.environ.get("SPOTIFY_TOKENS_TABLE", "")
PLAYER_SCOPES = "user-read-playback-state user-modify-playback-state user-read-currently-playing"

TOKEN_CACHE: dict[str, str | int] = {
    "access_token": "",
    "expires_at": 0,
}

_dynamodb = boto3.resource("dynamodb")


def _tokens_table():
    return _dynamodb.Table(SPOTIFY_TOKENS_TABLE)


def lambda_handler(event, context):
    method = (event.get("requestContext", {}).get("http", {}).get("method") or "").upper()
    path = event.get("rawPath", "")
    logger.info("⚪️ Spotify request recibida method=%s path=%s", method, path)

    if method == "OPTIONS":
        return build_response(200, {})

    try:
        if method == "GET" and path.endswith("/spotify/login"):
            return _spotify_login(event)

        if method == "GET" and path.endswith("/spotify/callback"):
            return _spotify_callback(event)

        if method == "GET":
            if path.endswith("/spotify/search"):
                return _search(event)

            if path.endswith("/spotify/player/currently-playing"):
                return _get_player_currently_playing(event)

            if path.endswith("/spotify/player/devices"):
                return _get_player_devices(event)

            if path.endswith("/spotify/player"):
                return _get_player_state(event)

            if "/spotify/artists/" in path and path.endswith("/top-tracks"):
                return _get_artist_top_tracks(event)

            if "/spotify/artists/" in path and path.endswith("/albums"):
                return _get_artist_albums(event)

            if path.endswith("/spotify/tracks") or "/spotify/tracks/" in path:
                return _get_track(event)

            if path.endswith("/spotify/artists") or "/spotify/artists/" in path:
                return _get_artist(event)

            if "/spotify/albums/" in path and path.endswith("/tracks"):
                return _get_album_tracks(event)

            if path.endswith("/spotify/albums") or "/spotify/albums/" in path:
                return _get_album(event)

            if "/spotify/playlists/" in path and path.endswith("/tracks"):
                return _get_playlist_tracks(event)

            if path.endswith("/spotify/playlists") or "/spotify/playlists/" in path:
                return _get_playlist(event)

        if method == "PUT":
            if path.endswith("/spotify/player/play"):
                return _player_play(event)

            if path.endswith("/spotify/player/pause"):
                return _player_pause(event)

            if path.endswith("/spotify/player/volume"):
                return _player_volume(event)

        if method == "POST":
            if path.endswith("/spotify/player/next"):
                return _player_next(event)

            if path.endswith("/spotify/player/previous"):
                return _player_previous(event)

        logger.warning("🟡 Ruta/metodo Spotify no encontrado: %s %s", method, path)
        return build_response(404, {"error": "Ruta Spotify no encontrada"})
    except PermissionError as exc:
        logger.warning("🟡 Spotify acceso no autorizado: %s", exc)
        return build_response(401, {"error": str(exc)})
    except ValueError as exc:
        logger.warning("🟡 Validacion Spotify: %s", exc)
        return build_response(400, {"error": str(exc)})
    except HTTPError as exc:
        logger.error("🔴 Spotify HTTPError status=%s", exc.code)
        return build_response(exc.code, {"error": "Error de Spotify API", "status": exc.code})
    except URLError as exc:
        logger.error("🔴 Spotify URLError reason=%s", exc.reason)
        return build_response(502, {"error": "No se pudo conectar con Spotify"})
    except Exception:
        logger.exception("🔴 Error inesperado en SpotifyAPI")
        return build_response(500, {"error": "Error interno del servidor"})


def _search(event):
    query = _get_query(event, "q")
    if not query:
        raise ValueError("Parametro requerido: q")

    search_type = (_get_query(event, "type") or "track").strip().lower()
    allowed_types = {"track", "artist", "album", "playlist"}
    if search_type not in allowed_types:
        raise ValueError(f"type invalido: {search_type}. Opciones: {', '.join(sorted(allowed_types))}")

    limit = _int_param(_get_query(event, "limit"), 10, min_value=1, max_value=50)
    market = (_get_query(event, "market") or DEFAULT_MARKET).strip().upper()

    logger.info("🔵 Search Spotify q_len=%s type=%s limit=%s market=%s", len(query), search_type, limit, market)

    payload = {
        "q": query,
        "type": search_type,
        "limit": limit,
        "offset": 0,
        "market": market,
    }

    data = _spotify_get("/search", payload)
    items = data.get(f"{search_type}s", {}).get("items", [])

    if search_type == "track":
        mapped = [_map_track(item) for item in items]
    elif search_type == "artist":
        mapped = [_map_artist(item) for item in items]
    else:
        mapped = items

    logger.info("🟢 Search Spotify exitoso resultados=%s", len(mapped))
    return build_response(200, {
        "query": query,
        "type": search_type,
        "market": market,
        "total": len(mapped),
        "items": mapped,
    })


def _get_track(event):
    path_id = (event.get("pathParameters") or {}).get("id")
    query_id = _get_query(event, "id")
    track_id = (path_id or query_id or "").strip()
    if not track_id:
        raise ValueError("Parametro requerido: id de track")

    market = (_get_query(event, "market") or DEFAULT_MARKET).strip().upper()
    logger.info("🔵 Get track Spotify id_present=%s market=%s", bool(track_id), market)

    data = _spotify_get(f"/tracks/{track_id}", {"market": market})
    return build_response(200, _map_track(data))


def _get_artist(event):
    path_id = (event.get("pathParameters") or {}).get("id")
    query_id = _get_query(event, "id")
    artist_id = (path_id or query_id or "").strip()
    if not artist_id:
        raise ValueError("Parametro requerido: id de artist")

    logger.info("🔵 Get artist Spotify id_present=%s", bool(artist_id))

    data = _spotify_get(f"/artists/{artist_id}")
    return build_response(200, _map_artist(data))


def _get_artist_top_tracks(event):
    path_params = event.get("pathParameters") or {}
    path_id = path_params.get("id")
    query_id = _get_query(event, "id")
    query_artist_id = _get_query(event, "artistId")
    artist_id = (path_id or query_artist_id or query_id or "").strip()
    if not artist_id:
        raise ValueError("Parametro requerido: id de artist")

    market = (_get_query(event, "market") or DEFAULT_MARKET).strip().upper()
    limit = _int_param(_get_query(event, "limit"), 10, min_value=1, max_value=50)

    logger.info("🔵 Get artist top tracks id_present=%s market=%s limit=%s", bool(artist_id), market, limit)

    data = _spotify_get(f"/artists/{artist_id}/top-tracks", {"market": market})
    tracks = [_map_track(item) for item in (data.get("tracks") or [])]

    logger.info("🟢 Artist top tracks Spotify exitoso total=%s", len(tracks))
    return build_response(200, {
        "artist_id": artist_id,
        "market": market,
        "total": min(len(tracks), limit),
        "tracks": tracks[:limit],
    })


def _get_artist_albums(event):
    path_params = event.get("pathParameters") or {}
    path_id = path_params.get("id")
    query_id = _get_query(event, "id")
    query_artist_id = _get_query(event, "artistId")
    artist_id = (path_id or query_artist_id or query_id or "").strip()
    if not artist_id:
        raise ValueError("Parametro requerido: id de artist")

    market = (_get_query(event, "market") or DEFAULT_MARKET).strip().upper()
    limit = _int_param(_get_query(event, "limit"), 20, min_value=1, max_value=50)
    offset = _int_param(_get_query(event, "offset"), 0, min_value=0, max_value=1000)
    include_groups = (_get_query(event, "include_groups") or "album,single").strip()

    logger.info("🔵 Get artist albums id_present=%s market=%s limit=%s", bool(artist_id), market, limit)

    data = _spotify_get(f"/artists/{artist_id}/albums", {
        "market": market,
        "limit": limit,
        "offset": offset,
        "include_groups": include_groups,
    })
    albums = [_map_album(item) for item in (data.get("items") or [])]

    logger.info("🟢 Artist albums Spotify exitoso total=%s", len(albums))
    return build_response(200, {
        "artist_id": artist_id,
        "market": market,
        "total": data.get("total", len(albums)),
        "limit": limit,
        "offset": offset,
        "albums": albums,
    })


def _get_album(event):
    path_id = (event.get("pathParameters") or {}).get("id")
    query_id = _get_query(event, "id")
    album_id = (path_id or query_id or "").strip()
    if not album_id:
        raise ValueError("Parametro requerido: id de album")

    market = (_get_query(event, "market") or DEFAULT_MARKET).strip().upper()
    logger.info("🔵 Get album Spotify id_present=%s market=%s", bool(album_id), market)

    data = _spotify_get(f"/albums/{album_id}", {"market": market})
    return build_response(200, _map_album(data))


def _get_album_tracks(event):
    path_params = event.get("pathParameters") or {}
    path_id = path_params.get("id")
    query_id = _get_query(event, "id")
    query_album_id = _get_query(event, "albumId")
    album_id = (path_id or query_album_id or query_id or "").strip()
    if not album_id:
        raise ValueError("Parametro requerido: id de album")

    market = (_get_query(event, "market") or DEFAULT_MARKET).strip().upper()
    limit = _int_param(_get_query(event, "limit"), 20, min_value=1, max_value=50)
    offset = _int_param(_get_query(event, "offset"), 0, min_value=0, max_value=1000)

    logger.info("🔵 Get album tracks id_present=%s market=%s limit=%s", bool(album_id), market, limit)

    data = _spotify_get(f"/albums/{album_id}/tracks", {"market": market, "limit": limit, "offset": offset})
    tracks = [_map_track(item) for item in (data.get("items") or [])]

    logger.info("🟢 Album tracks Spotify exitoso total=%s", len(tracks))
    return build_response(200, {
        "album_id": album_id,
        "market": market,
        "total": data.get("total", len(tracks)),
        "limit": limit,
        "offset": offset,
        "tracks": tracks,
    })


def _get_playlist(event):
    path_id = (event.get("pathParameters") or {}).get("id")
    query_id = _get_query(event, "id")
    playlist_id = (path_id or query_id or "").strip()
    if not playlist_id:
        raise ValueError("Parametro requerido: id de playlist")

    market = (_get_query(event, "market") or DEFAULT_MARKET).strip().upper()
    fields = "id,name,description,owner(display_name),images,external_urls,followers,tracks.total"
    logger.info("🔵 Get playlist Spotify id_present=%s market=%s", bool(playlist_id), market)

    data = _spotify_get(f"/playlists/{playlist_id}", {"market": market, "fields": fields})
    return build_response(200, _map_playlist(data))


def _get_playlist_tracks(event):
    path_params = event.get("pathParameters") or {}
    path_id = path_params.get("id")
    query_id = _get_query(event, "id")
    query_playlist_id = _get_query(event, "playlistId")
    playlist_id = (path_id or query_playlist_id or query_id or "").strip()
    if not playlist_id:
        raise ValueError("Parametro requerido: id de playlist")

    market = (_get_query(event, "market") or DEFAULT_MARKET).strip().upper()
    limit = _int_param(_get_query(event, "limit"), 20, min_value=1, max_value=50)
    offset = _int_param(_get_query(event, "offset"), 0, min_value=0, max_value=1000)

    logger.info("🔵 Get playlist tracks id_present=%s market=%s limit=%s", bool(playlist_id), market, limit)

    data = _spotify_get(f"/playlists/{playlist_id}/tracks", {
        "market": market,
        "limit": limit,
        "offset": offset,
        "fields": "total,items(track(id,name,artists(name),album(name,images),external_urls,uri))",
    })
    items = data.get("items") or []
    tracks = [_map_track(item.get("track") or {}) for item in items if item.get("track")]

    logger.info("🟢 Playlist tracks Spotify exitoso total=%s", len(tracks))
    return build_response(200, {
        "playlist_id": playlist_id,
        "market": market,
        "total": data.get("total", len(tracks)),
        "limit": limit,
        "offset": offset,
        "tracks": tracks,
    })


# ─── Authorization Code flow (vinculacion de cuenta de usuario) ─────────────


def _require_user_id(event) -> str:
    claims = (
        event.get("requestContext", {})
        .get("authorizer", {})
        .get("jwt", {})
        .get("claims", {})
    )
    user_id = (claims.get("sub") or "").strip()
    if not user_id:
        raise PermissionError("Usuario no autenticado")
    return user_id


def _sign_state(user_id: str) -> str:
    if not SPOTIFY_STATE_SECRET:
        raise ValueError("Falta configurar SPOTIFY_STATE_SECRET")

    payload = base64.urlsafe_b64encode(user_id.encode("utf-8")).decode("utf-8").rstrip("=")
    signature = hmac.new(SPOTIFY_STATE_SECRET.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{payload}.{signature}"


def _verify_state(state: str) -> str:
    if not SPOTIFY_STATE_SECRET:
        raise ValueError("Falta configurar SPOTIFY_STATE_SECRET")

    try:
        payload, signature = state.split(".", 1)
    except ValueError as exc:
        raise ValueError("state invalido") from exc

    expected_signature = hmac.new(SPOTIFY_STATE_SECRET.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected_signature):
        raise ValueError("state invalido o alterado")

    padding = "=" * (-len(payload) % 4)
    return base64.urlsafe_b64decode((payload + padding).encode("utf-8")).decode("utf-8")


def _spotify_login(event):
    user_id = _require_user_id(event)
    if not SPOTIFY_REDIRECT_URI:
        raise ValueError("Falta configurar SPOTIFY_REDIRECT_URI")

    client_id = (os.environ.get("SPOTIFY_CLIENT_ID") or "").strip()
    state = _sign_state(user_id)
    params = {
        "client_id": client_id,
        "response_type": "code",
        "redirect_uri": SPOTIFY_REDIRECT_URI,
        "scope": PLAYER_SCOPES,
        "state": state,
        "show_dialog": "false",
    }
    auth_url = f"{SPOTIFY_AUTHORIZE_URL}?{urlencode(params)}"

    logger.info("🟢 Spotify login URL generada user_id_present=%s", bool(user_id))
    return build_response(200, {"authUrl": auth_url})


def _spotify_callback(event):
    error = _get_query(event, "error")
    if error:
        logger.warning("🟡 Spotify callback con error=%s", error)
        return build_response(400, {"error": f"Spotify rechazo la autorizacion: {error}"})

    code = _get_query(event, "code")
    state = _get_query(event, "state")
    if not code or not state:
        raise ValueError("Parametros requeridos: code, state")

    user_id = _verify_state(state)

    client_id = (os.environ.get("SPOTIFY_CLIENT_ID") or "").strip()
    client_secret = (os.environ.get("SPOTIFY_CLIENT_SECRET") or "").strip()
    if not client_id or not client_secret or not SPOTIFY_REDIRECT_URI:
        raise ValueError("Faltan variables de configuracion de Spotify")

    auth_raw = f"{client_id}:{client_secret}".encode("utf-8")
    auth_header = base64.b64encode(auth_raw).decode("utf-8")

    body = urlencode({
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": SPOTIFY_REDIRECT_URI,
    }).encode("utf-8")

    req = Request(
        url=SPOTIFY_ACCOUNTS_URL,
        data=body,
        headers={
            "Authorization": f"Basic {auth_header}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )

    with urlopen(req, timeout=10) as response:
        token_data = json.loads(response.read().decode("utf-8"))

    access_token = token_data.get("access_token")
    refresh_token = token_data.get("refresh_token")
    expires_in = int(token_data.get("expires_in", 3600))
    scope = token_data.get("scope", "")

    if not access_token or not refresh_token:
        raise ValueError("Spotify no retorno tokens de usuario validos")

    _save_user_tokens(user_id, access_token, refresh_token, expires_in, scope)
    logger.info("🟢 Cuenta Spotify vinculada user_id_present=%s", bool(user_id))
    return build_response(200, {"message": "Cuenta de Spotify vinculada correctamente"})


def _save_user_tokens(user_id: str, access_token: str, refresh_token: str, expires_in: int, scope: str) -> None:
    now = int(time.time())
    _tokens_table().put_item(Item={
        "userId": user_id,
        "access_token": access_token,
        "refresh_token": refresh_token,
        "expires_at": now + int(expires_in),
        "scope": scope,
        "updated_at": now,
    })


def _update_user_access_token(user_id: str, access_token: str, expires_in: int) -> None:
    now = int(time.time())
    _tokens_table().update_item(
        Key={"userId": user_id},
        UpdateExpression="SET access_token = :at, expires_at = :ea, updated_at = :ua",
        ExpressionAttributeValues={
            ":at": access_token,
            ":ea": now + int(expires_in),
            ":ua": now,
        },
    )


def _get_user_access_token(user_id: str) -> str:
    item = _tokens_table().get_item(Key={"userId": user_id}).get("Item")
    if not item:
        raise ValueError("Usuario no ha vinculado su cuenta de Spotify. Usa /spotify/login primero")

    now = int(time.time())
    access_token = item.get("access_token")
    expires_at = int(item.get("expires_at") or 0)

    if access_token and now < (expires_at - 30):
        return access_token

    refresh_token = item.get("refresh_token")
    if not refresh_token:
        raise ValueError("No hay refresh_token disponible, vuelve a vincular tu cuenta de Spotify")

    client_id = (os.environ.get("SPOTIFY_CLIENT_ID") or "").strip()
    client_secret = (os.environ.get("SPOTIFY_CLIENT_SECRET") or "").strip()
    auth_raw = f"{client_id}:{client_secret}".encode("utf-8")
    auth_header = base64.b64encode(auth_raw).decode("utf-8")

    body = urlencode({
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }).encode("utf-8")

    req = Request(
        url=SPOTIFY_ACCOUNTS_URL,
        data=body,
        headers={
            "Authorization": f"Basic {auth_header}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )

    with urlopen(req, timeout=10) as response:
        token_data = json.loads(response.read().decode("utf-8"))

    new_access_token = token_data.get("access_token")
    new_expires_in = int(token_data.get("expires_in", 3600))
    new_refresh_token = token_data.get("refresh_token")

    if not new_access_token:
        raise ValueError("No se pudo renovar el token de usuario de Spotify")

    if new_refresh_token:
        _save_user_tokens(user_id, new_access_token, new_refresh_token, new_expires_in, item.get("scope", ""))
    else:
        _update_user_access_token(user_id, new_access_token, new_expires_in)

    logger.info("🟢 Token usuario Spotify renovado user_id_present=%s", bool(user_id))
    return new_access_token


def _spotify_user_request(method: str, path: str, user_id: str, params: dict | None = None, body: dict | None = None) -> dict:
    token = _get_user_access_token(user_id)
    query_string = f"?{urlencode(params)}" if params else ""
    url = f"{SPOTIFY_API_BASE}{path}{query_string}"
    data = json.dumps(body).encode("utf-8") if body is not None else None

    req = Request(
        url=url,
        data=data,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method=method,
    )

    with urlopen(req, timeout=10) as response:
        raw = response.read()
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))


# ─── Player (requiere cuenta de usuario vinculada) ──────────────────────────


def _get_player_state(event):
    user_id = _require_user_id(event)
    market = (_get_query(event, "market") or DEFAULT_MARKET).strip().upper()

    logger.info("🔵 Get player state user_id_present=%s", bool(user_id))
    data = _spotify_user_request("GET", "/me/player", user_id, {"market": market}) or {}

    logger.info("🟢 Player state obtenido is_playing=%s", data.get("is_playing"))
    return build_response(200, data)


def _get_player_currently_playing(event):
    user_id = _require_user_id(event)
    market = (_get_query(event, "market") or DEFAULT_MARKET).strip().upper()

    logger.info("🔵 Get player currently-playing user_id_present=%s", bool(user_id))
    data = _spotify_user_request("GET", "/me/player/currently-playing", user_id, {"market": market}) or {}
    return build_response(200, data)


def _get_player_devices(event):
    user_id = _require_user_id(event)
    logger.info("🔵 Get player devices user_id_present=%s", bool(user_id))

    data = _spotify_user_request("GET", "/me/player/devices", user_id) or {}
    devices = data.get("devices") or []

    logger.info("🟢 Player devices obtenidos total=%s", len(devices))
    return build_response(200, {"total": len(devices), "devices": devices})


def _player_play(event):
    user_id = _require_user_id(event)
    device_id = _get_query(event, "device_id")
    context_uri = _get_query(event, "context_uri")
    uris_raw = _get_query(event, "uris")

    body: dict = {}
    if context_uri:
        body["context_uri"] = context_uri
    if uris_raw:
        body["uris"] = [uri.strip() for uri in uris_raw.split(",") if uri.strip()]

    params = {"device_id": device_id} if device_id else None

    logger.info("🔵 Player play user_id_present=%s device_present=%s", bool(user_id), bool(device_id))
    _spotify_user_request("PUT", "/me/player/play", user_id, params, body or None)

    logger.info("🟢 Player play ejecutado")
    return build_response(200, {"message": "Reproduccion iniciada"})


def _player_pause(event):
    user_id = _require_user_id(event)
    device_id = _get_query(event, "device_id")
    params = {"device_id": device_id} if device_id else None

    logger.info("🔵 Player pause user_id_present=%s", bool(user_id))
    _spotify_user_request("PUT", "/me/player/pause", user_id, params)

    logger.info("🟢 Player pausado")
    return build_response(200, {"message": "Reproduccion pausada"})


def _player_next(event):
    user_id = _require_user_id(event)
    device_id = _get_query(event, "device_id")
    params = {"device_id": device_id} if device_id else None

    logger.info("🔵 Player next user_id_present=%s", bool(user_id))
    _spotify_user_request("POST", "/me/player/next", user_id, params)

    logger.info("🟢 Player siguiente pista")
    return build_response(200, {"message": "Siguiente pista"})


def _player_previous(event):
    user_id = _require_user_id(event)
    device_id = _get_query(event, "device_id")
    params = {"device_id": device_id} if device_id else None

    logger.info("🔵 Player previous user_id_present=%s", bool(user_id))
    _spotify_user_request("POST", "/me/player/previous", user_id, params)

    logger.info("🟢 Player pista anterior")
    return build_response(200, {"message": "Pista anterior"})


def _player_volume(event):
    user_id = _require_user_id(event)
    volume = _int_param(_get_query(event, "volume_percent"), default_value=50, min_value=0, max_value=100)
    device_id = _get_query(event, "device_id")

    params: dict = {"volume_percent": volume}
    if device_id:
        params["device_id"] = device_id

    logger.info("🔵 Player volume user_id_present=%s volume=%s", bool(user_id), volume)
    _spotify_user_request("PUT", "/me/player/volume", user_id, params)

    logger.info("🟢 Player volumen ajustado")
    return build_response(200, {"message": "Volumen ajustado", "volume_percent": volume})


def _spotify_get(path: str, params: dict | None = None) -> dict:
    token = _get_access_token()
    query_string = f"?{urlencode(params)}" if params else ""
    url = f"{SPOTIFY_API_BASE}{path}{query_string}"

    req = Request(
        url=url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="GET",
    )

    with urlopen(req, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def _get_access_token() -> str:
    now = int(time.time())
    token = str(TOKEN_CACHE.get("access_token") or "")
    expires_at = int(TOKEN_CACHE.get("expires_at") or 0)

    if token and now < (expires_at - 30):
        return token

    client_id = (os.environ.get("SPOTIFY_CLIENT_ID") or "").strip()
    client_secret = (os.environ.get("SPOTIFY_CLIENT_SECRET") or "").strip()

    if not client_id or not client_secret:
        raise ValueError("Faltan variables SPOTIFY_CLIENT_ID o SPOTIFY_CLIENT_SECRET")

    auth_raw = f"{client_id}:{client_secret}".encode("utf-8")
    auth_header = base64.b64encode(auth_raw).decode("utf-8")

    req = Request(
        url=SPOTIFY_ACCOUNTS_URL,
        data=b"grant_type=client_credentials",
        headers={
            "Authorization": f"Basic {auth_header}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )

    with urlopen(req, timeout=10) as response:
        body = json.loads(response.read().decode("utf-8"))

    access_token = body.get("access_token")
    expires_in = int(body.get("expires_in", 3600))

    if not access_token:
        raise ValueError("Spotify no retorno access_token")

    TOKEN_CACHE["access_token"] = access_token
    TOKEN_CACHE["expires_at"] = now + expires_in
    logger.info("🟢 Token Spotify renovado ttl=%s", expires_in)
    return access_token


def _get_query(event, key: str) -> str | None:
    return (event.get("queryStringParameters") or {}).get(key)


def _int_param(raw_value: str | None, default_value: int, min_value: int, max_value: int) -> int:
    if raw_value is None or not str(raw_value).strip():
        return default_value

    try:
        parsed = int(str(raw_value))
    except ValueError as exc:
        raise ValueError(f"Parametro invalido, se esperaba entero: {raw_value}") from exc

    return max(min_value, min(max_value, parsed))


def _float_param(raw_value: str | None, min_value: float, max_value: float) -> float:
    if raw_value is None or not str(raw_value).strip():
        raise ValueError("Parametro invalido, se esperaba decimal")

    try:
        parsed = float(str(raw_value))
    except ValueError as exc:
        raise ValueError(f"Parametro invalido, se esperaba decimal: {raw_value}") from exc

    return max(min_value, min(max_value, parsed))


def _map_track(track: dict) -> dict:
    artists = [artist.get("name") for artist in track.get("artists", []) if artist.get("name")]
    album_images = (track.get("album", {}).get("images") or [])
    image = album_images[0].get("url") if album_images else None

    return {
        "id": track.get("id"),
        "name": track.get("name"),
        "artists": artists,
        "album": (track.get("album") or {}).get("name"),
        "url": (track.get("external_urls") or {}).get("spotify"),
        "uri": track.get("uri"),
        "image": image,
    }


def _map_artist(artist: dict) -> dict:
    images = artist.get("images") or []
    image = images[0].get("url") if images else None
    return {
        "id": artist.get("id"),
        "name": artist.get("name"),
        "genres": artist.get("genres", []),
        "followers": (artist.get("followers") or {}).get("total"),
        "url": (artist.get("external_urls") or {}).get("spotify"),
        "uri": artist.get("uri"),
        "image": image,
    }


def _map_album(album: dict) -> dict:
    images = album.get("images") or []
    image = images[0].get("url") if images else None
    artists = [artist.get("name") for artist in album.get("artists", []) if artist.get("name")]

    return {
        "id": album.get("id"),
        "name": album.get("name"),
        "artists": artists,
        "release_date": album.get("release_date"),
        "total_tracks": album.get("total_tracks"),
        "album_type": album.get("album_type"),
        "url": (album.get("external_urls") or {}).get("spotify"),
        "uri": album.get("uri"),
        "image": image,
    }


def _map_playlist(playlist: dict) -> dict:
    images = playlist.get("images") or []
    image = images[0].get("url") if images else None

    return {
        "id": playlist.get("id"),
        "name": playlist.get("name"),
        "description": playlist.get("description"),
        "owner": (playlist.get("owner") or {}).get("display_name"),
        "total_tracks": (playlist.get("tracks") or {}).get("total"),
        "followers": (playlist.get("followers") or {}).get("total"),
        "url": (playlist.get("external_urls") or {}).get("spotify"),
        "image": image,
    }
