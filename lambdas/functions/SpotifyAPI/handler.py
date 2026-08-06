"""
spotify_handler.py — Integracion de Spotify Web API (solo endpoints GET)

Flujo de autenticacion:
- Usa Client Credentials (sin login de usuario final).
- El token se solicita en backend y se cachea en memoria del contenedor Lambda.

Endpoints expuestos:
- GET /spotify/search?q=...&type=track&limit=10&market=CO
- GET /spotify/recommendations?q=...&limit=20&market=CO&mood=romantico
- GET /spotify/moods
- GET /spotify/genres?q=pop
- GET /spotify/tracks/{id}?market=CO
- GET /spotify/artists/{id}
- GET /spotify/artists/{id}/top-tracks?market=CO
"""

import base64
import json
import logging
import os
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from common.utils import build_response

logger = logging.getLogger()
logger.setLevel(logging.INFO)

SPOTIFY_ACCOUNTS_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_API_BASE = "https://api.spotify.com/v1"
DEFAULT_MARKET = os.environ.get("SPOTIFY_MARKET", "CO")

TOKEN_CACHE: dict[str, str | int] = {
    "access_token": "",
    "expires_at": 0,
}

MOOD_PRESETS: dict[str, dict] = {
    "romantico": {
        "target_valence": 0.75,
        "target_energy": 0.45,
        "target_acousticness": 0.65,
        "max_tempo": 120.0,
    },
    "fiesta": {
        "target_energy": 0.9,
        "target_danceability": 0.85,
        "min_tempo": 110.0,
        "target_valence": 0.8,
    },
    "chill": {
        "target_energy": 0.35,
        "target_acousticness": 0.7,
        "target_instrumentalness": 0.35,
        "max_tempo": 105.0,
    },
    "focus": {
        "target_instrumentalness": 0.65,
        "target_speechiness": 0.08,
        "target_energy": 0.4,
        "max_tempo": 115.0,
    },
}

MOOD_DESCRIPTIONS: dict[str, str] = {
    "romantico": "Vibe romantico y emocional, ideal para citas tranquilas.",
    "fiesta": "Alta energia y baile para ambiente de fiesta.",
    "chill": "Relajado y suave para descanso o fondo musical.",
    "focus": "Concentracion y baja distraccion, util para estudiar o trabajar.",
}


def lambda_handler(event, context):
    method = (event.get("requestContext", {}).get("http", {}).get("method") or "").upper()
    path = event.get("rawPath", "")
    logger.info("⚪️ Spotify request recibida method=%s path=%s", method, path)

    if method == "OPTIONS":
        return build_response(200, {})

    if method != "GET":
        logger.warning("🟡 Metodo no permitido para Spotify: %s", method)
        return build_response(405, {"error": f"Metodo {method} no permitido"})

    try:
        if path.endswith("/spotify/search"):
            return _search(event)

        if path.endswith("/spotify/recommendations"):
            return _recommendations(event)

        if path.endswith("/spotify/moods"):
            return _get_available_moods()

        if path.endswith("/spotify/genres"):
            return _get_available_genres(event)

        if "/spotify/artists/" in path and path.endswith("/top-tracks"):
            return _get_artist_top_tracks(event)

        if path.endswith("/spotify/tracks") or "/spotify/tracks/" in path:
            return _get_track(event)

        if path.endswith("/spotify/artists") or "/spotify/artists/" in path:
            return _get_artist(event)

        logger.warning("🟡 Ruta Spotify no encontrada: %s", path)
        return build_response(404, {"error": "Ruta Spotify no encontrada"})
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


def _recommendations(event):
    query = _get_query(event, "q")
    if not query:
        raise ValueError("Parametro requerido: q")

    limit = _int_param(_get_query(event, "limit"), 20, min_value=1, max_value=100)
    market = (_get_query(event, "market") or DEFAULT_MARKET).strip().upper()
    mood = (_get_query(event, "mood") or "").strip().lower()
    mood_tuners = _get_mood_tuners(mood)
    explicit_tuners = _extract_recommendation_tuners(event)
    tuning_params = {
        **mood_tuners,
        **explicit_tuners,
    }

    logger.info(
        "🔵 Recommendations por busqueda q_len=%s limit=%s market=%s mood=%s tuners=%s",
        len(query),
        limit,
        market,
        mood or "none",
        sorted(tuning_params.keys()),
    )

    search_data = _spotify_get("/search", {
        "q": query,
        "type": "track",
        "limit": 1,
        "offset": 0,
        "market": market,
    })

    tracks = search_data.get("tracks", {}).get("items", [])

    recommendation_seed: dict[str, str] = {}
    seed_payload: dict = {}
    fallback_used = False

    if tracks and tracks[0].get("id"):
        seed_track = tracks[0]
        recommendation_seed["track"] = _map_track(seed_track)
        seed_payload["seed_tracks"] = seed_track.get("id")
    else:
        # Fallback: buscar artista y usar su top track como semilla de recomendaciones.
        fallback_used = True
        logger.warning("🟡 No hubo track semilla; activando fallback por artista para q_len=%s", len(query))

        artist_search = _spotify_get("/search", {
            "q": query,
            "type": "artist",
            "limit": 1,
            "offset": 0,
            "market": market,
        })

        artists = artist_search.get("artists", {}).get("items", [])
        if not artists:
            return build_response(404, {"error": "No se encontro track ni artista semilla para esa busqueda"})

        seed_artist = artists[0]
        seed_artist_id = (seed_artist.get("id") or "").strip()
        if not seed_artist_id:
            return build_response(404, {"error": "No se pudo obtener artist_id semilla"})

        recommendation_seed["artist"] = _map_artist(seed_artist)

        top_tracks_data = _spotify_get(f"/artists/{seed_artist_id}/top-tracks", {"market": market})
        top_tracks = top_tracks_data.get("tracks") or []
        top_seed_track = top_tracks[0] if top_tracks else None
        top_seed_track_id = (top_seed_track or {}).get("id") if top_seed_track else None

        if top_seed_track_id:
            recommendation_seed["track"] = _map_track(top_seed_track)
            seed_payload["seed_tracks"] = top_seed_track_id
        else:
            # Segundo fallback: usar directamente seed_artists para no devolver error.
            logger.warning("🟡 Artista sin top tracks para semilla; usando seed_artists")
            seed_payload["seed_artists"] = seed_artist_id

    recommendation_data = _spotify_get("/recommendations", {
        **seed_payload,
        "limit": limit,
        "market": market,
        **tuning_params,
    })

    recommended_tracks = [_map_track(item) for item in recommendation_data.get("tracks", [])]

    logger.info("🟢 Recommendations Spotify exitosas total=%s", len(recommended_tracks))
    return build_response(200, {
        "query": query,
        "market": market,
        "mood": mood or None,
        "seed": recommendation_seed,
        "preset_tuners": mood_tuners,
        "explicit_tuners": explicit_tuners,
        "tuners": tuning_params,
        "fallback_used": fallback_used,
        "total": len(recommended_tracks),
        "tracks": recommended_tracks,
    })


def _get_mood_tuners(mood: str) -> dict:
    if not mood:
        return {}

    if mood not in MOOD_PRESETS:
        valid = ", ".join(sorted(MOOD_PRESETS.keys()))
        raise ValueError(f"mood invalido: {mood}. Opciones: {valid}")

    return dict(MOOD_PRESETS[mood])


def _extract_recommendation_tuners(event) -> dict:
    tuners: dict = {}

    float_0_1_fields = [
        "target_acousticness",
        "target_danceability",
        "target_energy",
        "target_instrumentalness",
        "target_liveness",
        "target_speechiness",
        "target_valence",
        "min_acousticness",
        "min_danceability",
        "min_energy",
        "min_instrumentalness",
        "min_liveness",
        "min_speechiness",
        "min_valence",
        "max_acousticness",
        "max_danceability",
        "max_energy",
        "max_instrumentalness",
        "max_liveness",
        "max_speechiness",
        "max_valence",
    ]

    int_0_100_fields = [
        "target_popularity",
        "min_popularity",
        "max_popularity",
    ]

    for field in float_0_1_fields:
        raw_value = _get_query(event, field)
        if raw_value is not None and str(raw_value).strip():
            tuners[field] = _float_param(raw_value, min_value=0.0, max_value=1.0)

    for field in int_0_100_fields:
        raw_value = _get_query(event, field)
        if raw_value is not None and str(raw_value).strip():
            tuners[field] = _int_param(raw_value, default_value=0, min_value=0, max_value=100)

    raw_tempo_target = _get_query(event, "target_tempo")
    if raw_tempo_target is not None and str(raw_tempo_target).strip():
        tuners["target_tempo"] = _float_param(raw_tempo_target, min_value=0.0, max_value=300.0)

    raw_tempo_min = _get_query(event, "min_tempo")
    if raw_tempo_min is not None and str(raw_tempo_min).strip():
        tuners["min_tempo"] = _float_param(raw_tempo_min, min_value=0.0, max_value=300.0)

    raw_tempo_max = _get_query(event, "max_tempo")
    if raw_tempo_max is not None and str(raw_tempo_max).strip():
        tuners["max_tempo"] = _float_param(raw_tempo_max, min_value=0.0, max_value=300.0)

    return tuners


def _get_available_genres(event):
    q = (_get_query(event, "q") or "").strip().lower()
    limit = _int_param(_get_query(event, "limit"), 100, min_value=1, max_value=200)

    logger.info("🔵 Get genres Spotify q_len=%s limit=%s", len(q), limit)

    data = _spotify_get("/recommendations/available-genre-seeds")
    genres = data.get("genres") or []

    if q:
        genres = [genre for genre in genres if q in genre.lower()]

    genres = genres[:limit]

    logger.info("🟢 Genres Spotify exitoso total=%s", len(genres))
    return build_response(200, {
        "query": q,
        "total": len(genres),
        "genres": genres,
    })


def _get_available_moods():
    moods = []
    for mood_name in sorted(MOOD_PRESETS.keys()):
        moods.append({
            "id": mood_name,
            "label": mood_name.capitalize(),
            "description": MOOD_DESCRIPTIONS.get(mood_name, ""),
            "tuners": dict(MOOD_PRESETS[mood_name]),
        })

    logger.info("🟢 Moods Spotify disponibles total=%s", len(moods))
    return build_response(200, {
        "total": len(moods),
        "moods": moods,
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
