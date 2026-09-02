# Spotify Web API - Endpoints para APIDates

## Enfoque de autenticacion usado en este proyecto

- **Catalogo publico** (search, tracks, artists, albums, playlists): Client Credentials, sin login del usuario final.
- **Player** (reproduccion en tiempo real del usuario): Authorization Code flow. El usuario vincula su cuenta de Spotify una vez via `/spotify/login` y el backend guarda `access_token`/`refresh_token` por usuario en DynamoDB (`SpotifyUserTokens`), renovando el token automaticamente cuando expira.

## Endpoints implementados en nuestro backend

- GET /spotify/search?q=...&type=track&limit=10&market=CO
- GET /spotify/tracks/{id}?market=CO
- GET /spotify/artists/{id}
- GET /spotify/artists/{id}/top-tracks?market=CO&limit=10
- GET /spotify/artists/{id}/albums?market=CO&limit=20
- GET /spotify/albums/{id}?market=CO
- GET /spotify/albums/{id}/tracks?market=CO&limit=20
- GET /spotify/playlists/{id}?market=CO
- GET /spotify/playlists/{id}/tracks?market=CO&limit=20
- GET /spotify/login (requiere JWT, devuelve `authUrl` para redirigir al usuario a Spotify)
- GET /spotify/callback (publico, Spotify redirige aqui con `code`/`state`)
- GET /spotify/player?market=CO
- GET /spotify/player/currently-playing?market=CO
- GET /spotify/player/devices
- PUT /spotify/player/play
- PUT /spotify/player/pause
- PUT /spotify/player/volume?volume_percent=50
- POST /spotify/player/next
- POST /spotify/player/previous

> `recommendations`, `moods` y `genres` se eliminaron: dependian de la familia `/v1/recommendations`, marcada como deprecated por Spotify.

## Endpoints GET de catalogo (compatibles con Client Credentials)

1. Buscar contenido
- Spotify: GET /v1/search
- Uso: encontrar tracks, artistas, albumes o playlists.
- Parametros clave: q, type, market, limit, offset.
- Referencia: https://developer.spotify.com/documentation/web-api/reference/search

2. Detalle de track
- Spotify: GET /v1/tracks/{id}
- Uso: enriquecer resultados de busqueda.
- Parametros clave: id, market.
- Referencia: https://developer.spotify.com/documentation/web-api/reference/get-track

4. Detalle de artista
- Spotify: GET /v1/artists/{id}
- Uso: mostrar metadatos y generos asociados al artista.
- Parametros clave: id.
- Referencia: https://developer.spotify.com/documentation/web-api/reference/get-an-artist

5. Top tracks de artista
- Spotify: GET /v1/artists/{id}/top-tracks
- Uso: sugerir canciones populares de un artista encontrado por busqueda.
- Parametros clave: id, market.
- Referencia: https://developer.spotify.com/documentation/web-api/reference/get-an-artists-top-tracks

### Endpoint backend agregado para Top Tracks

- Ruta: GET /spotify/artists/{id}/top-tracks
- Query params:
	- market (opcional, default SPOTIFY_MARKET)
	- limit (opcional, default 10, max 50)
- Ejemplo:
	- /spotify/artists/4gzpq5DPGxSnKTe4SA8HAU/top-tracks?market=CO&limit=5

6. Detalle de album
- Spotify: GET /v1/albums/{id}
- Uso: portada, fecha de lanzamiento y total de tracks.
- Parametros clave: id, market.
- Referencia: https://developer.spotify.com/documentation/web-api/reference/get-an-album

7. Tracks de album
- Spotify: GET /v1/albums/{id}/tracks
- Uso: listar canciones de un album.
- Parametros clave: id, market, limit, offset.
- Referencia: https://developer.spotify.com/documentation/web-api/reference/get-an-albums-tracks

8. Albumes de artista
- Spotify: GET /v1/artists/{id}/albums
- Uso: listar discografia de un artista (albums/singles).
- Parametros clave: id, market, limit, offset, include_groups.
- Referencia: https://developer.spotify.com/documentation/web-api/reference/get-an-artists-albums

9. Detalle de playlist
- Spotify: GET /v1/playlists/{id}
- Uso: mostrar nombre, owner, portada y total de tracks.
- Parametros clave: id, market, fields.
- Referencia: https://developer.spotify.com/documentation/web-api/reference/get-playlist

10. Tracks de playlist
- Spotify: GET /v1/playlists/{id}/tracks
- Uso: listar canciones de una playlist publica.
- Parametros clave: id, market, limit, offset, fields.
- Referencia: https://developer.spotify.com/documentation/web-api/reference/get-playlists-tracks

## Player (requiere cuenta de usuario vinculada)

El Player NO funciona con Client Credentials; Spotify exige un token de usuario con scopes `user-read-playback-state`, `user-modify-playback-state` y `user-read-currently-playing`.

### Flujo de vinculacion (Authorization Code)

1. Frontend llama `GET /spotify/login` (autenticado con JWT de la app). El backend responde `{ "authUrl": "https://accounts.spotify.com/authorize?..." }`.
2. Frontend redirige/abre `authUrl` en el navegador. El usuario aprueba los permisos en Spotify.
3. Spotify redirige a `GET /spotify/callback?code=...&state=...` (ruta publica, sin JWT). El backend valida `state` (firmado con HMAC), intercambia `code` por `access_token`/`refresh_token` y los guarda en DynamoDB (`SpotifyUserTokens`) usando el `sub` del usuario como llave.
4. Desde ese momento, los endpoints `/spotify/player/*` funcionan para ese usuario; el `refresh_token` se usa automaticamente cuando el `access_token` expira.

### Endpoints de Player

- GET /spotify/player?market=CO — estado de reproduccion (dispositivo activo, track actual, `is_playing`).
- GET /spotify/player/currently-playing?market=CO — track actual con mas detalle.
- GET /spotify/player/devices — dispositivos disponibles del usuario.
- PUT /spotify/player/play — reanuda o inicia reproduccion. Query opcional `device_id`; body opcional `context_uri` (album/playlist URI) o `uris` (lista separada por comas de track URIs).
- PUT /spotify/player/pause — pausa reproduccion. Query opcional `device_id`.
- PUT /spotify/player/volume?volume_percent=50 — ajusta volumen (0-100). Query opcional `device_id`.
- POST /spotify/player/next — siguiente pista. Query opcional `device_id`.
- POST /spotify/player/previous — pista anterior. Query opcional `device_id`.

Referencias:
- https://developer.spotify.com/documentation/web-api/reference/get-information-about-the-users-current-playback
- https://developer.spotify.com/documentation/web-api/reference/get-the-users-currently-playing-track
- https://developer.spotify.com/documentation/web-api/reference/get-a-users-available-devices
- https://developer.spotify.com/documentation/web-api/reference/start-a-users-playback
- https://developer.spotify.com/documentation/web-api/reference/pause-a-users-playback
- https://developer.spotify.com/documentation/web-api/reference/skip-users-playback-to-next-track
- https://developer.spotify.com/documentation/web-api/reference/skip-users-playback-to-previous-track
- https://developer.spotify.com/documentation/web-api/reference/set-volume-for-users-playback

### Variables de entorno requeridas para Player

- `SPOTIFY_REDIRECT_URI`: debe coincidir con la URL registrada en el dashboard de Spotify Developer (ej. `https://{api}.execute-api.{region}.amazonaws.com/spotify/callback`).
- `SPOTIFY_STATE_SECRET`: secreto usado para firmar (HMAC-SHA256) el parametro `state` y evitar CSRF/forgery en el callback.
- `SPOTIFY_TOKENS_TABLE`: nombre de la tabla DynamoDB donde se guardan los tokens por usuario.

## Recomendacion para la fase 1 del proyecto

1. Mantener `search` como flujo principal de descubrimiento de catalogo.
2. Mantener market por defecto en CO, permitiendo override por query param.
3. Evitar guardar o exponer `client_secret` o tokens de usuario en el frontend; el frontend solo recibe `authUrl` y nunca ve `access_token`/`refresh_token` directamente.
4. Los endpoints de player deben usarse solo despues de confirmar que el usuario vinculo su cuenta (manejar el error 400 "Usuario no ha vinculado su cuenta de Spotify" mostrando el boton de conexion).

## Politicas relevantes de Spotify

- Atribuir contenido a Spotify cuando muestres resultados.
- No usar contenido de Spotify para entrenar modelos de IA.
- No descargar contenido de Spotify.

Referencia general de politicas y terminos:
- https://developer.spotify.com/documentation/web-api
- https://developer.spotify.com/policy
- https://developer.spotify.com/terms
