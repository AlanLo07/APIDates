# Spotify Web API - Endpoints GET para APIDates

## Enfoque de autenticacion usado en este proyecto

Este backend usa Client Credentials (sin login en la app cliente).

Eso permite consultar catalogo publico de Spotify desde backend, pero no datos privados de usuario.

## Endpoints implementados en nuestro backend

- GET /spotify/search?q=...&type=track&limit=10&market=CO
- GET /spotify/recommendations?q=...&limit=20&market=CO&mood=romantico
- GET /spotify/moods
- GET /spotify/genres?q=pop&limit=50
- GET /spotify/tracks/{id}?market=CO
- GET /spotify/artists/{id}
- GET /spotify/artists/{id}/top-tracks?market=CO&limit=10

### Logica de fallback en recommendations

Endpoint: GET /spotify/recommendations?q=...&limit=20&market=CO

1. Intenta encontrar track semilla por busqueda de texto.
2. Si no encuentra track, busca artista por texto.
3. Si encuentra artista, intenta usar su top track como semilla.
4. Si no hay top tracks, usa seed_artists directamente para no fallar.

La respuesta ahora incluye `fallback_used` para indicar si se aplico fallback.

### Parametros opcionales (tuners) en recommendations

Puedes enviar tuners para controlar el mood de los resultados:

- Popularidad (0-100):
	- `min_popularity`, `target_popularity`, `max_popularity`
- Tempo/BPM (0-300):
	- `min_tempo`, `target_tempo`, `max_tempo`
- Audio features (0.0-1.0):
	- `min_`, `target_`, `max_` para:
		- `acousticness`
		- `danceability`
		- `energy`
		- `instrumentalness`
		- `liveness`
		- `speechiness`
		- `valence`

Ejemplos:
- /spotify/recommendations?q=morat&limit=20&target_energy=0.75&target_valence=0.8
- /spotify/recommendations?q=rock%20latino&min_popularity=40&target_danceability=0.6
- /spotify/recommendations?q=fiesta&min_tempo=110&max_tempo=150&target_energy=0.9

### Presets de mood en recommendations

Puedes simplificar la llamada usando `mood`:

- `romantico`
- `fiesta`
- `chill`
- `focus`

Ejemplos:
- /spotify/recommendations?q=camila&limit=20&mood=romantico
- /spotify/recommendations?q=party&limit=20&mood=fiesta
- /spotify/recommendations?q=lofi&limit=20&mood=chill
- /spotify/recommendations?q=instrumental&limit=20&mood=focus

Regla de prioridad:
- Si envias `mood` y tuners explicitos en la misma llamada, los tuners explicitos sobrescriben el preset.

Respuesta enriquecida:
- `mood`: preset recibido
- `preset_tuners`: valores aportados por el preset
- `explicit_tuners`: tuners enviados en query
- `tuners`: resultado final aplicado en Spotify

### Endpoint backend para catalogo de moods

- Ruta: GET /spotify/moods
- Uso: poblar selects/toggles de mood en frontend sin hardcode.
- Respuesta: lista de moods con `id`, `label`, `description` y `tuners` sugeridos.

## Endpoints GET de Spotify recomendados (compatibles con Client Credentials)

1. Buscar contenido
- Spotify: GET /v1/search
- Uso: encontrar track semilla para recomendaciones, buscar artistas o albumes.
- Parametros clave: q, type, market, limit, offset.
- Referencia: https://developer.spotify.com/documentation/web-api/reference/search

2. Recomendaciones por semillas
- Spotify: GET /v1/recommendations
- Uso: recomendar tracks similares usando seed_tracks, seed_artists o seed_genres.
- Parametros clave: seed_tracks o seed_artists o seed_genres, market, limit.
- Nota: documentacion marca OAuth 2.0 como deprecated en esta pagina, pero el endpoint sigue disponible para catalogo segun el flujo vigente del app.
- Referencia: https://developer.spotify.com/documentation/web-api/reference/get-recommendations

3. Detalle de track
- Spotify: GET /v1/tracks/{id}
- Uso: enriquecer resultados de busqueda/recomendaciones.
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

6. Album de track
- Spotify: GET /v1/albums/{id}
- Uso: ampliar datos visuales del resultado (portadas, fecha, total de tracks).
- Parametros clave: id, market.
- Referencia: https://developer.spotify.com/documentation/web-api/reference/get-an-album

7. Generos disponibles para recomendaciones
- Spotify: GET /v1/recommendations/available-genre-seeds
- Uso: poblar filtros de genero en frontend para recomendaciones mas precisas.
- Referencia: https://developer.spotify.com/documentation/web-api/reference/get-recommendation-genres

### Endpoint backend agregado para generos

- Ruta: GET /spotify/genres
- Query params:
	- q (opcional, filtra por texto parcial, por ejemplo pop)
	- limit (opcional, default 100, max 200)
- Ejemplo:
	- /spotify/genres?q=pop&limit=20

## Endpoints GET que NO aplican sin login de usuario

Estos requieren token de usuario (Authorization Code), no solo Client Credentials:

- GET /v1/me
- GET /v1/me/top/tracks
- GET /v1/me/player/recently-played
- GET /v1/me/playlists
- GET /v1/me/tracks

## Recomendacion para la fase 1 del proyecto

1. Mantener search + recommendations como flujo principal.
2. Ya implementado: fallback a artista/top-tracks cuando no hay track semilla.
3. Mantener market por defecto en CO, permitiendo override por query param.
4. Evitar guardar o exponer tokens en frontend.

## Politicas relevantes de Spotify

- Atribuir contenido a Spotify cuando muestres resultados.
- No usar contenido de Spotify para entrenar modelos de IA.
- No descargar contenido de Spotify.

Referencia general de politicas y terminos:
- https://developer.spotify.com/documentation/web-api
- https://developer.spotify.com/policy
- https://developer.spotify.com/terms
