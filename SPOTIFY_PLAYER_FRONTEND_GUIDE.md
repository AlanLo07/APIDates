# Spotify Web Playback SDK — Guia para el agente de Frontend

Este documento es para el agente/dev que va a implementar el reproductor de Spotify en el front. Explica **que endpoints de nuestro backend usar, en que orden, y como conectarlos con el Web Playback SDK** de Spotify.

Referencia oficial del SDK: https://developer.spotify.com/documentation/web-playback-sdk/howtos/web-app-player

## Por que el SDK y no solo REST

Antes el front controlaba play/pause llamando directo a los endpoints REST (`/spotify/player/play`, `/pause`, etc.) sin que existiera un dispositivo de Spotify Connect activo. Si el usuario no tenia Spotify abierto en otro dispositivo, Spotify respondia `404 NO_ACTIVE_DEVICE` y esto se percibia como "se desconecta la cuenta".

El **Web Playback SDK** corre en el navegador y crea su propio dispositivo Connect. Una vez ese dispositivo se activa (transfer), los controles del propio SDK (`togglePlay`, `nextTrack`, `previousTrack`) funcionan de forma estable porque siempre hay un dispositivo activo.

## Requisito: cuenta Spotify Premium

El Web Playback SDK **solo funciona con cuentas Premium**. Si el usuario no es Premium, el SDK falla al inicializar (evento `initialization_error` / `account_error`). Mostrar un mensaje adecuado en ese caso en vez de reintentar.

## Pre-requisito: cuenta vinculada (Authorization Code flow)

El SDK necesita un `access_token` de usuario con scope `streaming`. Ese vinculo ya existe en el backend:

1. `GET /spotify/login` (requiere JWT de nuestra app) → devuelve `{ "authUrl": "..." }`. Redirigir al usuario a esa URL.
2. Spotify redirige a `GET /spotify/callback` (publico, lo maneja el backend solo). Al terminar, la cuenta queda vinculada y el backend guarda `access_token`/`refresh_token` en DynamoDB.
3. Si el front recibe `401`/`403` o el backend responde que el usuario no ha vinculado cuenta, mostrar un boton "Conectar con Spotify" que dispare el paso 1.

No hace falta que el front maneje ningun token de Spotify manualmente en este flujo (login/callback); eso lo hace el backend.

## Endpoints de backend para el player (todos requieren JWT de nuestra app, excepto `/spotify/callback`)

| Metodo | Ruta | Uso |
|---|---|---|
| GET | `/spotify/login` | Obtener `authUrl` para vincular cuenta |
| GET | `/spotify/player/token` | Obtener `access_token` crudo de Spotify para instanciar el SDK |
| PUT | `/spotify/player/transfer?device_id=...&play=true\|false` | Activar el dispositivo creado por el SDK como el dispositivo Connect activo |
| GET | `/spotify/player?market=CO` | Estado completo de reproduccion (fallback / polling) |
| GET | `/spotify/player/currently-playing?market=CO` | Track actual sonando |
| GET | `/spotify/player/devices` | Listar dispositivos Connect disponibles |
| PUT | `/spotify/player/play?device_id=...` | Reanudar reproduccion (usar solo si no se usa `player.togglePlay()`) |
| PUT | `/spotify/player/pause?device_id=...` | Pausar (idem) |
| PUT | `/spotify/player/volume?volume_percent=50&device_id=...` | Ajustar volumen |
| POST | `/spotify/player/next?device_id=...` | Siguiente pista |
| POST | `/spotify/player/previous?device_id=...` | Pista anterior |

> Los endpoints de play/pause/next/previous quedan como respaldo (por ejemplo, para controlar el player desde una vista sin el SDK cargado). El flujo recomendado principal es usar los metodos del propio objeto `player` del SDK, que no dependen de pasar `device_id` porque ya operan sobre el dispositivo local.

## Flujo de integracion paso a paso

1. **Verificar vinculo de cuenta.** Si el usuario no ha vinculado Spotify, mostrar boton de login (`/spotify/login` → redirect → `/spotify/callback` lo resuelve el backend).

2. **Pedir token para el SDK:**
   ```js
   const res = await fetch('/spotify/player/token', { headers: { Authorization: `Bearer ${appJwt}` } });
   const { access_token } = await res.json();
   ```

3. **Cargar el script del SDK una sola vez:**
   ```js
   const script = document.createElement('script');
   script.src = 'https://sdk.scdn.co/spotify-player.js';
   script.async = true;
   document.body.appendChild(script);
   ```

4. **Instanciar el player dentro de `onSpotifyWebPlaybackSDKReady`.** El `getOAuthToken` debe volver a pedir el token a `/spotify/player/token` cada vez que el SDK lo solicite (asi el backend maneja el refresh automatico, no hay que cachear el token en el front):
   ```js
   window.onSpotifyWebPlaybackSDKReady = () => {
     const player = new window.Spotify.Player({
       name: 'APIDates Web Player',
       getOAuthToken: async (cb) => {
         const res = await fetch('/spotify/player/token', { headers: { Authorization: `Bearer ${appJwt}` } });
         const { access_token } = await res.json();
         cb(access_token);
       },
       volume: 0.5,
     });

     player.addListener('ready', async ({ device_id }) => {
       // Activar este dispositivo UNA sola vez al quedar listo
       await fetch(`/spotify/player/transfer?device_id=${device_id}&play=false`, {
         method: 'PUT',
         headers: { Authorization: `Bearer ${appJwt}` },
       });
     });

     player.addListener('not_ready', ({ device_id }) => {
       console.warn('Device offline', device_id);
     });

     player.addListener('initialization_error', ({ message }) => console.error(message));
     player.addListener('authentication_error', ({ message }) => console.error(message));
     player.addListener('account_error', ({ message }) => console.error(message)); // no-Premium

     player.connect();
   };
   ```

5. **Escuchar cambios de estado para pintar la UI** (track actual, si esta pausado, progreso):
   ```js
   player.addListener('player_state_changed', (state) => {
     if (!state) return;
     setPaused(state.paused);
     setTrack(state.track_window.current_track);
   });
   ```

6. **Controles de reproduccion: usar los metodos del SDK, no los endpoints REST directamente**, para evitar el problema original de "no active device":
   ```js
   player.togglePlay();
   player.nextTrack();
   player.previousTrack();
   player.setVolume(0.5);
   ```

7. **Reproducir un track/album/playlist especifico** (buscar contenido primero via `/spotify/search`, `/spotify/albums/{id}`, etc., y luego iniciar reproduccion en el device del SDK):
   ```js
   await fetch('/spotify/player/play', {
     method: 'PUT',
     headers: { Authorization: `Bearer ${appJwt}`, 'Content-Type': 'application/json' },
     body: JSON.stringify({}), // el backend acepta context_uri/uris via query params: ?context_uri=... o ?uris=uri1,uri2
   });
   ```
   Nota: `/spotify/player/play` soporta query params `context_uri`, `uris` (coma-separado) y `device_id`. Si no se especifica `device_id`, Spotify usa el dispositivo activo (el del SDK, luego del `transfer`).

## Orden critico a respetar

1. Login/vinculo de cuenta (una sola vez por usuario).
2. Cargar SDK + crear `player` + `player.connect()`.
3. Esperar evento `ready` → llamar `/spotify/player/transfer` con ese `device_id` **antes** de cualquier play/pause.
4. Recien despues, controlar con los metodos del SDK (`togglePlay`, `nextTrack`, `previousTrack`) o con `/spotify/player/play` pasando `context_uri`/`uris` para iniciar contenido especifico.

## Errores comunes a manejar en el front

- `account_error` del SDK → usuario no tiene Spotify Premium, mostrar mensaje claro (no reintentar).
- `401`/`403` en cualquier endpoint `/spotify/player/*` → la cuenta no esta vinculada o el refresh_token se invalido; redirigir a `/spotify/login`.
- `404`/error al pausar/reproducir sin haber hecho `transfer` → asegurarse de esperar el evento `ready` antes de habilitar los controles en la UI.
