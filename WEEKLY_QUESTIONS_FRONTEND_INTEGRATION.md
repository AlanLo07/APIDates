# Pregunta de la semana: API e integracion frontend

## Autenticacion y alcance

Base: `${ApiUrl}/weekly-questions`.

Todas las rutas requieren `Authorization: Bearer <JWT de Cognito>`, igual que
los otros CRUD protegidos. Usar `Content-Type: application/json` al enviar JSON.
El directorio es el mismo User Pool de Cognito del proyecto, no usuarios IAM.
Debe contener exactamente dos usuarios con `sub` diferentes. Ambos pueden
crear y editar cualquier pregunta del catalogo, y solo su propia respuesta.
No se envia `userId`, `coupleId` ni el autor desde el frontend.

No hay endpoints DELETE, desactivacion, omision ni reinicio de respuestas.
El catalogo comienza vacio: los integrantes lo llenan mediante POST.

## Reglas semanales

- Semana: lunes 00:00 inclusive a lunes siguiente 00:00 exclusivo, zona
  `America/Mexico_City`. No es configurable; no usar la zona del dispositivo.
- `GET /current` selecciona y persiste la pregunta al primer acceso de la semana.
  Los listados y las consultas por ID no disparan una seleccion.
- Ambos integrantes ven la misma pregunta, aunque consulten simultaneamente.
- Se sortea uniformemente entre preguntas nunca presentadas y preguntas con
  cero o una respuesta. Se permite repetir de una semana a la siguiente.
- Una pregunta con dos respuestas no vuelve al sorteo. Si se completa durante
  su semana, sigue siendo la actual hasta el siguiente lunes.
- Las semanas sin consultar `/current` no generan asignaciones retroactivas.
- Una respuesta nueva solo se puede crear para la pregunta actual.
- Una respuesta existente se puede editar en cualquier momento, incluso
  fuera de su semana y despues de que ambos hayan respondido.
- Las respuestas son visibles para ambos inmediatamente al consultar la API.
  No hay push, WebSocket ni notificaciones nuevas en esta funcionalidad.
- Si una pregunta parcial se repite, conserva la respuesta existente.
- Si no quedan candidatas, `/current` devuelve 200 con `question: null` y
  `message: "No hay preguntas disponibles"`. No se congela la semana vacia:
  si agregan una pregunta, la siguiente consulta puede seleccionarla.

## Endpoints

Todas las rutas de esta tabla son relativas a `/weekly-questions`.

| Metodo | Ruta | Resultado |
|---|---|---|
| POST | `/catalog` | Crear pregunta; 201 |
| GET | `/catalog` | Listar contenido vigente del catalogo; 200 |
| GET | `/catalog/{id}` | Consultar contenido y version vigente; 200 |
| PUT | `/catalog/{id}` | Editar texto y categoria; 200 |
| GET | `/current` | Consultar/asignar pregunta semanal; 200 |
| GET | `/` (sin barra final en la URL real) | Listar preguntas y progreso; 200 |
| GET | `/{id}` | Consultar una pregunta y las respuestas; 200 |
| PUT | `/{id}/answer` | Crear respuesta propia (201) o editarla (200) |

Usar `/weekly-questions`, no `/weekly-questions/`, para el listado de progreso.
Los IDs son UUID canonicos en minusculas, devueltos al crear las preguntas.

## 1. Crear y editar el catalogo

### POST `/weekly-questions/catalog`

```json
{
  "text": "Como nos imaginamos dentro de cinco anos?",
  "category": "profundas"
}
```

Respuesta 201:

```json
{
  "id": "00000000-0000-4000-8000-000000000001",
  "text": "Como nos imaginamos dentro de cinco anos?",
  "category": "profundas",
  "version": 1,
  "createdAt": "2026-11-19T18:00:00+00:00",
  "updatedAt": "2026-11-19T18:00:00+00:00",
  "createdBy": "sub-integrante-1",
  "updatedBy": "sub-integrante-1"
}
```

Validaciones:

- `text`: string de 1 a 1000 caracteres despues de quitar espacios exteriores.
- `category`: string libre de 1 a 60 caracteres, con el mismo recorte.
  Sugerencias: `divertidas`, `profundas`, `romanticas`, `absurdas`, `otras`.
  No hay una enumeracion cerrada; el filtro compara exactamente, incluyendo
  mayusculas y acentos.
- El texto admite acentos y Unicode; los ejemplos usan ASCII por simplicidad.
- Los campos adicionales se rechazan con 400.

### PUT `/weekly-questions/catalog/{id}`

```json
{
  "text": "Que aventura queremos vivir dentro de cinco anos?",
  "category": "romanticas",
  "version": 1
}
```

Enviar ambos campos y la `version` vigente del catalogo (entero positivo).
La respuesta tiene el mismo formato de creacion con `version` incrementada.
Ante 409, volver a consultar el catalogo y reconciliar la edicion; no
sobrescribir automaticamente los cambios de la otra persona.

**Historial congelado:** al presentarse por primera vez, se guarda una copia
del ID, texto, categoria y version. Las consultas de progreso y las futuras
repeticiones conservan esa copia, aunque despues se edite el catalogo.
Las preguntas nunca presentadas si utilizan el contenido actualizado.

Por eso `catalogVersion` en progreso puede ser menor que `version` en el
catalogo. Para editar, obtener la version de `/catalog/{id}`, no del historial.

## 2. Consultar la pregunta actual

### GET `/weekly-questions/current`

```json
{
  "weekKey": "2026-11-16",
  "weekStart": "2026-11-16T00:00:00-06:00",
  "weekEnd": "2026-11-23T00:00:00-06:00",
  "timeZone": "America/Mexico_City",
  "users": [
    {"sub": "sub-integrante-1", "name": "Integrante 1"},
    {"sub": "sub-integrante-2", "name": "Integrante 2"}
  ],
  "question": {
    "id": "00000000-0000-4000-8000-000000000001",
    "text": "Como nos imaginamos dentro de cinco anos?",
    "category": "profundas",
    "catalogVersion": 1,
    "presented": true,
    "locked": false,
    "isCurrent": true,
    "status": "partial",
    "canAnswer": false,
    "canEdit": true,
    "answers": {
      "sub-integrante-1": {
        "text": "Viviendo juntos y viajando mas.",
        "createdAt": "2026-11-19T18:05:00+00:00",
        "updatedAt": "2026-11-19T18:05:00+00:00"
      }
    },
    "firstPresentedAt": "2026-11-19T18:00:00+00:00",
    "lastPresentedWeek": "2026-11-16"
  },
  "message": null
}
```

La respuesta del ejemplo se consulta como `sub-integrante-1`. Para el otro
integrante, la misma pregunta tendria `canAnswer: true` y `canEdit: false`.
`name` puede ser un string vacio si no esta definido en Cognito.

Si no hay preguntas disponibles, se conservan los campos de semana y usuarios,
pero `question` es `null` y `message` indica que no hay preguntas disponibles.
No es un error ni se debe inventar una pregunta desde el frontend.

## 3. Crear o editar la respuesta propia

### PUT `/weekly-questions/{id}/answer`

```json
{
  "text": "Viviendo juntos y viajando mas."
}
```

- Texto obligatorio, entre 1 y 5000 caracteres despues del recorte.
- El unico campo permitido es `text`. No se permite responder por otra persona.
- 201 si se crea y 200 si se edita.
- Devuelve directamente el objeto de progreso de la pregunta (el contenido de
  `question` del ejemplo anterior, sin el envoltorio semanal).
- Una edicion conserva `createdAt` y actualiza `updatedAt`.
- Una pregunta no presentada o una respuesta nueva fuera de su semana dan 403.
- Guardar no borra ni reemplaza la respuesta del otro integrante.
- Las respuestas no se pueden eliminar ni vaciar.

## 4. Listados, historial y filtros

`GET /weekly-questions/catalog` devuelve los registros actuales del catalogo.
Acepta `category`, `limit` y `nextToken`.

`GET /weekly-questions` devuelve objetos de progreso, incluidos los nunca
presentados. `GET /weekly-questions/{id}` devuelve uno de esos objetos.

| Campo | Significado |
|---|---|
| `status: unseen` | Nunca presentada; sin respuestas |
| `status: pending` | Presentada, cero respuestas |
| `status: partial` | Presentada, una respuesta |
| `status: completed` | Ambos respondieron |
| `presented` | Ya pertenece al historial |
| `locked` | Todavia no se ha presentado (`!presented`) |
| `isCurrent` | Es la seleccionada esta semana |
| `canAnswer` | Quien consulta puede crear su respuesta ahora |
| `canEdit` | Quien consulta ya tiene una respuesta y puede editarla |
| `answers` | Mapa por `sub` de Cognito; vacio si nadie respondio |

**No habilitar el formulario basandose solo en `locked`.** Una pregunta antigua
puede tener `locked: false`, `canAnswer: false` y `canEdit: true`.
El backend verifica los permisos de nuevo al guardar.

Filtros del listado de progreso:

| Parametro | Valores |
|---|---|
| `status` | `unseen`, `pending`, `partial`, `completed` |
| `pendingFor` | `me`, `partner`; solo preguntas ya presentadas |
| `category` | Categoria exacta del texto presentado (o del catalogo si es nueva) |
| `limit` | Entero de 1 a 100; predeterminado 20 |
| `nextToken` | Valor devuelto por la pagina anterior |

Los filtros se combinan con AND.

```text
GET /weekly-questions?status=completed
GET /weekly-questions?pendingFor=me
GET /weekly-questions?pendingFor=partner
GET /weekly-questions?status=pending
GET /weekly-questions?status=partial
GET /weekly-questions?status=unseen
GET /weekly-questions/catalog?category=romanticas&limit=20
```

Ambos listados responden:

```json
{
  "items": [],
  "nextToken": null
}
```

Una lista vacia significa que no hay coincidencias. Si `nextToken` no es null,
consultar otra pagina conservando los filtros. El orden es por ID ascendente,
no cronologico; las paginas no son una fotografia inmutable si se agregan
preguntas durante la navegacion.

## Flujo recomendado del frontend

1. Consultar `/current` al entrar y usar `users` para identificar a los integrantes.
2. Si `question` es null, mostrar el estado vacio y la opcion de agregar preguntas.
3. Si `canAnswer` es true, mostrar el formulario de respuesta nueva.
4. Si `canEdit` es true, mostrar la respuesta propia y permitir modificarla.
5. Leer la respuesta ajena de `answers[partnerSub]`; si falta, indicar pendiente.
6. Despues de guardar, actualizar la tarjeta con la respuesta de la API.
7. Refrescar al recuperar foco o mediante una accion de actualizar para ver
   respuestas de la pareja. No se implementa actualizacion en tiempo real.
8. Al alcanzar `weekEnd`, volver a consultar `/current`; no sortear ni prolongar
   la semana desde el cliente.
9. Usar el listado con `status=completed` para el historial completado y
   `pendingFor=me` para pendientes propias.

Las respuestas exitosas incluyen `Cache-Control: no-store`; no persistir ni
compartir respuestas privadas en caches publicas.

## Errores

El handler devuelve `{"error": "descripcion"}`. API Gateway puede responder con
su propio formato cuando rechaza una ruta o JWT antes de ejecutar la Lambda.

| HTTP | Causa |
|---|---|
| 400 | JSON, campos, longitudes, ID, version o filtros invalidos |
| 401 | Falta autenticacion |
| 403 | Usuario ajeno a la pareja o respuesta nueva no permitida |
| 404 | Pregunta o ruta inexistente |
| 405 | Metodo no permitido si llega al handler; no hay rutas DELETE en Gateway |
| 409 | Edicion de catalogo desactualizada o concurrencia persistente |
| 500 | Error interno inesperado |
| 503 | Configuracion de usuarios invalida o servicio AWS no disponible |

Ante un 409 de seleccion/respuestas, refrescar y permitir reintentar sin
perder el texto escrito. No reintentar automaticamente un conflicto de
catalogo con una version nueva sin revisar los cambios ajenos.

## Persistencia y despliegue

Recursos definidos en [la plantilla SAM](lambdas/template.yaml):

- `WeeklyQuestionsFunction`, runtime heredado Python 3.13 y authorizer JWT existente.
- `WeeklyQuestionsTable`, PK/SK, pago por uso, cifrado, PITR y retencion.
- Permisos limitados a leer/escribir esa tabla y listar el User Pool.
- Variables: `WEEKLY_QUESTIONS_TABLE_NAME` y `COGNITO_USER_POOL_ID`.
- Outputs: `WeeklyQuestionsEndpoint` y `WeeklyQuestionsTableName`.
- Dependencia `tzdata` para resolver la zona IANA tambien en sistemas sin base
  de zonas instalada (por ejemplo Windows).

Modelo de una tabla:

| PK | SK | Contenido |
|---|---|---|
| `CATALOG` | `QUESTION#{id}` | Pregunta editable y version |
| `COUPLE#{hash de los dos sub}` | `WEEK#{fecha del lunes}` | Seleccion semanal inmutable |
| `COUPLE#{hash de los dos sub}` | `HISTORY#{id}` | Copia presentada, respuestas y version interna |

La asignacion y el historial se escriben en una transaccion condicionada para
evitar selecciones distintas y registros parciales. Las respuestas usan
versiones internas y reintentos acotados para conservar escrituras concurrentes.
Los listados consultan las paginas DynamoDB del catalogo/historial y luego
filtran/paginan en memoria: esta implementacion esta pensada para una sola
pareja, no para un catalogo masivo.

No se modifica el directorio de usuarios. Reemplazar usuarios no forma parte
del flujo: cambiar sus `sub` produciria otra identidad de pareja.

Para validar localmente, usar el entorno virtual del proyecto con `boto3` y
los requisitos de la nueva funcion instalados:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -p test_weekly_questions_handler.py
```

Desde la carpeta `lambdas`, antes de desplegar:

```powershell
sam validate --lint
sam build
```

Revisar el changeset antes de desplegar al entorno deseado. Esta implementacion
no ejecuta el despliegue ni modifica datos de AWS.
