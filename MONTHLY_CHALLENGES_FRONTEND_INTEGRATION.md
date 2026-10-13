# Retos mensuales compartidos: integración frontend

## Base y autenticación

La API base es el output `ApiUrl` del stack SAM. Todas las rutas están bajo `/monthly-challenges` y aceptan JSON. Enviar el JWT de Cognito en `Authorization: Bearer <token>` y `Content-Type: application/json`.

El backend obtiene `sub`, `email` y `name` de los claims; no enviar `userId`, `coupleId`, `createdBy` ni `proposedBy` para atribuir una acción. Esos datos se resuelven en servidor. La pertenencia a pareja de este módulo es independiente de FinancesCRUD, por lo que debe inicializarse aquí también. Las operaciones de pareja requieren membresía; catálogo no requiere membresía, aunque sí la autenticación del authorizer de la API.

Las respuestas usan el envelope habitual de API Gateway: JSON en el body y HTTP status code. Los errores tienen forma `{ "error": "..." }`.

## Estados y reglas visibles

- `PENDING`: aún no hay reto elegido. Los integrantes proponen y eligen explícitamente.
- `ASSIGNED`: hay un único reto elegido y activo para el mes.
- `COMPLETED`: finalizado por uno de los dos; guarda quién/cuándo y el cupón asociado.

`monthKey` es `YYYY-MM` en UTC. El plazo predeterminado es el día 5 del mes a las 23:59:59 UTC. No es un cron: el sistema evalúa el plazo perezosamente al consultar o ejecutar una operación. Si vence sin acuerdo, intenta asignar un reto aleatorio; si el catálogo no tiene retos activos, permanece `PENDING`.

El silencio no cuenta como aceptación. Ambos deben elegir la misma propuesta, o ambos solicitar el desempate aleatorio. Un desacuerdo mantiene el mes `PENDING`; cualquiera puede cambiar su voto. Si uno propone y el otro no ha participado, el estado sigue `PENDING` y el segundo integrante puede elegir esa propuesta o proponer otra.

La categoría `ahorro` solo etiqueta una actividad/compromiso. Este módulo no crea movimientos financieros.

## Rutas

| Método y ruta | Uso | Body / query | Respuesta |
|---|---|---|---|
| `POST /monthly-challenges/init` | Inicializar pareja y membresías propias del módulo | `user1Email`, `user2Email`, `user1Name`, `user2Name` | `201` y `coupleId`; `409` si emails/pareja ya registrados |
| `GET /monthly-challenges/current` | Consultar mes UTC actual; evalúa el plazo | Ninguno | Objeto estado mensual |
| `GET /monthly-challenges/{monthKey}` | Consultar un mes existente, formato `YYYY-MM` | Ninguno | Objeto estado; `404` si no hay estado |
| `POST /monthly-challenges/current/proposals` | Crear o reemplazar la propuesta del usuario en el ciclo actual | Snapshot custom o id de catálogo | Estado mensual actualizado |
| `DELETE /monthly-challenges/current/proposals` | Retirar la propuesta propia | Ninguno | Estado mensual actualizado |
| `POST /monthly-challenges/current/choice` | Elegir una propuesta propia/de la pareja o pedir desempate aleatorio | `proposedBy` o `requestRandomTiebreak: true` | Estado actualizado; puede pasar a `ASSIGNED` |
| `PUT /monthly-challenges/current/active` | Editar título, descripción o categoría del reto asignado | Uno o más campos editables | Estado actualizado |
| `POST /monthly-challenges/current/cancel` | Cancelar el reto asignado y abrir otro ciclo de elección | Opcional: `reason` | Estado `PENDING`, ciclo incrementado |
| `POST /monthly-challenges/current/replace` | Cancelar el activo y registrar una propuesta de reemplazo | Snapshot custom/catalog; opcional `reason` | Estado `PENDING` con la nueva propuesta |
| `POST /monthly-challenges/current/complete` | Completar reto y crear cupón compartido | Ninguno | `201` si creó el cupón; `200` si ya estaba completado |
| `GET /monthly-challenges/current/coupon` | Obtener cupón ligado al reto actual | Ninguno | Cupón o `404` si aún no existe |
| `GET /monthly-challenges/catalog` | Listar catálogo, opcionalmente filtrar | `?category=...`; `?includeInactive=true` | `{ items, count }` |
| `GET /monthly-challenges/catalog/random` | Sugerencia aleatoria, no asigna el reto mensual | Opcional `?category=...` | Reto del catálogo; `404` sin opciones |
| `GET /monthly-challenges/catalog/{challengeId}` | Consultar reto del catálogo | Ninguno | Reto; `404` si no existe |
| `POST /monthly-challenges/catalog` | Añadir reto al catálogo | `title`, `description`, `category` | Reto creado (`201`) |
| `PUT /monthly-challenges/catalog/{challengeId}` | Corregir contenido/categoría o activar/desactivar | Campos parciales: `title`, `description`, `category`, `active` | Reto actualizado |
| `DELETE /monthly-challenges/catalog/{challengeId}` | Desactivar (no borrar) reto del catálogo | Ninguno | Confirmación |
| `GET /monthly-challenges/favorites` | Listar favoritos compartidos de la pareja | Ninguno | `{ items, count }` |
| `POST /monthly-challenges/favorites` | Marcar favorito | Snapshot custom/catalog | Favorito creado (`201`); `409` si ya existe |
| `DELETE /monthly-challenges/favorites/{favoriteId}` | Desmarcar favorito | Ninguno | Confirmación |

Categorías válidas: `diversion`, `actividades_juntos`, `comunicacion`, `ahorro`, `otro`.

### Propuestas

Para proponer un reto personalizado:

```json
{
  "source": "custom",
  "title": "Preparar una cena juntos",
  "description": "Elegir una receta nueva y cocinarla entre los dos.",
  "category": "actividades_juntos"
}
```

Para proponer uno del catálogo:

```json
{
  "source": "catalog",
  "catalogChallengeId": "<id>"
}
```

Se admite una propuesta por usuario y ciclo. Repetir `POST` cambia la propuesta propia y conserva el autor original de la acción. Los títulos admiten hasta 120 caracteres y las descripciones hasta 600. Un reto de catálogo se copia como snapshot en la propuesta; los cambios posteriores en el catálogo no cambian ese snapshot.

### Elección y resolución

Elegir una propuesta identificada por el autor (el valor es el `proposedBy` devuelto en `proposals`):

```json
{ "proposedBy": "<sub-del-proponente>" }
```

Si ambos eligen el mismo `proposedBy`, la propuesta queda asignada. Para pedir aleatorio como desempate, ambos envían:

```json
{ "requestRandomTiebreak": true }
```

El backend actualiza la elección del usuario que realiza la solicitud. Si cambia de parecer antes de resolverse, vuelve a llamar con otra `proposedBy` o con `requestRandomTiebreak: true`. Si retira su propuesta, las elecciones que la señalaban se eliminan.

### Estado mensual

Ejemplo abreviado de `GET /monthly-challenges/current` o de una mutación de propuesta/elección:

```json
{
  "monthKey": "2026-10",
  "cycle": 1,
  "status": "PENDING",
  "deadlineAt": "2026-10-05T23:59:59+00:00",
  "selectedChallenge": null,
  "selectedSource": null,
  "assignedAt": null,
  "completedBy": null,
  "completedByEmail": null,
  "completedAt": null,
  "couponId": null,
  "proposals": [
    {
      "proposedBy": "<sub>",
      "proposedByEmail": "persona@example.com",
      "proposedByName": "Persona",
      "source": "custom",
      "catalogChallengeId": null,
      "title": "Preparar una cena juntos",
      "description": "Elegir una receta nueva.",
      "category": "actividades_juntos",
      "createdAt": "<ISO-8601>",
      "updatedAt": "<ISO-8601>"
    }
  ],
  "choices": [],
  "disagreement": false,
  "availableActions": ["propose", "chooseProposal"],
  "viewer": { "sub": "<sub>", "email": "persona@example.com", "name": "Persona" },
  "partner": { "email": "pareja@example.com", "name": "Pareja" },
  "generatedAt": "<ISO-8601>"
}
```

En `PENDING`, `proposals` y `choices` contienen los datos visibles de ese ciclo. `availableActions` está calculado para la persona autenticada: `propose`, `updateProposal`, `withdrawProposal`, `chooseProposal` y, al existir desacuerdo, `requestRandomTiebreak`. En `ASSIGNED`: `completeChallenge`, `editChallenge`, `cancelChallenge`, `replaceChallenge`. En `COMPLETED`: `viewCoupon`.

`selectedChallenge` incluye `source`, `catalogChallengeId`, `title`, `description` y `category`. `selectedSource` indica `agreement`, `agreement_random` o `auto_random`.

### Finalización y cupón

La finalización registra el `sub`/email/nombre del usuario autenticado. La respuesta incluye:

```json
{
  "month": { "monthKey": "2026-10", "status": "COMPLETED", "couponId": "<id>" },
  "coupon": {
    "couponId": "<id>",
    "title": "Recompensa: <título del reto>",
    "description": "<descripción>",
    "sourceMonthKey": "2026-10",
    "sourceCycle": 1,
    "sourceChallengeTitle": "<título>",
    "createdBy": "<sub del usuario que completó>",
    "createdByEmail": "<email>",
    "createdByName": "<nombre>",
    "createdAt": "<ISO-8601>",
    "redeemed": false
  },
  "couponCreated": true,
  "alreadyCompleted": false
}
```

Un reintento posterior devuelve `200`, el mismo cupón y `couponCreated: false`; la finalización y el cupón se escriben atómicamente. El cupón pertenece a la partición de la pareja y no consume cupones de la cuponera existente.

## Errores que debe manejar el cliente

- `400`: validación, JSON/categoría/campos inválidos.
- `401`: no hay JWT con `sub` y `email`.
- `403`: usuario autenticado sin membresía de pareja en este módulo.
- `404`: mes/elemento/propuesta/cupón inexistente o catálogo sin resultados aleatorios.
- `409`: estado no permite la operación, propuesta ya resuelta, favorito duplicado o conflicto de concurrencia. Volver a consultar `GET /current` antes de permitir reintento.
- `502`: error de DynamoDB; `500`: error inesperado.

## Límites actuales a tener en cuenta

- La ruta `GET /monthly-challenges/{monthKey}` devuelve el estado del mes, pero no expone las entradas `MESHISTORIAL` de ediciones/cancelaciones; no hay endpoint de galería/auditoría todavía.
- Después de asignar/completar, el estado mensual devuelve `proposals: []` y `choices: []`; la API no conserva esos datos en la respuesta pública del mes.
- `replace` cancela el reto anterior y registra la nueva propuesta del usuario que llamó; el otro integrante todavía debe elegirla. No es una asignación inmediata.
- El favorito conserva un snapshot, pero no hay endpoint `POST proposal-from-favorite`. Para re-proponerlo como contenido personalizado, el frontend puede enviar su snapshot con `source: "custom"`; para usar un elemento de catálogo, enviar su `catalogChallengeId`.
- Las rutas de escritura del catálogo no comprueban rol administrativo en el handler. Aunque requieren el authorizer JWT de API Gateway, cualquier usuario autenticado puede invocarlas. No exponer estas operaciones desde la UI general hasta restringir su autorización en backend.
- El contrato no incluye catálogo precargado; la lista puede estar vacía hasta que se carguen retos.
