# API de Checklists

Esta guia describe el contrato entre Flutter y el backend para checklists de supermercado, viaje, lista de deseos/compras y listas personalizadas.

## Checklists vinculados a citas

La cita de tipo `evento` guarda exclusivamente `checklistId` (string o `null`), no una copia de los items. Los checklists se almacenan en `ChecklistsTable` y se gestionan mediante los endpoints existentes de `/checklists`.

Los endpoints siguen protegidos por Cognito. Enviar `Authorization: Bearer <token>` y, cuando haya body, `Content-Type: application/json`.

### Servicios modificados

| Metodo | Ruta | Cambio |
|---|---|---|
| POST | `/citas` | Permite omitir checklist, vincular uno existente o crear uno nuevo |
| PUT | `/citas/{id}` | Permite conservar, cambiar, crear o desvincular el checklist |
| GET | `/citas/{id}` | Devuelve `checklistId` para los eventos nuevos, sin expandir el checklist |
| GET | `/citas?type=evento` | Cada evento nuevo contiene `checklistId`; mantiene su respuesta como array |
| POST | `/checklists` | Acepta `items` iniciales, ademas de `grupos`; creacion atomica |

No se agregan rutas. GET de citas conserva las rutas y la estructura de respuesta existentes; cambia el modelo persistido por POST/PUT.

### Crear cita y vincular un checklist existente

`POST /citas`

```json
{
  "type": "evento",
  "title": "Viaje de fin de semana",
  "description": "Preparar equipaje",
  "date": "31-10-2026",
  "checklistId": "default-viaje"
}
```

Respuesta `201`:

```json
{
  "message": "Cita creada",
  "id": "uuid-cita",
  "type": "evento",
  "checklistId": "default-viaje"
}
```

La existencia del checklist se comprueba dentro de la misma transaccion que guarda la cita. Un ID inexistente devuelve `404` y no crea la cita. No se clona ni se reinicia el checklist seleccionado.

### Crear cita y un checklist nuevo

`POST /citas`

```json
{
  "type": "evento",
  "title": "Picnic",
  "description": "Salida al parque",
  "date": "31-10-2026",
  "checklist": {
    "titulo": "Preparativos del picnic",
    "kind": "personalizado",
    "usaGrupos": true,
    "grupos": [
      {"id": "comida", "nombre": "Comida", "orden": 0}
    ],
    "items": [
      {
        "nombre": "Sandwiches",
        "groupId": "comida",
        "comprado": false,
        "prioridad": "alta",
        "prioridadOrden": 1,
        "nota": "Para dos personas"
      },
      {"nombre": "Manta", "comprado": false}
    ]
  }
}
```

Respuesta `201`:

```json
{
  "message": "Cita creada",
  "id": "uuid-cita",
  "type": "evento",
  "checklistId": "uuid-checklist-nuevo"
}
```

El objeto `checklist` es solo de entrada. La cita guarda `checklistId`; la cabecera, grupos e items se guardan en la tabla de checklists. Todos esos registros y la cita se escriben en una sola transaccion: si falla, no se guarda ninguna parte. El checklist creado aparece en `GET /checklists` mediante el indice existente (puede haber un breve retraso por consistencia eventual).

`checklist` acepta el mismo body que `POST /checklists`: `titulo` obligatorio; `kind`, `emoji`, `colorValue`, `usaGrupos`, `grupos`, `items`, `id`/`checklistId` opcionales. No requiere items ni grupos. Si los items referencian grupos iniciales, enviar IDs explicitos para esos grupos. No se aceptan IDs de grupos ajenos al checklist.

### Crear sin checklist

Omitir tanto `checklistId` como `checklist`, o enviar `checklistId: null`. En un evento nuevo, la respuesta y el registro tendran `checklistId: null`.

### Editar el vinculo

`PUT /citas/{id}` conserva su actualizacion parcial: no requiere reenviar todos los campos de la cita.

| Body enviado | Resultado |
|---|---|
| `{"title":"Otro titulo"}` | Conserva el checklist actual |
| `{"checklistId":"otro-id"}` | Vincula otro checklist existente |
| `{"checklistId":null}` | Desvincula el checklist |
| `{"checklist":{"titulo":"Nueva lista","items":[{"nombre":"Reservar"}]}}` | Crea y vincula un checklist nuevo atomicamente |

Respuesta `200`:

```json
{
  "message": "Cita actualizada",
  "id": "uuid-cita",
  "checklistId": "id-vinculado-o-null"
}
```

En la respuesta real, `checklistId` es el string vinculado o el valor JSON `null`, no la cadena `"null"`.

Desvincular, reemplazar el vinculo o borrar una cita **no elimina** ningun checklist. Al cambiar una cita a un tipo distinto de `evento`, se elimina su referencia, no el checklist.

### Consultar y modificar desde el frontend

1. Cargar opciones con `GET /checklists` (respuesta `{items, count}`).
2. Crear o editar la cita usando una sola alternativa: `checklistId` o `checklist`.
3. Guardar el `checklistId` devuelto por POST/PUT.
4. Consultar el detalle con `GET /checklists/{checklistId}` (respuesta `{checklist, grupos, items, resumen}`).
5. Crear items con `POST /checklists/{checklistId}/items`; editar con `PUT /checklists/{checklistId}/items/{itemId}`.
6. Cambiar estado con `PATCH /checklists/{checklistId}/items/{itemId}/comprado` y body `{"comprado":true}`.

Ejemplo de `GET /citas/{id}` para un evento nuevo:

```json
{
  "id": "uuid-cita",
  "type": "evento",
  "title": "Picnic",
  "description": "Salida al parque",
  "date": "31-10-2026",
  "icon": "backpack_outlined",
  "documentos": [],
  "itinerario": {"actividades": []},
  "presupuesto": {"gastado": 0, "limite": 0, "conceptos": []},
  "checklistId": "uuid-checklist-nuevo"
}
```

El estado `comprado` es compartido: modificar o reiniciar un checklist afecta a todas las citas vinculadas a el. Para estados independientes, crear un checklist nuevo en lugar de reutilizar el ID.

`DELETE /checklists/{checklistId}` mantiene su comportamiento: elimina el checklist sin actualizar citas. Si una cita referencia un checklist borrado, su consulta de detalle devuelve `404`; el front debe permitir desvincularlo o seleccionar otro. No hay borrado en cascada ni integridad referencial automatica tras la vinculacion.

### Validaciones y errores

| HTTP | Caso |
|---|---|
| 400 | Enviar `checklistId` y `checklist` juntos, incluso si uno es `null` |
| 400 | `checklistId` vacio/no textual, `checklist: null`, o checklist sin `titulo` |
| 400 | Enviar cualquiera de esos campos para un tipo distinto de `evento` |
| 400 | Items/grupos invalidos, IDs repetidos, `groupId` desconocido o estado no booleano |
| 400 | Usar el antiguo campo `incluido`; ahora se usa `comprado` |
| 400 | Mas de 98 grupos e items iniciales combinados, incluyendo grupos por defecto |
| 404 | Cita a editar inexistente o checklist referenciado inexistente |
| 409 | ID de cita/checklist ya ocupado o conflicto concurrente detectado |
| 502 | Otro error de DynamoDB; no se confirma la transaccion |

Errores de citas usan `message` y errores de checklists usan `error`, conservando las convenciones de cada servicio. Tambien aplican los limites de DynamoDB: 400 KB por registro y 4 MB por transaccion. Para listas mayores, crear la cabecera y agregar grupos/items por los endpoints individuales.

No hay deduplicacion automatica si el cliente reintenta con nuevos UUID tras perder una respuesta. Consultar el estado antes de reintentar; el front puede enviar IDs estables para la cita y el checklist nuevo, y tratar un `409` consultando esos IDs.

POST masivo `/citas` conserva `{created: [ids], errors: [{index, error}]}` y HTTP `201`/`207`. Cada cita se guarda en su propia transaccion; el lote completo no es atomico. Para obtener el checklist generado de cada cita, consultar su detalle usando el ID de `created`.

### Compatibilidad con citas antiguas

No hay migracion automatica ni escrituras durante GET. Los eventos antiguos pueden seguir devolviendo `checklist: {items: [...]}`; el front debe tolerarlo mientras se migran. Una actualizacion que omita el vinculo conserva ese contenido antiguo.

Para migrar, enviar PUT con `checklist: {titulo, items}` usando `comprado` en cada item (convertir el antiguo `incluido` al mismo valor booleano). El servicio crea el checklist en su tabla, guarda la referencia y elimina el objeto embebido en una transaccion. Tambien se puede reemplazar por un `checklistId` existente o desvincular con `checklistId: null`; ambas opciones retiran el objeto antiguo.

### Despliegue

Desplegar CitasCRUD, ChecklistsCRUD, CommonLayer y la plantilla SAM juntos. CitasCRUD necesita lectura/escritura sobre `ChecklistsTable` y `dynamodb:ConditionCheckItem`; la plantilla incluye estos permisos. No cambia el esquema ni se requiere una tabla nueva.

Base URL:

```text
https://{api-id}.execute-api.{region}.amazonaws.com/checklists
```

Todas las solicitudes con body usan el encabezado `Content-Type: application/json`. Las respuestas son JSON y los valores numericos de DynamoDB se entregan como `int` o `double`.

## Flujo de inicio recomendado

Al abrir por primera vez la seccion de checklists, el front debe invocar una vez:

```text
POST /checklists/seed-defaults
```

La operacion es idempotente. Crea solo los tableros que no existan:

| ID | Titulo | kind | Grupos iniciales |
|---|---|---|---|
| `default-supermercado` | Supermercado | `supermercado` | Si |
| `default-viaje` | Viaje | `viaje` | Si |
| `default-deseos` | Cosas por comprar | `deseos` | No |

Despues, invoca `GET /checklists` para pintar los tableros. Para abrir un tablero y disponer de sus grupos, items y resumen, usa `GET /checklists/{checklistId}`.

## Modelos de respuesta

### ChecklistBoard

```json
{
  "id": "default-supermercado",
  "checklistId": "default-supermercado",
  "entityType": "checklist_board",
  "titulo": "Supermercado",
  "kind": "supermercado",
  "emoji": "...",
  "colorValue": 4283215696,
  "usaGrupos": true,
  "isDefault": true,
  "createdAt": "2026-08-25T14:30:00Z",
  "updatedAt": "2026-08-25T14:30:00Z"
}
```

- `kind`: `supermercado`, `viaje`, `deseos` o `personalizado`.
- Si no se informa `emoji`, `colorValue` o `usaGrupos`, el backend asigna los valores del tipo.
- `id` y `checklistId` son equivalentes en las respuestas.
- `colorValue` es el entero ARGB esperado por `Color(colorValue)` en Flutter.

### ChecklistGroup

```json
{
  "id": "uuid",
  "checklistId": "default-supermercado",
  "entityType": "checklist_grupo",
  "type": "checklist_grupo",
  "nombre": "Frutas y verduras",
  "emoji": "...",
  "orden": 0,
  "createdAt": "2026-08-25T14:30:00Z",
  "updatedAt": "2026-08-25T14:30:00Z"
}
```

### ChecklistItem

```json
{
  "id": "uuid",
  "checklistId": "default-supermercado",
  "entityType": "checklist_item",
  "type": "checklist_item",
  "nombre": "Manzanas",
  "groupId": "uuid-del-grupo",
  "prioridad": "alta",
  "prioridadOrden": 1,
  "precio": 12500.0,
  "emoji": "...",
  "comprado": false,
  "nota": "Preferir verdes",
  "createdAt": "2026-08-25T14:30:00Z",
  "updatedAt": "2026-08-25T14:30:00Z"
}
```

- `groupId`, `precio`, `emoji` y `nota` son opcionales y pueden ser `null`.
- `prioridad`: `alta`, `media` o `baja`. Si se omite, toma `media`.
- `prioridadOrden`: entero mayor o igual a `1`; `1` es la mayor prioridad. Si se omite, toma `999`.
- `comprado` es booleano y, si se omite al crear, toma `false`.
- Un item sin `groupId` se presenta como `Sin categoria` en el modelo Flutter.

## Endpoints

| Metodo | Ruta | Uso |
|---|---|---|
| GET | `/checklists` | Lista los tableros sin cargar grupos/items |
| POST | `/checklists` | Crea un tablero con grupos e items iniciales opcionales |
| POST | `/checklists/seed-defaults` | Crea/verifica los tres tableros principales |
| GET | `/checklists/{checklistId}` | Carga tablero, grupos, items y resumen |
| PUT | `/checklists/{checklistId}` | Actualiza campos del tablero |
| DELETE | `/checklists/{checklistId}` | Borra el tablero y todos sus grupos/items |
| PATCH | `/checklists/{checklistId}/reset` | Marca todos los items como no comprados |
| GET | `/checklists/{checklistId}/grupos` | Lista los grupos ordenados |
| POST | `/checklists/{checklistId}/grupos` | Crea un grupo |
| GET | `/checklists/{checklistId}/grupos/{itemId}` | Obtiene un grupo |
| PUT | `/checklists/{checklistId}/grupos/{itemId}` | Actualiza un grupo |
| DELETE | `/checklists/{checklistId}/grupos/{itemId}` | Borra un grupo y desasigna sus items |
| GET | `/checklists/{checklistId}/items` | Lista items pendientes antes que comprados y por prioridad numérica |
| POST | `/checklists/{checklistId}/items` | Crea un item |
| GET | `/checklists/{checklistId}/items/{itemId}` | Obtiene un item |
| PUT | `/checklists/{checklistId}/items/{itemId}` | Actualiza un item |
| DELETE | `/checklists/{checklistId}/items/{itemId}` | Borra un item |
| PATCH | `/checklists/{checklistId}/items/{itemId}/comprado` | Cambia solo el estado de compra |

## Respuestas y ejemplos

### Inicializar los tableros principales

`POST /checklists/seed-defaults`

No requiere body.

```json
{
  "message": "Checklists por defecto verificados",
  "created": ["default-supermercado", "default-viaje", "default-deseos"],
  "skipped": []
}
```

Los IDs ya existentes aparecen en `skipped`. El front puede ejecutar esta llamada cada vez que inicia la vista sin crear duplicados.

### Listar tableros

`GET /checklists`

```json
{
  "items": [
    {
      "id": "default-supermercado",
      "titulo": "Supermercado",
      "kind": "supermercado",
      "emoji": "...",
      "colorValue": 4283215696,
      "usaGrupos": true,
      "isDefault": true,
      "createdAt": "2026-08-25T14:30:00Z",
      "updatedAt": "2026-08-25T14:30:00Z"
    }
  ],
  "count": 1
}
```

La respuesta de listado no incluye `grupos` ni `items`. Solicita el detalle al navegar al checklist seleccionado.

### Crear checklist

`POST /checklists`

```json
{
  "titulo": "Mudanza",
  "kind": "personalizado",
  "emoji": "...",
  "colorValue": 4285161686,
  "usaGrupos": true,
  "grupos": [
    {"id": "cocina", "nombre": "Cocina", "emoji": "...", "orden": 0},
    {"nombre": "Habitacion", "emoji": "...", "orden": 1}
  ],
  "items": [
    {"nombre": "Empacar platos", "groupId": "cocina", "comprado": false}
  ]
}
```

Respuesta `201`:

```json
{
  "message": "Checklist creado",
  "checklistId": "uuid",
  "grupos": ["cocina", "uuid-grupo-2"],
  "items": ["uuid-item-1"]
}
```

Reglas al crear:

- `titulo` es requerido y no puede ser vacio.
- `kind` es opcional; por defecto es `personalizado`.
- Para `supermercado` y `viaje`, si `usaGrupos` es `true` y no se manda `grupos`, se crean las categorias predefinidas.
- En `deseos` los items pueden permanecer sin categoria; tambien puedes habilitar grupos explicitamente.
- Puedes enviar `id` o `checklistId` para usar un identificador conocido; de lo contrario se genera un UUID.
- `grupos` e `items`, si se envian, deben ser listas. `usaGrupos`, si se envia, debe ser booleano.
- Los items iniciales usan el modelo ChecklistItem; `comprado` debe ser booleano y no se admite `incluido`.
- Se validan todos los registros antes de escribirlos juntos: un error no deja una cabecera o grupos parciales.
- Se permiten hasta 98 grupos e items iniciales combinados. Se conservan las rutas individuales para agregar mas despues.
- La respuesta agrega `items` con los IDs de los items creados (array vacio si no se enviaron).

### Cargar el detalle de un checklist

`GET /checklists/default-supermercado`

```json
{
  "checklist": {
    "id": "default-supermercado",
    "titulo": "Supermercado",
    "kind": "supermercado",
    "usaGrupos": true
  },
  "grupos": [
    {"id": "uuid-frutas", "nombre": "Frutas y verduras", "orden": 0}
  ],
  "items": [
    {
      "id": "uuid-manzanas",
      "nombre": "Manzanas",
      "groupId": "uuid-frutas",
      "prioridad": "alta",
      "prioridadOrden": 1,
      "precio": 12500.0,
      "comprado": false
    }
  ],
  "resumen": {
    "totalItems": 1,
    "compradosCount": 0,
    "progreso": 0.0,
    "precioTotal": 12500.0,
    "precioPendiente": 12500.0
  }
}
```

`items` se ordena con pendientes primero, `prioridadOrden` ascendente y nombre. El valor `1` representa la mayor prioridad. Los items creados antes de este campo se ubican después de los que tienen una prioridad explícita.

### Crear y actualizar un grupo

`POST /checklists/{checklistId}/grupos`

```json
{
  "nombre": "Aseo",
  "emoji": "...",
  "orden": 2
}
```

`nombre` es requerido. Si se omite `orden`, se agrega al final.

`PUT /checklists/{checklistId}/grupos/{groupId}`

```json
{
  "nombre": "Limpieza y aseo",
  "orden": 3
}
```

Si se elimina un grupo, los items que lo referencian no se eliminan: se actualizan para quedar sin `groupId`.

### Crear y actualizar un item

`POST /checklists/{checklistId}/items`

```json
{
  "nombre": "Jabones",
  "groupId": "uuid-grupo-aseo",
  "prioridad": "media",
  "prioridadOrden": 2,
  "precio": 9800.0,
  "emoji": "...",
  "comprado": false,
  "nota": "Comprar dos unidades"
}
```

Respuesta `201`:

```json
{
  "message": "Item creado",
  "checklistId": "default-supermercado",
  "id": "uuid-item"
}
```

`PUT /checklists/{checklistId}/items/{itemId}` admite actualizaciones parciales:

```json
{
  "precio": 10500.0,
  "prioridad": "alta",
  "prioridadOrden": 1,
  "nota": "Marca preferida"
}
```

Para mover un item a otra categoria, actualiza `groupId`. Para dejarlo sin categoria, envia `"groupId": null`.

### Marcar comprado y resetear

Para el switch de cada item:

`PATCH /checklists/{checklistId}/items/{itemId}/comprado`

```json
{
  "comprado": true
}
```

Respuesta `200`:

```json
{
  "message": "Estado de compra actualizado"
}
```

Para el boton de reinicio de un tablero:

`PATCH /checklists/{checklistId}/reset`

No requiere body. Respuesta `200`:

```json
{
  "message": "Checklist reiniciado",
  "checklistId": "default-supermercado",
  "itemsActualizados": 12
}
```

## Integracion Flutter

El modelo proporcionado ya puede consumir los campos de la API. Esta implementacion usa `package:http/http.dart` y asume que `ChecklistBoard`, `ChecklistGroup` y `ChecklistItem` estan definidos en `lib/models/checklist.dart`.

```dart
import 'dart:convert';

import 'package:http/http.dart' as http;

import '../models/checklist.dart';

class ChecklistApiException implements Exception {
  ChecklistApiException(this.message, this.statusCode);

  final String message;
  final int statusCode;

  @override
  String toString() => 'ChecklistApiException($statusCode): $message';
}

class ChecklistsService {
  ChecklistsService(this.apiBaseUrl, {http.Client? client})
      : _client = client ?? http.Client();

  final String apiBaseUrl;
  final http.Client _client;

  Map<String, String> get _headers => const {
        'Content-Type': 'application/json',
      };

  Uri _uri(String path) => Uri.parse('$apiBaseUrl$path');

  Future<Map<String, dynamic>> _decode(http.Response response) async {
    final dynamic decoded = jsonDecode(response.body);
    final data = Map<String, dynamic>.from(decoded as Map);

    if (response.statusCode < 200 || response.statusCode >= 300) {
      throw ChecklistApiException(
        data['error']?.toString() ?? 'Error en Checklists API',
        response.statusCode,
      );
    }
    return data;
  }

  Future<void> seedDefaults() async {
    final response = await _client.post(
      _uri('/checklists/seed-defaults'),
      headers: _headers,
    );
    await _decode(response);
  }

  Future<List<ChecklistBoard>> getChecklists() async {
    final response = await _client.get(_uri('/checklists'), headers: _headers);
    final data = await _decode(response);
    return (data['items'] as List<dynamic>)
        .map((item) => ChecklistBoard.fromJson(Map<String, dynamic>.from(item as Map)))
        .toList();
  }

  Future<ChecklistBoard> getChecklist(String checklistId) async {
    final response = await _client.get(
      _uri('/checklists/$checklistId'),
      headers: _headers,
    );
    final data = await _decode(response);
    final board = Map<String, dynamic>.from(data['checklist'] as Map);
    board['grupos'] = data['grupos'];
    board['items'] = data['items'];
    return ChecklistBoard.fromJson(board);
  }

  Future<String> createChecklist({
    required String titulo,
    ChecklistKind kind = ChecklistKind.personalizado,
    String? emoji,
    int? colorValue,
    bool? usaGrupos,
    List<ChecklistGroup>? grupos,
  }) async {
    final response = await _client.post(
      _uri('/checklists'),
      headers: _headers,
      body: jsonEncode({
        'titulo': titulo,
        'kind': kind.name,
        if (emoji != null) 'emoji': emoji,
        if (colorValue != null) 'colorValue': colorValue,
        if (usaGrupos != null) 'usaGrupos': usaGrupos,
        if (grupos != null) 'grupos': grupos.map((group) => group.toJson()).toList(),
      }),
    );
    return (await _decode(response))['checklistId'].toString();
  }

  Future<String> createGroup(String checklistId, ChecklistGroup group) async {
    final response = await _client.post(
      _uri('/checklists/$checklistId/grupos'),
      headers: _headers,
      body: jsonEncode(group.toJson()),
    );
    return (await _decode(response))['id'].toString();
  }

  Future<String> createItem(String checklistId, ChecklistItem item) async {
    final response = await _client.post(
      _uri('/checklists/$checklistId/items'),
      headers: _headers,
      body: jsonEncode(item.toJson()),
    );
    return (await _decode(response))['id'].toString();
  }

  Future<void> updateItem(String checklistId, String itemId, Map<String, dynamic> patch) async {
    final response = await _client.put(
      _uri('/checklists/$checklistId/items/$itemId'),
      headers: _headers,
      body: jsonEncode(patch),
    );
    await _decode(response);
  }

  Future<void> setComprado(String checklistId, String itemId, bool comprado) async {
    final response = await _client.patch(
      _uri('/checklists/$checklistId/items/$itemId/comprado'),
      headers: _headers,
      body: jsonEncode({'comprado': comprado}),
    );
    await _decode(response);
  }

  Future<void> resetChecklist(String checklistId) async {
    final response = await _client.patch(
      _uri('/checklists/$checklistId/reset'),
      headers: _headers,
    );
    await _decode(response);
  }

  Future<void> deleteItem(String checklistId, String itemId) async {
    final response = await _client.delete(
      _uri('/checklists/$checklistId/items/$itemId'),
      headers: _headers,
    );
    await _decode(response);
  }

  void dispose() => _client.close();
}
```

Uso sugerido en la pantalla:

```dart
await checklistsService.seedDefaults();
final boards = await checklistsService.getChecklists();
final supermercado = await checklistsService.getChecklist('default-supermercado');

await checklistsService.setComprado(
  supermercado.id,
  item.id,
  !item.comprado,
);

await checklistsService.resetChecklist(supermercado.id);
```

Despues de crear, editar, eliminar, marcar comprado o resetear, vuelve a cargar el checklist con `getChecklist(checklistId)`. Asi el estado local se mantiene alineado con los totales y el orden persistidos.

## Errores esperados

| Estado | Cuando ocurre | Forma de respuesta |
|---|---|---|
| 400 | Body invalido, campos requeridos faltantes, `kind` o `prioridad` invalidos, `comprado` no booleano | `{ "error": "..." }` |
| 404 | No existe el checklist, grupo o item solicitado | `{ "error": "..." }` |
| 405 | Metodo HTTP no permitido para la ruta | `{ "error": "..." }` |
| 502 | Error al acceder a DynamoDB | `{ "error": "Error de base de datos" }` |
| 500 | Error inesperado del servidor | `{ "error": "Error interno del servidor" }` |

El front debe mostrar `error` al usuario o registrarlo de forma controlada, sin asumir que todos los errores de red tienen el mismo formato.
