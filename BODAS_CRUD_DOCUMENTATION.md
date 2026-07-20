# API de Bodas

Este backend agrega un nuevo dominio `bodas` para soportar dos apps:

- App de pareja: administra invitados, checklist, gastos, proveedores, música, looks e itinerario.
- App de invitados: consulta información pública de la boda, revisa el itinerario y confirma asistencia.

## Tabla DynamoDB

Se usa una sola tabla llamada `BodasTable` con modelo de agregado por boda.

### Claves

- `pk = BODA#{bodaId}`
- `sk = META` para el registro principal de la boda
- `sk = INVITADO#{id}`
- `sk = TAREA#{id}`
- `sk = PASO#{id}`
- `sk = GASTO#{id}`
- `sk = CANCION#{id}`
- `sk = PROVEEDOR#{id}`
- `sk = LOOK#{id}`
- `sk = HOSPEDAJE#{id}`
- `sk = MENU#{id}`
- `sk = FOTO#{id}`

### GSI

- `entityType-index`: permite listar rápidamente las bodas raíz (`entityType = boda`).

## Payload de la boda raíz

`POST /bodas`

```json
{
  "nombre": "Boda de Ana y Luis",
  "fechaEvento": "2027-04-21",
  "lugar": "Hacienda San Miguel",
  "direccion": "Carretera 57 km 12",
  "mensajeBienvenida": "Gracias por acompañarnos",
  "dressCode": "Formal tropical",
  "contacto": "+52 5555555555",
  "instagramHashtag": "#AnaYLuis2027",
  "coverImage": "https://.../cover.jpg"
}
```

## Colecciones soportadas

- `invitados`
- `tareas`
- `itinerario`
- `gastos`
- `canciones`
- `proveedores`
- `looks`
- `hospedaje`
- `menu`
- `album`

Cada item mantiene el `type` esperado por tu modelo Dart:

- `invitado`
- `tarea_boda`
- `paso_boda`
- `gasto_boda`
- `cancion_boda`
- `proveedor_boda`
- `look_boda`
- `hospedaje_boda`
- `menu_boda`
- `foto_boda`

## Endpoints

### Bodas

- `GET /bodas`
- `POST /bodas`
- `GET /bodas/{bodaId}`
- `PUT /bodas/{bodaId}`
- `DELETE /bodas/{bodaId}`
- `GET /bodas/{bodaId}/public`

### CRUD genérico por colección

- `GET /bodas/{bodaId}/{collection}`
- `POST /bodas/{bodaId}/{collection}`
- `GET /bodas/{bodaId}/{collection}/{itemId}`
- `PUT /bodas/{bodaId}/{collection}/{itemId}`
- `DELETE /bodas/{bodaId}/{collection}/{itemId}`

### Upload de fotos para álbum

- `POST /bodas/{bodaId}/album/upload-url`

Body:

```json
{
  "fileName": "selfie.jpg",
  "fileType": "image/jpeg",
  "titulo": "Entrada al evento",
  "subidoPor": "Carlos",
  "comentario": "Llegamos temprano"
}
```

Respuesta:

```json
{
  "message": "Upload URL creada",
  "id": "uuid-foto",
  "key": "weddings/{bodaId}/album/{uuid}.jpg",
  "uploadUrl": "https://...presigned...",
  "finalUrl": "https://bucket.s3.amazonaws.com/weddings/{bodaId}/album/{uuid}.jpg"
}
```

Tipos de imagen permitidos:

- `image/jpeg`
- `image/jpg`
- `image/png`
- `image/webp`
- `image/heic`

### RSVP para invitados

- `PATCH /bodas/{bodaId}/invitados/{itemId}/rsvp`

Body:

```json
{
  "rsvp": "confirmado",
  "personas": 2
}
```

Valores válidos para `rsvp`:

- `confirmado`
- `pendiente`
- `noVa`

Valores válidos para `estado` en proveedores:

- `pendiente`
- `confirmado`
- `pagado`

## Ejemplos por colección

### Invitado

`POST /bodas/{bodaId}/invitados`

```json
{
  "nombre": "María Pérez",
  "grupo": "Familia",
  "personas": 2,
  "rsvp": "pendiente"
}
```

### Tarea

`POST /bodas/{bodaId}/tareas`

```json
{
  "titulo": "Confirmar menú",
  "categoria": "Catering",
  "completada": false,
  "fechaLimite": "15-03-2027"
}
```

### Itinerario

`POST /bodas/{bodaId}/itinerario`

```json
{
  "titulo": "Ceremonia",
  "hora": "17:00",
  "nota": "Llegar 20 minutos antes",
  "localizacion": "Jardín principal",
  "coordenadas": {
    "lat": 20.65341,
    "lng": -103.32523
  },
  "emoji": "💒"
}
```

### Gasto

`POST /bodas/{bodaId}/gastos`

```json
{
  "concepto": "Banquete",
  "categoria": "Catering",
  "estimado": 50000,
  "pagado": 15000
}
```

### Canción

`POST /bodas/{bodaId}/canciones`

```json
{
  "titulo": "Perfect",
  "artista": "Ed Sheeran",
  "momento": "Primer baile",
  "link": "https://open.spotify.com/..."
}
```

### Proveedor

`POST /bodas/{bodaId}/proveedores`

```json
{
  "nombre": "Floristería Roma",
  "categoria": "Flores",
  "contacto": "+52 5555555555",
  "link": "https://instagram.com/...",
  "costo": 12000,
  "estado": "confirmado",
  "notas": "Incluye montaje"
}
```

### Look

`POST /bodas/{bodaId}/looks`

```json
{
  "persona": "Ella",
  "prenda": "Vestido",
  "tienda": "Novias Centro",
  "talla": "M",
  "precio": 18000,
  "comprado": true,
  "notas": "Ajuste pendiente"
}
```

### Hospedaje

`POST /bodas/{bodaId}/hospedaje`

```json
{
  "nombre": "Hotel Vista Mar",
  "direccion": "Av. Costera 123",
  "contacto": "+52 5512345678",
  "checkIn": "2027-04-20 15:00",
  "checkOut": "2027-04-22 12:00",
  "mapaUrl": "https://maps.google.com/...",
  "nota": "Mencionar código ANA-LUIS"
}
```

### Menú

`POST /bodas/{bodaId}/menu`

```json
{
  "nombre": "Ravioles de espinaca",
  "momento": "Cena",
  "descripcion": "Con salsa de cuatro quesos",
  "tipo": "Plato fuerte",
  "restricciones": ["Sin nuez"],
  "esVegetariano": true
}
```

### Álbum (carga manual opcional)

`POST /bodas/{bodaId}/album`

```json
{
  "titulo": "Primer baile",
  "url": "https://bucket.s3.amazonaws.com/weddings/{bodaId}/album/abc.jpg",
  "s3Key": "weddings/{bodaId}/album/abc.jpg",
  "mimeType": "image/jpeg",
  "subidoPor": "Invitado",
  "comentario": "Hermoso momento"
}
```

## Uso desde Flutter

Tu modelo Dart actual puede consumir estos payloads casi sin transformación porque los nombres de campos y el `type` se conservan. La única entidad nueva es la boda raíz, que sirve como contenedor general para ambas apps.

## Siguiente paso recomendado

Agregar autenticación y separación de permisos:

- app pareja: acceso completo a `/bodas/**`
- app invitados: acceso de solo lectura a `/bodas/{bodaId}/public` y escritura acotada a RSVP