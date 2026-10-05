# CRUD de cuponeras

Cada usuario autenticado tiene como máximo una cuponera. El backend toma el
`userId` del claim `sub` del JWT de Cognito; el cliente nunca lo envía.

## Modelo

La tabla `CouponsTable` usa `userId` como clave de partición. Cada fila guarda:

```json
{
  "userId": "cognito-sub",
  "regalos": [
    {
      "nombre": "Cena favorita",
      "descripcion": "Una cena en el lugar que elijas",
      "canjeado": false
    }
  ],
  "createdAt": "2026-10-05T00:00:00+00:00",
  "updatedAt": "2026-10-05T00:00:00+00:00"
}
```

Los nombres de regalos son únicos dentro de una cuponera y no distinguen entre
mayúsculas y minúsculas.

## Rutas

Todas requieren `Authorization: Bearer <JWT>`.

| Método | Ruta | Acción |
| --- | --- | --- |
| `GET` | `/cuponera` | Obtiene la cuponera del usuario |
| `POST` | `/cuponera` | Crea la cuponera; acepta `regalos` opcional |
| `PUT` | `/cuponera` | Reemplaza la lista completa de regalos |
| `DELETE` | `/cuponera` | Elimina la cuponera |
| `GET` | `/cuponera/regalos` | Lista los regalos |
| `POST` | `/cuponera/regalos` | Agrega un regalo |
| `GET` | `/cuponera/regalos/{nombre}` | Obtiene un regalo por nombre |
| `PUT` | `/cuponera/regalos/{nombre}` | Reemplaza un regalo |
| `DELETE` | `/cuponera/regalos/{nombre}` | Elimina un regalo |
| `PATCH` | `/cuponera/regalos/{nombre}/canjear` | Cambia su estado de canje |

Para nombres con espacios o caracteres especiales se debe usar URL encoding.

### Crear una cuponera

```json
{
  "regalos": [
    {
      "nombre": "Desayuno en cama",
      "descripcion": "Incluye café y el desayuno que prefieras",
      "canjeado": false
    }
  ]
}
```

### Crear o reemplazar un regalo

```json
{
  "nombre": "Tarde de películas",
  "descripcion": "Tú eliges las películas y los snacks",
  "canjeado": false
}
```

### Canjear o descanjear

`canjeado` es opcional en esta ruta y vale `true` por defecto.

```json
{
  "canjeado": true
}
```

## Reinicio semestral

La función tiene asociada la regla EventBridge
`cron(0 0 1 1,7 ? *)`, que descanjea todos los regalos el 1 de enero y el 1 de
julio a las 00:00 UTC. Se despliega con `Enabled: false`; por tanto, queda
creada pero no se ejecuta hasta habilitarla explícitamente.