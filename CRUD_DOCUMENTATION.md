# 📋 Nuevos CRUDs: Challenges y Dice

## 🚀 Descripción General

Se han agregado dos nuevos CRUDs a la API:

### 1. **Challenges CRUD** (`/challenges`)
Gestiona retos/desafíos para parejas con niveles de intensidad.

**Campos:**
- `id` (UUID) - Autogenerado si no se proporciona
- `text` (String) - El reto/desafío ✅ requerido
- `emoji` (String) - Emoji representativo ✅ requerido
- `level` (Enum) - `suave | picante | atrevido` ✅ requerido
- `createdAt` (ISO timestamp) - Autogenerado
- `updatedAt` (ISO timestamp) - Autogenerado

**Tabla DynamoDB:** `ChallengesTable`

---

### 2. **Dice CRUD** (`/dice`)
Gestiona los tres dados del juego: Acciones, Zonas y Modificadores.

**Campos:**
- `id` (UUID) - Autogenerado si no se proporciona
- `text` (String) - El contenido del dado ✅ requerido
- `level` (Enum) - `suave | picante | atrevido` ✅ requerido
- `diceType` (Enum) - `acciones | zonas | modificadores` ✅ requerido
- `createdAt` (ISO timestamp) - Autogenerado
- `updatedAt` (ISO timestamp) - Autogenerado

**Tabla DynamoDB:** `DiceTable`

---

## 📡 Endpoints

### Challenges

```
GET    /challenges              # Obtener todos, opcionalmente filtrado por ?level=suave
GET    /challenges/random       # Obtener uno aleatorio, opcionalmente por ?level=suave
GET    /challenges/{id}         # Obtener por ID
POST   /challenges              # Crear uno nuevo
POST   /challenges              # Crear múltiples (enviar array)
PUT    /challenges/{id}         # Actualizar
DELETE /challenges/{id}         # Eliminar
```

### Dice

```
GET    /dice                    # Obtener todos, opcionalmente ?level=suave y/o ?diceType=acciones
GET    /dice/random             # Obtener uno aleatorio, opcionalmente ?level=suave y/o ?diceType=acciones
GET    /dice/{id}               # Obtener por ID
POST   /dice                    # Crear uno nuevo
POST   /dice                    # Crear múltiples (enviar array)
PUT    /dice/{id}               # Actualizar
DELETE /dice/{id}               # Eliminar
```

---

## 📝 Ejemplos de Uso

### ✨ Challenges - Crear uno

```bash
curl -X POST https://api.example.com/challenges \
  -H "Content-Type: application/json" \
  -d '{
    "text": "Denle un masaje de manos por 3 minutos, sin hablar",
    "emoji": "🤲",
    "level": "suave"
  }'
```

**Respuesta (201):**
```json
{
  "message": "Reto creado con éxito",
  "id": "550e8400-e29b-41d4-a716-446655440000"
}
```

### ✨ Challenges - Cargar múltiples

```bash
curl -X POST https://api.example.com/challenges \
  -H "Content-Type: application/json" \
  -d '[
    {
      "text": "Denlen un masaje de manos por 3 minutos, sin hablar",
      "emoji": "🤲",
      "level": "suave"
    },
    {
      "text": "Bailen lento abrazados una canción especial",
      "emoji": "💃",
      "level": "suave"
    }
  ]'
```

**Respuesta (201):**
```json
{
  "message": "2 retos creados",
  "created": ["id1", "id2"],
  "errors": []
}
```

### 🎲 Dice - Crear entrada

```bash
curl -X POST https://api.example.com/dice \
  -H "Content-Type: application/json" \
  -d '{
    "text": "Besa",
    "level": "suave",
    "diceType": "acciones"
  }'
```

**Respuesta (201):**
```json
{
  "message": "Entrada de dado creada con éxito",
  "id": "550e8400-e29b-41d4-a716-446655440001"
}
```

### 🎲 Dice - Obtener aleatorio (Acciones, nivel Picante)

```bash
curl -X GET "https://api.example.com/dice/random?diceType=acciones&level=picante"
```

**Respuesta (200):**
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440001",
  "text": "Da pequeños besos por todo",
  "level": "picante",
  "diceType": "acciones",
  "createdAt": "2026-07-12T15:30:45Z",
  "updatedAt": "2026-07-12T15:30:45Z"
}
```

### 🎲 Dice - Obtener todos (Zonas)

```bash
curl -X GET "https://api.example.com/dice?diceType=zonas"
```

**Respuesta (200):**
```json
{
  "items": [
    {
      "id": "550e8400-e29b-41d4-a716-446655440001",
      "text": "la frente",
      "level": "suave",
      "diceType": "zonas",
      "createdAt": "2026-07-12T15:30:45Z",
      "updatedAt": "2026-07-12T15:30:45Z"
    },
    {
      "id": "550e8400-e29b-41d4-a716-446655440002",
      "text": "el cuello",
      "level": "picante",
      "diceType": "zonas",
      "createdAt": "2026-07-12T15:30:45Z",
      "updatedAt": "2026-07-12T15:30:45Z"
    }
  ],
  "count": 2
}
```

### 📝 Actualizar

```bash
curl -X PUT https://api.example.com/challenges/{id} \
  -H "Content-Type: application/json" \
  -d '{
    "emoji": "💜",
    "level": "picante"
  }'
```

**Respuesta (200):**
```json
{
  "message": "Reto actualizado",
  "id": "550e8400-e29b-41d4-a716-446655440000"
}
```

### 🗑️ Eliminar

```bash
curl -X DELETE https://api.example.com/challenges/{id}
```

**Respuesta (200):**
```json
{
  "message": "Reto eliminado con éxito",
  "id": "550e8400-e29b-41d4-a716-446655440000"
}
```

---

## 🔍 Filtros Disponibles

### Challenges
- `?level=suave|picante|atrevido` - Filtrar por nivel
- `?lastKey=uuid` - Paginación (usar LastEvaluatedKey de la respuesta anterior)

### Dice
- `?level=suave|picante|atrevido` - Filtrar por nivel
- `?diceType=acciones|zonas|modificadores` - Filtrar por tipo
- `?lastKey=uuid` - Paginación
- Los filtros se pueden combinar: `?level=picante&diceType=zonas`

---

## 🚨 Validaciones

### Challenges
- `text`: Requerido, no puede estar vacío
- `emoji`: Requerido, no puede estar vacío
- `level`: Requerido, debe ser uno de `suave`, `picante`, `atrevido`

### Dice
- `text`: Requerido, no puede estar vacío
- `level`: Requerido, debe ser uno de `suave`, `picante`, `atrevido`
- `diceType`: Requerido, debe ser uno de `acciones`, `zonas`, `modificadores`

---

## 🏗️ Estructura de Archivos Agregados

```
lambdas/
  functions/
    ChallengesCRUD/
      handler.py              # CRUD completo con validaciones
      requirements.txt        # Dependencias (boto3)
    DiceCRUD/
      handler.py              # CRUD con soporte para 3 tipos de dados
      requirements.txt        # Dependencias (boto3)
  template.yaml               # CloudFormation actualizado con:
                              #  - Nuevas variables de entorno
                              #  - Funciones Lambda para ambos CRUDs
                              #  - Tablas DynamoDB (ChallengesTable, DiceTable)
                              #  - Nuevos endpoints HTTP
                              #  - Outputs para los endpoints
```

---

## 📊 Logs con Semáforos

Todos los logs utilizan semáforos fijos:
- 🟢 INFO RELEVANT: Acciones exitosas (crear, actualizar, eliminar)
- 🟡 WARNING: Validaciones o búsquedas sin resultados
- 🔴 ERROR: Errores de DynamoDB o validación
- 🟤 DEBUG: Detalles de operaciones

---

## ✅ Próximos Pasos

1. **Desplegar el template:** `./deploy.ps1`
2. **Cargar datos iniciales:** Ver script `seed_data.py` (próximamente)
3. **Probar los endpoints:** Usar ejemplos curl arriba
4. **Integrar en Frontend:** Los datos ya son accesibles por API
