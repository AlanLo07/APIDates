# 🎉 APIDates - Backend Serverless para Parejas

API REST completamente serverless en AWS para gestionar planes, citas, finanzas, bodas y entretenimiento para parejas.

## 📚 Tabla de Contenidos
- [Stack Tecnológico](#stack-tecnológico)
- [CRUDs Disponibles](#cruds-disponibles)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Modelos de Datos](#modelos-de-datos)
- [Endpoints Rápida Referencia](#endpoints-rápida-referencia)
- [Despliegue](#despliegue)
- [Arquitectura](#arquitectura)

---

## 🛠️ Stack Tecnológico

| Componente | Tecnología |
|-----------|-----------|
| **Compute** | AWS Lambda (Python 3.14) |
| **API** | API Gateway HTTP API + CORS |
| **Database** | DynamoDB (NoSQL) |
| **Storage** | S3 (Imágenes y audios) |
| **Autenticación** | API Keys (por implementar) |
| **Infrastructure** | AWS SAM + CloudFormation |
| **Deployment** | PowerShell Scripts |

---

## 🎯 CRUDs Disponibles

### 1️⃣ **Challenges** — Retos/Desafíos
Gestiona retos/desafíos para parejas con niveles de intensidad.
- **Tabla:** `ChallengesTable`
- **Función:** `challenges-crud`
- **Campos:** `text`, `emoji`, `level` (suave|picante|atrevido)
- **Endpoints:** `/challenges/*`

### 2️⃣ **Dice** — Tres Dados
Acciones, Zonas y Modificadores para juegos de parejas.
- **Tabla:** `DiceTable`
- **Función:** `dice-crud`
- **Tipos:** `acciones`, `zonas`, `modificadores`
- **Endpoints:** `/dice/*`

### 3️⃣ **Finances** — Gestión de Finanzas
Registro de gastos, presupuestos y análisis financiero para parejas.
- **Tabla:** `FinancesTable`
- **Función:** `finances-crud`
- **Características:** 12 categorías, presupuestos mensuales, históricos
- **Endpoints:** `/finances/*`

### 4️⃣ **Bodas** — Administración de Bodas
Gestión completa de bodas (invitados, tareas, gastos, proveedores, etc.).
- **Tabla:** `BodasTable`
- **Función:** `bodas-crud`
- **Colecciones:** invitados, tareas, gastos, canciones, proveedores, looks, hospedaje, menú, álbum
- **Endpoints:** `/bodas/*`

### 5️⃣ **Planes & Citas** — Actividades
Gestión de planes turísticos y citas agendadas.
- **Tablas:** `Planes`, `Citas`, `LovePhrasesTable`
- **Funciones:** `planes-crud`, `citas-crud`, `love-phrases-crud`
- **Endpoints:** `/planes/*`, `/citas/*`, `/love-phrases/*`

### 6️⃣ **Imágenes & Audios** — Storage
Gestión de subida de imágenes y audios con URLs prefirmadas.
- **Storage:** S3 Bucket
- **Función:** `images-manager`
- **Endpoints:** `/images/upload-url`, `/audio/upload-url`

### 7️⃣ **Spotify** — Búsqueda y Recomendaciones
Consulta catálogo público de Spotify para búsquedas y recomendaciones por semilla.
- **Función:** `spotify-api`
- **Autenticación backend:** Client Credentials (sin login del usuario final)
- **Endpoints:** `/spotify/*`

---

## 📁 Estructura del Proyecto

```
APIDates/
├── lambdas/
│   ├── template.yaml                # SAM template (infraestructura)
│   ├── deploy.ps1                   # Script de despliegue
│   ├── functions/
│   │   ├── ChallengesCRUD/
│   │   │   ├── handler.py
│   │   │   └── requirements.txt
│   │   ├── DiceCRUD/
│   │   │   ├── handler.py
│   │   │   └── requirements.txt
│   │   ├── FinancesCRUD/
│   │   │   ├── handler.py
│   │   │   └── requirements.txt
│   │   ├── BodasCRUD/
│   │   │   └── handler.py
│   │   ├── DatesCRUD/          # Planes
│   │   │   └── handler.py
│   │   ├── CitasCRUD/
│   │   │   └── handler.py
│   │   ├── PhrasesCRUD/
│   │   │   └── handler.py
│   │   ├── ImagesManager/
│   │   │   └── handler.py
│   │   ├── KamasutraCRUD/
│   │   │   └── handler.py
│   │   ├── SpotifyAPI/
│   │   │   ├── handler.py
│   │   │   └── requirements.txt
│   │   └── RandomDates/
│   │       └── handler.py
│   └── layers/
│       └── baselayer/
│           └── python/
│               └── common/
│                   ├── __init__.py
│                   ├── utils.py    # Funciones compartidas
│                   └── requirements.txt
├── README.md                        # Este archivo
├── BODAS_CRUD_DOCUMENTATION.md     # Documentación detallada Bodas
├── FINANCES_CRUD_DOCUMENTATION.md  # Documentación detallada Finances
├── ARCHITECTURE.mmd                 # Diagrama de arquitectura
├── FLOW.mmd                         # Diagrama de flujo
└── SEQUENCE.mmd                     # Diagrama de secuencia
```

---

## 🗂️ Modelos de Datos

### Challenge
```json
{
  "id": "uuid",
  "text": "Denlen un masaje de manos por 3 minutos",
  "emoji": "🤲",
  "level": "suave|picante|atrevido",
  "createdAt": "ISO 8601",
  "updatedAt": "ISO 8601"
}
```

### Dice Entry
```json
{
  "id": "uuid",
  "text": "Besa",
  "level": "suave|picante|atrevido",
  "diceType": "acciones|zonas|modificadores",
  "createdAt": "ISO 8601",
  "updatedAt": "ISO 8601"
}
```

### Expense (Finances)
```json
{
  "PK": "PAREJA#DEFAULT",
  "SK": "GASTO#{gastoId}",
  "gastoId": "uuid",
  "title": "Cena",
  "amount": 45.90,
  "date": "ISO 8601",
  "category": "subscriptions|groceries|transport|dateNights|home|health|vacations|gifts|pets|hobbies|savings|others",
  "monthYear": "YYYY-MM",
  "note": "opcional",
  "createdBy": "email",
  "createdAt": "ISO 8601",
  "updatedAt": "ISO 8601"
}
```

### Boda
```json
{
  "PK": "BODA#{bodaId}",
  "SK": "META",
  "nombre": "Boda de Ana y Luis",
  "fechaEvento": "2027-04-21",
  "lugar": "Hacienda San Miguel",
  "direccion": "...",
  "mensajeBienvenida": "...",
  "dressCode": "Formal tropical",
  "contacto": "+52 5555555555",
  "instagramHashtag": "#AnaYLuis2027",
  "coverImage": "https://...",
  "createdAt": "ISO 8601",
  "updatedAt": "ISO 8601"
}
```

---

## 📡 Endpoints Rápida Referencia

### Challenges
| Método | Ruta | Descripción |
|--------|------|-------------|
| GET | `/challenges` | Listar todos |
| GET | `/challenges/random` | Uno aleatorio |
| GET | `/challenges/{id}` | Por ID |
| POST | `/challenges` | Crear (array o single) |
| PUT | `/challenges/{id}` | Actualizar |
| DELETE | `/challenges/{id}` | Eliminar |

**Filtros:** `?level=suave`

### Dice
| Método | Ruta | Descripción |
|--------|------|-------------|
| GET | `/dice` | Listar todos |
| GET | `/dice/random` | Uno aleatorio |
| GET | `/dice/{id}` | Por ID |
| POST | `/dice` | Crear (array o single) |
| PUT | `/dice/{id}` | Actualizar |
| DELETE | `/dice/{id}` | Eliminar |

**Filtros:** `?level=suave&diceType=acciones`

### Finances
| Método | Ruta | Descripción |
|--------|------|-------------|
| POST | `/finances/init` | Inicializar pareja |
| GET | `/finances` | Datos de pareja |
| GET | `/finances/resumen` | Resumen general |
| GET | `/finances/gastos` | Listar gastos |
| POST | `/finances/gastos` | Crear gasto |
| GET | `/finances/gastos/{id}` | Por ID |
| PUT | `/finances/gastos/{id}` | Actualizar |
| DELETE | `/finances/gastos/{id}` | Eliminar |
| POST | `/finances/presupuesto/{mes}` | Set presupuesto |
| GET | `/finances/presupuesto/{mes}` | Get presupuesto |
| GET | `/finances/historico` | Históricos |

**Filtros:** `?month=2026-07&category=groceries`

### Bodas
| Método | Ruta | Descripción |
|--------|------|-------------|
| GET | `/bodas` | Listar bodas |
| POST | `/bodas` | Crear boda |
| GET | `/bodas/{bodaId}` | Obtener boda |
| PUT | `/bodas/{bodaId}` | Actualizar boda |
| DELETE | `/bodas/{bodaId}` | Eliminar boda |
| GET | `/bodas/{bodaId}/{collection}` | Listar colección |
| POST | `/bodas/{bodaId}/{collection}` | Crear item |
| PUT | `/bodas/{bodaId}/{collection}/{itemId}` | Actualizar item |
| DELETE | `/bodas/{bodaId}/{collection}/{itemId}` | Eliminar item |
| PATCH | `/bodas/{bodaId}/invitados/{id}/rsvp` | Confirmar asistencia |
| POST | `/bodas/{bodaId}/album/upload-url` | URL para fotos |

### Spotify
| Método | Ruta | Descripción |
|--------|------|-------------|
| GET | `/spotify/search?q={texto}&type=track&limit=10&market=CO` | Buscar tracks, artists, albums o playlists |
| GET | `/spotify/recommendations?q={texto}&limit=20&market=CO&mood=romantico` | Recomendar tracks similares por búsqueda, mood y tuners opcionales |
| GET | `/spotify/moods` | Listar moods disponibles con tuners sugeridos |
| GET | `/spotify/genres?q=pop&limit=50` | Listar géneros disponibles para recomendaciones |
| GET | `/spotify/tracks/{id}?market=CO` | Obtener detalle de track |
| GET | `/spotify/artists/{id}` | Obtener detalle de artista |
| GET | `/spotify/artists/{id}/top-tracks?market=CO&limit=10` | Obtener top tracks de un artista |

**Notas:**
- `type` permitido en búsqueda: `track`, `artist`, `album`, `playlist`.
- `market` por defecto viene de `SpotifyMarket` en el deploy (ejemplo: `CO`).
- `recommendations` usa fallback por artista/top-tracks cuando no encuentra track semilla.
- Presets de `mood` disponibles: `romantico`, `fiesta`, `chill`, `focus`.
- `GET /spotify/moods` devuelve catálogo de moods con descripción y tuners para UI dinámica.
- Tuners opcionales en `recommendations`: `target_energy`, `target_valence`, `target_danceability`, `min_popularity`, `max_popularity`, `target_popularity`, `min_tempo`, `target_tempo`, `max_tempo` y variantes `min_/max_/target_` de acousticness, danceability, energy, instrumentalness, liveness, speechiness, valence.
- Si envías `mood` y además tuners explícitos, los tuners explícitos tienen prioridad.
- Estos endpoints usan catálogo público y no exponen `client_secret` al frontend.

---

## 🚀 Despliegue

### Requisitos Previos
```bash
# AWS CLI configurado
aws configure

# SAM CLI
pip install aws-sam-cli

# Python 3.14+ local
python --version
```

### Pasos de Despliegue

#### 1. Navegar a la carpeta
```bash
cd lambdas/
```

#### 2. Build SAM
```bash
sam build
```

#### 3. Despliegue Guiado (Primera vez)
```bash
sam deploy --guided
```

Responder:
```
Stack Name: citas-app
Region: us-east-1
Confirm changes: y
Allow IAM role creation: y
Save parameters: y
```

Cuando SAM pregunte parámetros de Spotify, configura:
```
SpotifyClientId: <tu_client_id>
SpotifyClientSecret: <tu_client_secret>
SpotifyMarket: CO
```

#### 4. Despliegue Rápido (Siguientes veces)
```bash
sam deploy
```

#### 5. Obtener URLs de Endpoints
```bash
aws cloudformation describe-stacks --stack-name citas-app --query 'Stacks[0].Outputs' --region us-east-1
```

---

## 📊 Arquitectura

La arquitectura sigue un patrón **HTTP API + Lambda + DynamoDB + S3**:

```
Cliente
  ↓
API Gateway HTTP (CORS habilitado)
  ↓
Lambda Functions (Python 3.14)
  ├─ ChallengesCRUD
  ├─ DiceCRUD
  ├─ FinancesCRUD
  ├─ BodasCRUD
  ├─ DatesCRUD
  ├─ CitasCRUD
  ├─ PhrasesCRUD
  ├─ ImagesManager
  ├─ KamasutraCRUD
  ├─ SpotifyAPI
  └─ RandomDates
  ↓
Shared Layer (common/utils.py)
  ↓
Database Layer
  ├─ DynamoDB (Challenges, Dice, Finances, Bodas, etc.)
  └─ S3 (Imágenes, audios)
```

**Características de Seguridad:**
- CORS configurado en API Gateway
- Validación de entrada en todos los handlers
- Logging con semáforos (⚪️🟢🔵🟡🔴🟤)
- SSE encryption en DynamoDB
- Point-in-time recovery en tablas
- Presigned URLs para S3

---

## 📖 Documentación Detallada

- **Finances:** Ver [FINANCES_CRUD_DOCUMENTATION.md](FINANCES_CRUD_DOCUMENTATION.md)
- **Bodas:** Ver [BODAS_CRUD_DOCUMENTATION.md](BODAS_CRUD_DOCUMENTATION.md)
- **Diagramas:** Ver [ARCHITECTURE.mmd](ARCHITECTURE.mmd), [FLOW.mmd](FLOW.mmd), [SEQUENCE.mmd](SEQUENCE.mmd)

---

## 🔍 Desarrollo Local

### SAM Local (Emular Lambda + API Gateway)
```bash
sam local start-api
```

La API estará en `http://localhost:3000`

### Testing
```bash
# Crear challenge
curl -X POST http://localhost:3000/challenges \
  -H "Content-Type: application/json" \
  -d '{"text":"Denle un masaje","emoji":"🤲","level":"suave"}'

# Obtener todos
curl http://localhost:3000/challenges

# Buscar tracks en Spotify
curl "http://localhost:3000/spotify/search?q=morat&type=track&limit=5&market=CO"

# Recomendaciones desde texto de búsqueda
curl "http://localhost:3000/spotify/recommendations?q=coldplay&limit=10&market=CO"
```

---

## 📝 Logging

Todos los handlers usan semáforos de logging para facilitar debugging:
- ⚪️ `INFO NOT RELEVANT` — Logs de entrada/inicio
- 🟢 `INFO RELEVANT` — Operaciones CRUD exitosas
- 🔵 `INFO VERY RELEVANT` — Cálculos y validaciones importantes
- 🟡 `WARNING` — Advertencias
- 🔴 `ERROR` — Errores y excepciones
- 🟤 `DEBUG` — Información de debugging

---

## 🐛 Troubleshooting

**Problema:** Lambda no puede acceder a DynamoDB
```bash
# Verificar que el rol IAM tiene permisos DynamoDBCrudPolicy
aws iam get-role --role-name citas-app-rolle
```

**Problema:** CORS errors en frontend
```bash
# Verificar CORS en API Gateway
aws apigatewayv2 get-apis
```

**Problema:** Tabla DynamoDB no existe
```bash
# Listar tablas
aws dynamodb list-tables
```

---

## 📞 Contacto & Contribución

Proyecto personal de parejas. Para contribuciones o mejoras, contactar al owner.

---

**Última actualización:** 2026-07-28  
**Versión:** 1.0 - MVP Completo
