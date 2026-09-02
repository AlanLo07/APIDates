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
| **Compute** | AWS Lambda (Python 3.13) |
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

### 5️⃣ **Checklists** — Compras, Viajes y Listas
Tableros de supermercado, viaje, deseos/compras y listas personalizadas.
- **Tabla:** `ChecklistsTable`
- **Función:** `checklists-crud`
- **Colecciones:** grupos, items
- **Características:** prioridades, precio opcional, estado comprado y reinicio masivo
- **Endpoints:** `/checklists/*`

### 6️⃣ **Planes & Citas** — Actividades
Gestión de planes turísticos y citas agendadas.
- **Tablas:** `Planes`, `Citas`, `LovePhrasesTable`
- **Funciones:** `planes-crud`, `citas-crud`, `love-phrases-crud`
- **Endpoints:** `/planes/*`, `/citas/*`, `/love-phrases/*`

### 7️⃣ **Imágenes & Audios** — Storage
Gestión de subida de imágenes y audios con URLs prefirmadas.
- **Storage:** S3 Bucket
- **Función:** `images-manager`
- **Endpoints:** `/images/upload-url`, `/audio/upload-url`

### 8️⃣ **Spotify** — Catálogo y Reproductor
Consulta catálogo público de Spotify (search, tracks, artists, albums, playlists) y controla la reproducción del usuario (player).
- **Función:** `spotify-api`
- **Autenticación catálogo:** Client Credentials (sin login del usuario final)
- **Autenticación player:** Authorization Code (el usuario vincula su cuenta vía `/spotify/login`)
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
│   │   ├── ChecklistsCRUD/
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

### Checklist
```json
{
  "id": "uuid",
  "titulo": "Supermercado",
  "kind": "supermercado|viaje|deseos|personalizado",
  "emoji": "...",
  "colorValue": 4283215696,
  "usaGrupos": true,
  "createdAt": "ISO 8601",
  "updatedAt": "ISO 8601"
}
```

### Checklist Item
```json
{
  "id": "uuid",
  "checklistId": "uuid",
  "nombre": "Manzanas",
  "groupId": "uuid|null",
  "prioridad": "alta|media|baja",
  "precio": 12500.0,
  "comprado": false,
  "nota": "opcional",
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

### Checklists
| Método | Ruta | Descripción |
|--------|------|-------------|
| GET | `/checklists` | Listar tableros |
| POST | `/checklists` | Crear tablero |
| POST | `/checklists/seed-defaults` | Crear/verificar listas principales |
| GET | `/checklists/{checklistId}` | Tablero completo, grupos, items y resumen |
| PUT | `/checklists/{checklistId}` | Actualizar tablero |
| DELETE | `/checklists/{checklistId}` | Eliminar tablero y contenido |
| PATCH | `/checklists/{checklistId}/reset` | Marcar todos los items como pendientes |
| GET/POST | `/checklists/{checklistId}/grupos` | Listar o crear grupos |
| GET/PUT/DELETE | `/checklists/{checklistId}/grupos/{itemId}` | Gestionar grupo |
| GET/POST | `/checklists/{checklistId}/items` | Listar o crear items |
| GET/PUT/DELETE | `/checklists/{checklistId}/items/{itemId}` | Gestionar item |
| PATCH | `/checklists/{checklistId}/items/{itemId}/comprado` | Cambiar estado comprado |

### Spotify
| Método | Ruta | Descripción |
|--------|------|-------------|
| GET | `/spotify/search?q={texto}&type=track&limit=10&market=CO` | Buscar tracks, artists, albums o playlists |
| GET | `/spotify/tracks/{id}?market=CO` | Obtener detalle de track |
| GET | `/spotify/artists/{id}` | Obtener detalle de artista |
| GET | `/spotify/artists/{id}/top-tracks?market=CO&limit=10` | Obtener top tracks de un artista |
| GET | `/spotify/artists/{id}/albums?market=CO&limit=20` | Listar álbumes/sencillos de un artista |
| GET | `/spotify/albums/{id}?market=CO` | Obtener detalle de álbum |
| GET | `/spotify/albums/{id}/tracks?market=CO&limit=20` | Listar tracks de un álbum |
| GET | `/spotify/playlists/{id}?market=CO` | Obtener detalle de playlist |
| GET | `/spotify/playlists/{id}/tracks?market=CO&limit=20` | Listar tracks de una playlist |
| GET | `/spotify/login` | Devuelve `authUrl` para vincular la cuenta del usuario (requiere JWT) |
| GET | `/spotify/callback` | Callback público de Spotify, guarda tokens del usuario |
| GET | `/spotify/player?market=CO` | Estado actual de reproducción del usuario |
| GET | `/spotify/player/currently-playing?market=CO` | Track que se está reproduciendo |
| GET | `/spotify/player/devices` | Dispositivos disponibles del usuario |
| PUT | `/spotify/player/play` | Reanudar/iniciar reproducción (`context_uri`, `uris`, `device_id` opcionales) |
| PUT | `/spotify/player/pause` | Pausar reproducción |
| PUT | `/spotify/player/volume?volume_percent=50` | Ajustar volumen |
| POST | `/spotify/player/next` | Saltar a la siguiente pista |
| POST | `/spotify/player/previous` | Volver a la pista anterior |

**Notas:**
- `type` permitido en búsqueda: `track`, `artist`, `album`, `playlist`.
- `market` por defecto viene de `SpotifyMarket` en el deploy (ejemplo: `CO`).
- `recommendations`, `moods` y `genres` se eliminaron por depender de la familia `recommendations`, marcada como deprecated por Spotify.
- Los endpoints de catálogo usan Client Credentials y no exponen `client_secret` al frontend.
- Los endpoints de `player` requieren que el usuario vincule su cuenta antes con `/spotify/login` (Authorization Code flow); los tokens se guardan por usuario en DynamoDB (`SpotifyUserTokens`).

---

## 🚀 Despliegue

### Requisitos Previos
```bash
# AWS CLI configurado
aws configure

# SAM CLI
pip install aws-sam-cli

# Python 3.13+ local
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
SpotifyRedirectUri: https://<tu-api-id>.execute-api.<region>.amazonaws.com/spotify/callback
SpotifyStateSecret: <secreto_aleatorio_largo>
```

`SpotifyRedirectUri` debe coincidir exactamente con la URL registrada en el dashboard de Spotify Developer para tu app (Redirect URIs).

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
Lambda Functions (Python 3.13)
  ├─ ChallengesCRUD
  ├─ DiceCRUD
  ├─ FinancesCRUD
  ├─ BodasCRUD
  ├─ ChecklistsCRUD
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
- **Checklists:** Ver [CHECKLISTS_CRUD_DOCUMENTATION.md](CHECKLISTS_CRUD_DOCUMENTATION.md)
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

**Última actualización:** 2026-08-25  
**Versión:** 1.0 - MVP Completo
