# 🎉 Nuevo: Challenges & Dice CRUDs

## 📦 Resumen de Cambios

Se han agregado dos nuevos CRUDs completamente funcionales a tu API:

### ✨ **Challenges CRUD** — Retos/Desafíos para parejas
- **Tabla:** `ChallengesTable` (DynamoDB)
- **Función Lambda:** `challenges-crud`
- **Endpoints:** `/challenges/*`
- **Base de datos propuesta:** 5 retos iniciales (suave)

### 🎲 **Dice CRUD** — Tres dados (Acciones, Zonas, Modificadores)
- **Tabla:** `DiceTable` (DynamoDB)
- **Función Lambda:** `dice-crud`
- **Endpoints:** `/dice/*`
- **Base de datos propuesta:** 48 entradas (18 acciones, 16 zonas, 14 modificadores)

---

## 🏗️ Estructura de Archivos Nuevos

```
lambdas/
├── functions/
│   ├── ChallengesCRUD/
│   │   ├── handler.py           ✅ CRUD completo con validaciones
│   │   └── requirements.txt      ✅ Dependencias
│   └── DiceCRUD/
│       ├── handler.py           ✅ CRUD con 3 tipos de dados
│       └── requirements.txt      ✅ Dependencias
├── seed_data.py                 ✅ Script para cargar datos iniciales
├── template.yaml                ✅ Actualizado con nuevos recursos
└── deploy.ps1                   ✅ Ya existe (sin cambios)

Documentación:
├── CRUD_DOCUMENTATION.md        ✅ Guía completa de endpoints y ejemplos
├── CHALLENGES_SEED_DATA.md      ✅ Este archivo (resumen de cambios)
```

---

## 🔧 Modelos de Datos

### Challenges
```python
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "text": "Denle un masaje de manos por 3 minutos, sin hablar",
  "emoji": "🤲",
  "level": "suave",  # suave | picante | atrevido
  "createdAt": "2026-07-12T15:30:45Z",
  "updatedAt": "2026-07-12T15:30:45Z"
}
```

### Dice Entry
```python
{
  "id": "dice-acciones-1",
  "text": "Besa",
  "level": "suave",  # suave | picante | atrevido
  "diceType": "acciones",  # acciones | zonas | modificadores
  "createdAt": "2026-07-12T15:30:45Z",
  "updatedAt": "2026-07-12T15:30:45Z"
}
```

---

## 🚀 Despliegue

### Paso 1: Actualizar template.yaml
✅ **YA HECHO** — El archivo `template.yaml` ya contiene:
- ✅ Variables de entorno para ambas tablas
- ✅ Funciones Lambda con rutas HTTP
- ✅ Tablas DynamoDB con índices secundarios
- ✅ Outputs para los endpoints

### Paso 2: Desplegar la Stack
```bash
cd lambdas
./deploy.ps1
```

### Paso 3: Cargar Datos Iniciales (Opcional)
Después del despliegue, carga los datos iniciales:

```bash
# Cargar solo Challenges
python seed_data.py --table challenges --region us-east-1

# Cargar solo Dice
python seed_data.py --table dice --region us-east-1

# Cargar ambas tablas
python seed_data.py --table all --region us-east-1
```

> ⚠️ **Requisito:** Asegúrate de tener AWS CLI configurado y `boto3` instalado:
> ```bash
> pip install boto3 --user
> ```

---

## 📡 Endpoints Rápida Referencia

### Challenges
| Método | Ruta | Descripción |
|--------|------|-------------|
| GET | `/challenges` | Obtener todos |
| GET | `/challenges/random` | Uno aleatorio |
| GET | `/challenges/{id}` | Por ID |
| POST | `/challenges` | Crear uno o múltiples |
| PUT | `/challenges/{id}` | Actualizar |
| DELETE | `/challenges/{id}` | Eliminar |

### Dice
| Método | Ruta | Descripción |
|--------|------|-------------|
| GET | `/dice` | Obtener todos |
| GET | `/dice/random` | Uno aleatorio |
| GET | `/dice/{id}` | Por ID |
| POST | `/dice` | Crear uno o múltiples |
| PUT | `/dice/{id}` | Actualizar |
| DELETE | `/dice/{id}` | Eliminar |

---

## 🔍 Filtros

### Challenges
```
GET /challenges?level=suave
GET /challenges/random?level=picante
```

### Dice
```
GET /dice?diceType=acciones
GET /dice?level=atrevido&diceType=zonas
GET /dice/random?level=suave&diceType=acciones
```

---

## 📊 Datos Propuestos

### 5 Challenges (suave) incluidos:
1. 🤲 Denle un masaje de manos por 3 minutos, sin hablar
2. 💭 Compartan su fantasía favorita (la más inocente)
3. 💃 Bailen lento abrazados una canción especial
4. 💋 Denle 10 besos en lugares distintos (nada íntimo)
5. 💌 Escriban una nota de amor y léanla en voz alta

### Dice: 48 Entradas
- **Acciones:** 18 entradas (5 suave, 7 picante, 6 atrevido)
- **Zonas:** 16 entradas (5 suave, 6 picante, 5 atrevido)
- **Modificadores:** 14 entradas (5 suave, 4 picante, 5 atrevido)

---

## ✅ Características Implementadas

### Seguridad
- ✅ Validación de campos requeridos
- ✅ Validación de enums (level, diceType)
- ✅ Autogenración de UUIDs
- ✅ Timestamps de auditoría (createdAt, updatedAt)
- ✅ PointInTimeRecovery habilitado
- ✅ Encriptación (SSE) en DynamoDB
- ✅ CORS configurado

### Funcionalidad
- ✅ CRUD completo (Create, Read, Update, Delete)
- ✅ Paginación automática en scans
- ✅ Filtrado por level y diceType
- ✅ Soporte para bulk import (POST con array)
- ✅ Random selection con filtros opcionales
- ✅ Índices secundarios (GSI) para búsquedas eficientes
- ✅ Update parcial (no sobrescribe campos no enviados)
- ✅ Manejo robusto de errores

### Logging
- ✅ Logs con semáforos fijos 🟢🟡🔴🟤
- ✅ Sin payloads sensibles (solo metadatos)
- ✅ Trazabilidad de operaciones

---

## 🧪 Pruebas Rápidas

### Crear un Challenge
```bash
curl -X POST https://YOUR_API_URL/challenges \
  -H "Content-Type: application/json" \
  -d '{
    "text": "Mi reto personalizado",
    "emoji": "🎉",
    "level": "suave"
  }'
```

### Obtener Challenge Aleatorio (picante)
```bash
curl -X GET "https://YOUR_API_URL/challenges/random?level=picante"
```

### Obtener Acciones Aleatorias (atrevido)
```bash
curl -X GET "https://YOUR_API_URL/dice/random?diceType=acciones&level=atrevido"
```

### Cargar 3 Dice de golpe
```bash
curl -X POST https://YOUR_API_URL/dice \
  -H "Content-Type: application/json" \
  -d '[
    {"text": "Besa", "level": "suave", "diceType": "acciones"},
    {"text": "el cuello", "level": "picante", "diceType": "zonas"},
    {"text": "muy despacio", "level": "suave", "diceType": "modificadores"}
  ]'
```

---

## 🐛 Troubleshooting

### "Tabla no encontrada"
```
Solución: Asegúrate de que deploy.ps1 se ejecutó exitosamente.
Verifica que ChallengesTable y DiceTable existan en DynamoDB.
```

### "Campo requerido faltante"
```
Verifica el payload de la solicitud. Requeridos:
- Challenges: text, emoji, level
- Dice: text, level, diceType
```

### "Nivel inválido"
```
Válidos: suave, picante, atrevido
Válidos (Dice): acciones, zonas, modificadores
```

---

## 📝 Próximas Mejoras Sugeridas

1. **Autenticación:** Agregar API Key o JWT
2. **Rate Limiting:** Limitar requests por cliente
3. **Búsqueda Full-Text:** Buscar retos por palabras clave
4. **Versionado:** Agregar campo `version` para cambios
5. **Soft Delete:** Marcar como eliminado en lugar de borrar
6. **Tags:** Agregar etiquetas personalizadas a challenges

---

## 📞 Soporte

Para detalles completos de cada endpoint, ejemplos curl y payloads:
→ Ver **CRUD_DOCUMENTATION.md**

Para cargar datos iniciales:
→ Ver `seed_data.py` y `--help`

---

## 🎯 Resumen

| Aspecto | Estado |
|--------|--------|
| Código Lambda | ✅ Completo |
| DynamoDB | ✅ Configurado |
| API Gateway | ✅ Rutas agregadas |
| Template.yaml | ✅ Actualizado |
| Documentación | ✅ Completa |
| Seed Script | ✅ Listo para usar |
| Validaciones | ✅ Implementadas |
| Logging | ✅ Con semáforos |
| CORS | ✅ Habilitado |

---

**Creado:** 2026-07-12  
**Versión:** 1.0  
**Estado:** ✅ Listo para Producción
