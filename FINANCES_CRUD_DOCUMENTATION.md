# 💰 CRUD Finanzas de Parejas - API Documentation

## 📋 Descripción General

API REST para gestión de finanzas compartidas de una pareja. Permite registrar gastos, establecer presupuestos mensuales, categorizar transacciones y visualizar históricos de gastos.

**Modelo Simplificado (Una Pareja Única):**
- 🔵 PK fija: `PAREJA#DEFAULT` (sin parejaId en rutas)
- 🔵 Rutas más limpias y simples
- 🔵 Mejor rendimiento para caso de uso único

**Características:**
- ✅ Inicialización de pareja (POST /finances/init)
- ✅ CRUD completo de gastos (Create, Read, Update, Delete)
- ✅ Sistema de categorías predefinidas (12 categorías)
- ✅ Presupuestos mensuales con alertas
- ✅ Cálculo automático de estadísticas por mes
- ✅ Histórico de gastos con análisis
- ✅ Filtrado por mes y categoría
- ✅ Auditoría de cambios (createdAt, updatedAt)
- ✅ Logging con semáforos (⚪️🟢🔵🟡🔴🟤)

---

## 🗂️ Estructura de Datos

### Modelo de Pareja
```json
{
  "PK": "PAREJA#DEFAULT",
  "SK": "META",
  "user1": {
    "name": "string",
    "email": "string"
  },
  "user2": {
    "name": "string",
    "email": "string"
  },
  "monthlyBudget": 0,
  "currency": "$",
  "locale": "es_ES",
  "createdAt": "ISO 8601",
  "updatedAt": "ISO 8601"
}
```

### Modelo de Gasto
```json
{
  "PK": "PAREJA#DEFAULT",
  "SK": "GASTO#{gastoId}",
  "gastoId": "string (UUID)",
  "title": "string (concepto)",
  "amount": 0.00,
  "date": "ISO 8601",
  "category": "subscriptions|groceries|transport|dateNights|home|health|vacations|gifts|pets|hobbies|savings|others",
  "monthYear": "YYYY-MM",
  "note": "string (opcional)",
  "createdBy": "email del usuario",
  "createdAt": "ISO 8601",
  "updatedAt": "ISO 8601"
}
```

### Modelo de Presupuesto Mensual
```json
{
  "PK": "PAREJA#DEFAULT",
  "SK": "PRESUPUESTO#{monthYear}",
  "monthYear": "YYYY-MM",
  "amount": 0.00,
  "notes": "string (opcional)",
  "createdAt": "ISO 8601",
  "updatedAt": "ISO 8601"
}
```

### Modelo de Histórico Mensual
```json
{
  "PK": "PAREJA#DEFAULT",
  "SK": "HISTORICO#{monthYear}",
  "monthYear": "YYYY-MM",
  "totalSpent": 0.00,
  "budgetAmount": 0.00,
  "byCategory": {
    "subscriptions": 0.00,
    "groceries": 0.00
  },
  "expenseCount": 0,
  "overBudget": false,
  "difference": 0.00,
  "calculatedAt": "ISO 8601"
}
```

### Categorías Disponibles

| ID | Nombre | Emoji | Color | Sugerencias |
|----|--------|-------|-------|------------|
| subscriptions | Suscripciones | 🎬 | #6A88D6 | Netflix, Spotify Duo, Google One, Canva Pro |
| groceries | Supermercado | 🛒 | #4CAF50 | Supermercado semanal, Mercado, Limpieza |
| transport | Transporte | 🚗 | #42A5F5 | Gasolina, Uber, Parqueadero, Peajes |
| dateNights | Citas y salidas | 🍷 | #E57373 | Cena aniversario, Cine, Cafe, Fin de semana |
| home | Casa | 🏠 | #8D6E63 | Arriendo, Servicios, Internet, Mantenimiento |
| health | Salud y bienestar | 🧘 | #26A69A | Farmacia, Consulta médica, Gimnasio, Vitaminas |
| vacations | Vacaciones | ✈️ | #FF8A65 | Reserva hotel, Tiquetes, Tour, Fondo viaje |
| gifts | Regalos | 🎁 | #AB47BC | Cumpleaños, Aniversario, Sorpresa, Flores |
| pets | Mascotas | 🐾 | #8D6E63 | Concentrado, Veterinario, Baño, Juguetes |
| hobbies | Gustos personales | ❤️ | #E91E63 | Videojuego, Libro, Ropa, Curso online |
| savings | Ahorro | 🏦 | #5C6BC0 | Emergencia, Meta carro, Meta apartamento, Boda |
| others | Otros | 📋 | #8D6E63 | Imprevisto, Comisión, Pago pendiente |

---

## 🔌 Endpoints

### 1️⃣ Inicialización

#### Inicializar Pareja (Una sola vez)
```http
POST /finances/init
Content-Type: application/json

{
  "user1Email": "user1@example.com",
  "user2Email": "user2@example.com",
  "user1Name": "Juan",
  "user2Name": "María"
}
```

**Response (201):**
```json
{
  "user1": { "email": "user1@example.com", "name": "Juan" },
  "user2": { "email": "user2@example.com", "name": "María" },
  "createdAt": "2026-07-23T10:00:00+00:00"
}
```

---

### 2️⃣ Datos de la Pareja

#### Obtener Datos de la Pareja
```http
GET /finances
```

**Response (200):**
```json
{
  "PK": "PAREJA#DEFAULT",
  "SK": "META",
  "user1": { "name": "Juan", "email": "user1@example.com" },
  "user2": { "name": "María", "email": "user2@example.com" },
  "monthlyBudget": 0,
  "currency": "$",
  "locale": "es_ES",
  "createdAt": "2026-07-23T10:00:00+00:00",
  "updatedAt": "2026-07-23T10:00:00+00:00"
}
```

**Response (404):**
```json
{
  "error": "Pareja no inicializada. Use POST /finances/init"
}
```

---

### 3️⃣ Resumen General

#### Obtener Resumen de Finanzas
```http
GET /finances/resumen
```

**Response (200):**
```json
{
  "couple": { /* objeto pareja */ },
  "totalSpent": 1234.56,
  "weeklySpent": 120.50,
  "expenseCount": 45,
  "availableMonths": ["2026-07", "2026-06", "2026-05"],
  "byCategory": {
    "groceries": 450.00,
    "transport": 200.00,
    "dateNights": 180.00
  },
  "expenseCategories": { /* todas las categorías disponibles */ }
}
```

---

### 4️⃣ Gastos (Expenses)

#### Listar Gastos
```http
GET /finances/gastos
GET /finances/gastos?month=2026-07
GET /finances/gastos?category=groceries
GET /finances/gastos?month=2026-07&category=groceries
```

**Query Parameters:**
- `month` (optional): Filtrar por YYYY-MM
- `category` (optional): Filtrar por categoría

**Response (200):**
```json
{
  "expenses": [
    {
      "PK": "PAREJA#DEFAULT",
      "SK": "GASTO#uuid",
      "gastoId": "uuid",
      "title": "Spotify Duo",
      "amount": 14.99,
      "date": "2026-07-21T15:30:00+00:00",
      "category": "subscriptions",
      "monthYear": "2026-07",
      "note": "Pago mensual",
      "createdBy": "user1@example.com",
      "createdAt": "2026-07-21T15:30:00+00:00",
      "updatedAt": "2026-07-21T15:30:00+00:00"
    }
  ]
}
```

#### Crear Gasto
```http
POST /finances/gastos
Content-Type: application/json

{
  "title": "Cena aniversario",
  "amount": 45.90,
  "date": "2026-07-20T20:00:00+00:00",
  "category": "dateNights",
  "note": "Restaurante italiano",
  "createdBy": "user1@example.com"
}
```

**Response (201):** Objeto gasto creado

#### Obtener Gasto Específico
```http
GET /finances/gastos/{gastoId}
```

**Response (200):** Objeto gasto

#### Actualizar Gasto
```http
PUT /finances/gastos/{gastoId}
Content-Type: application/json

{
  "title": "Cena aniversario - Actualizado",
  "amount": 50.00,
  "note": "Actualizado con propina"
}
```

**Response (200):** Objeto gasto actualizado

#### Eliminar Gasto
```http
DELETE /finances/gastos/{gastoId}
```

**Response (200):**
```json
{
  "message": "Gasto eliminado exitosamente"
}
```

---

### 5️⃣ Presupuestos Mensuales

#### Establecer/Actualizar Presupuesto
```http
POST /finances/presupuesto/{monthYear}
Content-Type: application/json

{
  "amount": 300.00,
  "notes": "Presupuesto ajustado para vacaciones"
}
```

**Path Parameters:**
- `monthYear`: YYYY-MM (ej: 2026-07)

**Response (200):**
```json
{
  "monthYear": "2026-07",
  "amount": 300.00,
  "updatedAt": "2026-07-23T10:00:00+00:00"
}
```

#### Obtener Presupuesto de un Mes
```http
GET /finances/presupuesto/{monthYear}
```

**Response (200):**
```json
{
  "PK": "PAREJA#DEFAULT",
  "SK": "PRESUPUESTO#2026-07",
  "monthYear": "2026-07",
  "amount": 300.00,
  "notes": "Presupuesto normal",
  "createdAt": "2026-07-01T00:00:00+00:00",
  "updatedAt": "2026-07-23T10:00:00+00:00"
}
```

**Response (404):** Si no existe presupuesto para ese mes

---

### 6️⃣ Históricos

#### Obtener Histórico de un Mes
```http
GET /finances/historico/{monthYear}
```

**Response (200):**
```json
{
  "monthYear": "2026-07",
  "totalSpent": 284.40,
  "budgetAmount": 300.00,
  "byCategory": {
    "subscriptions": 14.99,
    "groceries": 38.50,
    "vacations": 120.00,
    "dateNights": 45.90
  },
  "expenseCount": 4,
  "overBudget": false,
  "difference": 15.60,
  "calculatedAt": "2026-07-23T10:05:00+00:00"
}
```

#### Obtener Histórico de Últimos N Meses
```http
GET /finances/historico
GET /finances/historico?limit=12
```

**Query Parameters:**
- `limit` (optional): Número de meses (default: 12)

**Response (200):**
```json
{
  "history": [
    {
      "monthYear": "2026-07",
      "totalSpent": 284.40,
      "budgetAmount": 300.00,
      "overBudget": false,
      "difference": 15.60,
      "calculatedAt": "2026-07-23T10:05:00+00:00"
    },
    {
      "monthYear": "2026-06",
      "totalSpent": 352.10,
      "budgetAmount": 320.00,
      "overBudget": true,
      "difference": -32.10,
      "calculatedAt": "2026-07-01T10:05:00+00:00"
    }
  ]
}
```

---

## 🔐 Códigos de Estado HTTP

| Código | Significado | Caso de Uso |
|--------|------------|-----------|
| **200** | OK | Operación exitosa (GET, PUT) |
| **201** | Created | Recurso creado exitosamente (POST) |
| **400** | Bad Request | Datos inválidos o campos faltantes |
| **404** | Not Found | Pareja, gasto o presupuesto no encontrado |
| **500** | Internal Server Error | Error del servidor (raro, logging activado) |

---

## ⚠️ Manejo de Errores

Todas las respuestas de error incluyen estructura consistente:

```json
{
  "error": "Descripción del error" | ["Error 1", "Error 2"]
}
```

**Ejemplos:**

### Campos faltantes
```json
{
  "error": [
    "Campo requerido faltante: title",
    "Campo requerido faltante: amount"
  ]
}
```

### Monto inválido
```json
{
  "error": "El monto debe ser mayor a 0"
}
```

### Categoría inválida
```json
{
  "error": "Categoría inválida: miCategoria"
}
```

### Pareja no encontrada
```json
{
  "error": "Pareja no encontrada"
}
```

---

## 📊 Características Especiales

### 1. Cálculo Automático de Estadísticas
- Cada vez que se crea, actualiza o elimina un gasto, se recalculan automáticamente las estadísticas del mes.
- Se actualiza la tabla HISTORICO# con totales, por categoría, conteos, etc.

### 2. Validación Strict de Datos
- ✅ Validación de montos positivos
- ✅ Validación de categorías conocidas
- ✅ Validación de fechas ISO 8601
- ✅ Validación de campos requeridos

### 3. Auditoría Integrada
Cada operación registra:
- **createdAt**: Timestamp de creación (ISO 8601)
- **updatedAt**: Timestamp de última actualización
- **createdBy**: Email del usuario que realizó la acción
- **modifiedBy**: Email del usuario que realizó la última modificación (futuro)

### 4. Filtrado Avanzado
```http
GET /finances/{parejaId}/gastos?month=2026-07&category=dateNights
```
Retorna solo gastos de "Citas y salidas" en julio 2026.

### 5. Alertas por Presupuesto Excedido
Cuando se agrega/actualiza un gasto que causa que se exceda el presupuesto mensual:
- El frontend recibe notificación en el histórico
- El campo `overBudget` se establece a `true`
- El `difference` es negativo

---

## 🗄️ Estructura de Tabla DynamoDB

**Nombre:** `FinancesTable`  
**Modo de Facturación:** `PAY_PER_REQUEST` (sin costo si no se usa)

### Clave Primaria
- **PK (HASH):** `PAREJA#DEFAULT` (fija, siempre igual)
- **SK (RANGE):** `META|GASTO#{id}|PRESUPUESTO#{monthYear}|HISTORICO#{monthYear}`

### Índices Globales Secundarios (GSI)

1. **monthYear-index**
   - Permite búsquedas eficientes por mes
   - Usado para listar meses disponibles

2. **category-index**
   - Permite búsquedas por categoría
   - Usado para filtrado por categoría

---

## 💡 Ejemplo Completo de Uso

### Flujo: Crear pareja y agregar primer gasto

```bash
# 1. Crear pareja
curl -X POST https://api.example.com/finances \
  -H "Content-Type: application/json" \
  -d '{
    "user1Email": "juan@example.com",
    "user2Email": "maria@example.com",
    "user1Name": "Juan",
    "user2Name": "María"
  }'

# Response:
{
  "parejaId": "abc-123",
  "user1": { "email": "juan@example.com", "name": "Juan" },
  "user2": { "email": "maria@example.com", "name": "María" },
  "createdAt": "2026-07-23T10:00:00+00:00"
}

# 2. Establecer presupuesto para este mes
curl -X POST https://api.example.com/finances/abc-123/presupuesto/2026-07 \
  -H "Content-Type: application/json" \
  -d '{ "amount": 300 }'

# 3. Agregar primer gasto
curl -X POST https://api.example.com/finances/abc-123/gastos \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Spotify Duo",
    "amount": 14.99,
    "date": "2026-07-23T10:00:00+00:00",
    "category": "subscriptions",
    "createdBy": "juan@example.com"
  }'

# 4. Obtener resumen
curl -X GET https://api.example.com/finances/abc-123/resumen

# Response:
{
  "parejaId": "abc-123",
  "couple": { /* datos de pareja */ },
  "totalSpent": 14.99,
  "weeklySpent": 14.99,
  "expenseCount": 1,
  "availableMonths": ["2026-07"],
  "byCategory": { "subscriptions": 14.99 }
}

# 5. Obtener histórico del mes
curl -X GET https://api.example.com/finances/abc-123/historico/2026-07

# Response:
{
  "monthYear": "2026-07",
  "totalSpent": 14.99,
  "budgetAmount": 300.00,
  "byCategory": { "subscriptions": 14.99 },
  "expenseCount": 1,
  "overBudget": false,
  "difference": 285.01,
  "calculatedAt": "2026-07-23T10:05:00+00:00"
}
```

---

## 🔄 Diferencias Respecto a Maqueta Flutter

La maqueta Flutter en el front fue mockup (datos estáticos). Este backend:

✅ **Agrega:**
- Persistencia en DynamoDB
- Multi-pareja con aislamiento de datos
- Autenticación preparada (email-based)
- Históricos calculados automáticamente
- Auditoría completa
- Validación strict
- Escalabilidad serverless

---

## 🚀 Próximas Mejoras Potenciales

1. **Autenticación**: Integrar Cognito para validar tokens JWT
2. **Análisis Avanzado**: Tendencias, predicciones, recomendaciones
3. **Categorías Personalizadas**: Permitir parejas crear sus propias categorías
4. **Compartición de Gastos**: Marcar quién pagó y quién debe (para dividir)
5. **Notificaciones**: SNS/SES para alertas de presupuesto
6. **Exportación**: Generar reportes en PDF/Excel
7. **Multi-divisas**: Soportar diferentes monedas
8. **Metas de Ahorro**: Goals y tracking hacia objetivos
9. **Transacciones Recurrentes**: Gastos automáticos mensuales
10. **Integración Bancaria**: Importar transacciones de bancos reales

---

## 📝 Logging

Todos los eventos usan semáforos para mejor legibilidad:

- ⚪️ **INFO NOT RELEVANT**: Eventos de rutina (querystrings, paths)
- 🟢 **INFO RELEVANT**: Operaciones exitosas (create, update, delete)
- 🔵 **INFO VERY RELEVANT**: Cálculos importantes, validaciones
- 🟡 **WARNING**: Intentos inválidos, datos edge-case
- 🔴 **ERROR**: Fallos de operación, errores del sistema
- 🟤 **DEBUG**: Información detallada para troubleshooting

---

## ✅ Conclusión

API completa, escalable y production-ready para finanzas de parejas. Todas las operaciones están validadas, auditadas y optimizadas para la base de datos serverless de AWS DynamoDB.
