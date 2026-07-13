# 📊 Diagramas de Flujo, Secuencia y Arquitectura

## 🎯 Descripción General

Se han creado tres diagramas Mermaid que visualizan cómo funcionan los dos CRUDs (Challenges y Dice):

1. **Diagrama de Flujo** — Muestra el flujo de decisiones y procesos
2. **Diagrama de Secuencia** — Muestra la interacción paso a paso entre componentes
3. **Diagrama de Arquitectura** — Muestra la estructura general del sistema

---

## 1️⃣ Diagrama de Flujo (`diagrama_flujo_crud.mmd`)

### 📋 Descripción

Visualiza el flujo completo de las operaciones CRUD:

```
Cliente → API Gateway → Dispatcher → GET/POST/PUT/DELETE → DynamoDB → Response
```

### 🔄 Flujos Incluidos

#### **GET Endpoints**
- `GET /challenges` — Scan con filtro opcional por level
- `GET /challenges/random` — Scan + Pick aleatorio
- `GET /challenges/ID` — Query directo por ID
- Similar para `/dice`

#### **POST Endpoint**
- Detecta si es array o single item
- Si array → Bulk create con batch_writer
- Si single → Validar, normalizar, crear
- Manejo de errores por item

#### **PUT Endpoint**
- Validar que existe (ConditionExpression)
- Validar campos actualizables
- Agregar timestamp updatedAt
- UpdateExpression solo con campos enviados

#### **DELETE Endpoint**
- Verificar existencia
- Eliminar si existe
- Return 404 si no existe

### 🎨 Colores

- 🟠 **Naranja** — API Gateway
- 🟢 **Verde** — Response exitosa
- 🔴 **Rojo** — Errores (400, 404, 405, 502)
- 🔵 **Azul** — Cliente (inicio/fin)

---

## 2️⃣ Diagrama de Secuencia (`diagrama_secuencia_crud.mmd`)

### 📋 Descripción

Muestra la interacción temporal entre:
- 👤 Cliente (Frontend)
- 🌐 API Gateway
- ⚡ Lambda Function
- 📦 DynamoDB
- 📝 CloudWatch Logs

### 🔄 Casos de Uso Cubiertos

#### **Caso 1: GET /challenges?level=suave**
```
1. Cliente solicita todos los challenges suave
2. Lambda inicia GET_ALL
3. Lambda hace Scan con FilterExpression
4. DynamoDB retorna items
5. Lambda retorna 200 JSON
```

#### **Caso 2: GET /challenges/random?level=picante**
```
1. Cliente solicita challenge aleatorio (picante)
2. Lambda inicia GET_RANDOM
3. Lambda hace Scan con filtro
4. Lambda elige uno aleatorio
5. Lambda retorna 200 JSON
```

#### **Caso 3: POST /challenges (single)**
```
1. Cliente envía nuevo challenge
2. Lambda valida campos requeridos
3. Lambda genera UUID + timestamps
4. Lambda PutItem en DynamoDB
5. Lambda retorna 201 Created + id
```

#### **Caso 4: POST /dice (bulk)**
```
1. Cliente envía array de items
2. Lambda detecta array
3. Para cada item:
   - Validar
   - Normalizar
   - batch.put_item (queued)
4. Batch writer auto-commits
5. Lambda retorna 201 + created list + errors list
```

#### **Caso 5: PUT /challenges/ID**
```
1. Cliente solicita actualizar
2. Lambda construye UpdateExpression
3. Lambda UpdateItem con ConditionExpression
4. DynamoDB actualiza si existe
5. Lambda retorna 200 OK o 404
```

#### **Caso 6: DELETE /challenges/ID**
```
1. Cliente solicita eliminar
2. Lambda verifica existencia
3. Lambda DeleteItem si existe
4. Lambda retorna 200 OK o 404
```

#### **Caso 7: Error Handling**
```
1. Cliente solicita crear
2. Validación exitosa
3. Lambda PutItem
4. DynamoDB retorna error conexión
5. Lambda retorna 502 Bad Gateway
```

### 🎨 Colores (Rect)

- 🔵 Azul claro — GET operations
- 🟣 Púrpura — GET /random
- 🟢 Verde — POST single item
- 🟡 Amarillo claro — POST bulk
- 🔴 Rosa — PUT update
- 🔴 Rojo claro — DELETE
- 🔴 Rojo intenso — Error handling

### 📝 Logs con Semáforos

- 🟢 `INFO RELEVANT` — Operaciones exitosas
- 🟡 `WARNING` — Validaciones fallidas
- 🔴 `ERROR` — Errores de DB
- 🟤 `DEBUG` — Detalles internos

---

## 3️⃣ Diagrama de Arquitectura (`diagrama_arquitectura_crud.mmd`)

### 📋 Descripción

Muestra cómo los componentes AWS están conectados:

```
Internet
    ↓
API Gateway (CORS Handler)
    ↓
Lambda Functions (ChallengesCRUD + DiceCRUD)
    ↓
DynamoDB Tables (+ GSI, Encryption, PITR)
```

### 🏗️ Capas

#### **Capa Internet**
- `Frontend Client` — App Flutter o Web que consume API

#### **Capa API Gateway**
- `API Endpoint /challenges` — Enruta solicitudes a ChallengesCRUD Lambda
- `API Endpoint /dice` — Enruta solicitudes a DiceCRUD Lambda
- `CORS Handler` — Agrega headers CORS a todas las respuestas

#### **Capa Lambda (Compute)**
- `ChallengesCRUD Lambda` — Procesa 6 operaciones CRUD
- `DiceCRUD Lambda` — Procesa 6 operaciones CRUD
- `Common Layer` — Código compartido (utils.py, validaciones, respuestas)

#### **Capa DynamoDB (Data)**
- `ChallengesTable` — Tabla con GSI en `level`
  - Partición: id
  - GSI: level (para filtrar eficientemente)
  
- `DiceTable` — Tabla con GSI en `level` y `diceType`
  - Partición: id
  - GSI 1: level (para filtrar por nivel)
  - GSI 2: diceType (para filtrar por tipo de dado)

- `SSE Encryption` — Encriptación en reposo (AES-256)
- `Point-in-Time Recovery` — Backups automáticos

### 🔗 Conexiones

```
Frontend Client 
    ↓ HTTP Requests
CORS Handler (Agrega headers CORS)
    ↓ Enruta por path
    ├─ /challenges → ChallengesCRUD Lambda
    └─ /dice → DiceCRUD Lambda
        ↓ Usan código compartido
    Common Layer (utils.py)
        ↓ Queries/Mutations
        ├─ ChallengesTable (level-index)
        └─ DiceTable (level-index, diceType-index)
            ↓ Con protecciones
        SSE Encryption (en reposo)
        Point-in-Time Recovery (backups)
```

### 📊 Características de Seguridad

✅ **DynamoDB:**
- Encriptación SSE (Server-Side Encryption)
- Point-in-Time Recovery (PITR) habilitado
- Índices secundarios (GSI) para queries eficientes
- Billing mode: PAY_PER_REQUEST (sin provisionamiento)

✅ **API Gateway:**
- CORS configurado
- Enrutamiento automático
- Rate limiting (opcional)
- CloudWatch Logs

✅ **Lambda:**
- IAM Policies restrictivas (solo acceso a sus tablas)
- Tracing activo (X-Ray)
- Timeouts: 29 segundos
- Memory: 128 MB

---

## 📚 Cómo Usar los Diagramas

### **Para Entender el Flujo General**
→ Ver `diagrama_arquitectura_crud.mmd` primero

### **Para Ver Cómo Opera un Endpoint Específico**
→ Ver `diagrama_secuencia_crud.mmd`

### **Para Entender Lógica de Decisiones**
→ Ver `diagrama_flujo_crud.mmd`

---

## 🔍 Ejemplos de Lectura

### Escenario 1: "¿Cómo se crea un Challenge?"

1. **Arquitectura** → Cliente → API Gateway → ChallengesCRUD Lambda → ChallengesTable
2. **Secuencia** → Caso 3: POST /challenges (single)
   - Validar campos
   - Generar UUID
   - PutItem en DB
   - Retornar 201
3. **Flujo** → POST → CheckPost → ValidateCRUD → GenUUID → CreateItem → Response

### Escenario 2: "¿Qué pasa si hago GET /dice/random?diceType=acciones&level=picante?"

1. **Arquitectura** → DiceCRUD Lambda → DiceTable con 2 filtros
2. **Secuencia** → Caso 2: GET /challenges/random (similar para dice)
   - Scan con 2 FilterExpressions
   - Pick aleatorio
   - Retornar 200
3. **Flujo** → GET → Dispatch → GetRnd2 → Response

### Escenario 3: "¿Qué pasa si falla la conexión a DynamoDB?"

1. **Secuencia** → Caso 7: Error handling
   - Lambda recibe error de DDB
   - Log 🔴 ERROR
   - Retorna 502
2. **Flujo** → Error502 → Response (con error)

---

## 🎯 Datos Recopilados en Diagramas

### Desafíos (Challenges)
- Operaciones: GET (todos, random, uno), POST, PUT, DELETE
- Filtros: level (suave, picante, atrevido)
- Índices: level-index (GSI)

### Dados (Dice)
- Operaciones: GET (todos, random, uno), POST, PUT, DELETE
- Filtros: level + diceType (2 dimensiones)
- Índices: level-index (GSI), diceType-index (GSI)

---

## 📞 Notas Adicionales

- Los diagramas son **dinámicos** — Se pueden editar si cambia la arquitectura
- Incluyen **casos de éxito y error** — Ver todas las ramificaciones
- Usan **semáforos fijos** — Siguiendo convención del proyecto
- Compatible con **mermaid.js versión 11+** — Para renderizado en GitHub, GitLab, etc.

---

**Creado:** 2026-07-12  
**Formatos:** `.mmd` (Mermaid) — Abiertos con VS Code + Mermaid extension  
**Estados:** ✅ Validados y Previsualizados
