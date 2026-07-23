# 🚀 Guía de Despliegue: CRUD Finanzas de Parejas

## 📁 Archivos Creados/Modificados

### Backend (Lambda)
```
lambdas/
├── functions/
│   └── FinancesCRUD/
│       ├── handler.py              ✅ 1000+ líneas - toda la lógica
│       └── requirements.txt         ✅ Vacío (usa common layer)
├── template.yaml                    ✅ Actualizado con:
│   ├── Lambda: FinancesFunction
│   ├── Tabla: FinancesTable (DynamoDB)
│   ├── Variable: FINANCES_TABLE_NAME
│   └── Outputs: FinancesEndpoint
└── layers/
    └── baselayer/
        └── python/
            └── common/
                └── utils.py         (compartido con otros CRUD)
```

### Documentación
```
├── FINANCES_CRUD_DOCUMENTATION.md    ✅ API completa con ejemplos
├── FINANCES_FRONTEND_INTEGRATION.md  ✅ Guía para conectar Flutter
└── DEPLOYMENT_GUIDE.md               📄 Este archivo
```

---

## 🛠️ Requisitos Previos

- AWS CLI configurado con credenciales
- SAM CLI instalado (`pip install aws-sam-cli`)
- Python 3.14+ local
- Docker (para SAM local testing)

---

## 📦 Estructura de la API

| Recurso | Método | Endpoint |
|---------|--------|----------|
| **Parejas** | POST | `/finances` |
| | GET | `/finances/{parejaId}` |
| **Gastos** | POST | `/finances/{parejaId}/gastos` |
| | GET | `/finances/{parejaId}/gastos` |
| | GET | `/finances/{parejaId}/gastos/{gastoId}` |
| | PUT | `/finances/{parejaId}/gastos/{gastoId}` |
| | DELETE | `/finances/{parejaId}/gastos/{gastoId}` |
| **Presupuestos** | POST | `/finances/{parejaId}/presupuesto/{monthYear}` |
| | GET | `/finances/{parejaId}/presupuesto/{monthYear}` |
| **Históricos** | GET | `/finances/{parejaId}/historico` |
| | GET | `/finances/{parejaId}/historico/{monthYear}` |
| **Resumen** | GET | `/finances/{parejaId}/resumen` |

---

## 🚀 Pasos de Despliegue

### 1. Preparar Código

```bash
cd lambdas/
```

### 2. Build SAM

```bash
sam build
```

**Esperado:**
```
Building resources...
Building the package (python3.14)
Successfully packaged code under ./build directory
```

### 3. Deploy Guiado (Primera vez)

```bash
sam deploy --guided
```

**Responder a las preguntas:**
```
Stack Name [sam-app]: citas-app-finances
Region [us-east-1]: us-east-1 (o tu región)
Confirm changes before deploy [y/N]: y
Allow SAM CLI IAM role creation [Y/n]: Y
Save parameters to samconfig.toml [Y/n]: Y
```

### 4. Deploy Directo (Siguientes veces)

```bash
sam deploy
```

### 5. Obtener URL de API

```bash
aws cloudformation describe-stacks \
  --stack-name citas-app-finances \
  --query 'Stacks[0].Outputs[?OutputKey==`FinancesEndpoint`].OutputValue' \
  --output text
```

**Output:**
```
https://abc123xyz.execute-api.us-east-1.amazonaws.com/finances
```

---

## 🧪 Testear Localmente

### Iniciar API Local

```bash
sam local start-api
```

**Esperado:**
```
Mounting FinancesCRUD at http://127.0.0.1:3000/finances [POST, GET, PUT, DELETE...]
```

### Test Local: Crear Pareja

```bash
curl -X POST http://localhost:3000/finances \
  -H "Content-Type: application/json" \
  -d '{
    "user1Email": "juan@example.com",
    "user2Email": "maria@example.com",
    "user1Name": "Juan",
    "user2Name": "María"
  }'
```

**Response (201):**
```json
{
  "parejaId": "abc-123-xyz",
  "user1": {"email": "juan@example.com", "name": "Juan"},
  "user2": {"email": "maria@example.com", "name": "María"},
  "createdAt": "2026-07-23T10:00:00+00:00"
}
```

### Test Local: Crear Gasto

Usar el `parejaId` de arriba:

```bash
curl -X POST http://localhost:3000/finances/abc-123-xyz/gastos \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Spotify Duo",
    "amount": 14.99,
    "date": "2026-07-23T10:00:00+00:00",
    "category": "subscriptions",
    "createdBy": "juan@example.com"
  }'
```

### Test Local: Listar Gastos

```bash
curl -X GET http://localhost:3000/finances/abc-123-xyz/gastos
```

### Test Local: Obtener Resumen

```bash
curl -X GET http://localhost:3000/finances/abc-123-xyz/resumen
```

---

## 🔍 Monitoreo y Debugging

### Ver Logs de CloudWatch

```bash
sam logs -n FinancesFunction --stack-name citas-app-finances
```

### Ver Logs en Tiempo Real

```bash
sam logs -n FinancesFunction --stack-name citas-app-finances --tail
```

### Obtener Métricas de Lambda

```bash
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Duration \
  --dimensions Name=FunctionName,Value=finances-crud \
  --start-time 2026-07-23T00:00:00Z \
  --end-time 2026-07-24T00:00:00Z \
  --period 3600 \
  --statistics Average,Maximum
```

### Ver Tabla DynamoDB

```bash
aws dynamodb scan \
  --table-name FinancesTable \
  --limit 10
```

---

## 💰 Costos Estimados

**Modelo de facturación: PAY_PER_REQUEST (sin contrato)**

| Componente | Precio | Notas |
|-----------|--------|-------|
| DynamoDB | $1.25 por millón de WCUs | Muy bajo si no se usa |
| Lambda | $0.20 por millón de invocaciones | First 1M free tier |
| Data Transfer | $0.09 por GB | Entre AWS es gratis |
| **Total** | ~$0-5/mes | Dependiendo del uso |

**En free tier (primer año):**
- Lambda: 1M invocaciones gratis
- DynamoDB: 25 GB almacenamiento gratis
- Data Transfer: 1 GB/mes gratis

---

## 🔐 Seguridad (Mejoras Futuras)

### Autenticación
```python
# Agregar validación de JWT
import jwt

def verify_token(event):
    token = event['headers'].get('Authorization', '').replace('Bearer ', '')
    try:
        payload = jwt.decode(token, 'SECRET_KEY', algorithms=['HS256'])
        return payload['user_id']
    except:
        return None
```

### CORS
```yaml
# En template.yaml
CorsConfiguration:
  AllowOrigins:
    - "https://yourdomain.com"  # Especificar dominio
  AllowMethods:
    - GET
    - POST
    - PUT
    - DELETE
  AllowHeaders:
    - Authorization
    - Content-Type
```

### Rate Limiting
```python
# Usar AWS API Gateway throttling
# Settings → Throttle → Rate limit: 10000 req/s
# Burst limit: 5000 concurrent
```

---

## 🧹 Limpiar Recursos (si es necesario)

### Eliminar Stack

```bash
aws cloudformation delete-stack --stack-name citas-app-finances
```

### Confirmar eliminación

```bash
aws cloudformation describe-stacks --stack-name citas-app-finances
# Debe retornar: Stack with id citas-app-finances does not exist
```

### Eliminar tabla DynamoDB

```bash
aws dynamodb delete-table --table-name FinancesTable
```

⚠️ **Advertencia**: Esto eliminará todos los datos de finanzas. El template tiene `DeletionPolicy: Retain` para proteger esto, pero revisa antes de borrar.

---

## 📊 Monitoreo con CloudWatch Dashboard

### Crear Dashboard

```bash
aws cloudwatch put-dashboard \
  --dashboard-name FinancesCRUD \
  --dashboard-body file://dashboard.json
```

### Archivo `dashboard.json`

```json
{
  "widgets": [
    {
      "type": "metric",
      "properties": {
        "metrics": [
          ["AWS/Lambda", "Duration", {"stat": "Average"}],
          ["AWS/Lambda", "Errors", {"stat": "Sum"}],
          ["AWS/DynamoDB", "ConsumedWriteCapacityUnits"],
          ["AWS/DynamoDB", "ConsumedReadCapacityUnits"]
        ],
        "period": 300,
        "stat": "Average",
        "region": "us-east-1",
        "title": "Finances CRUD Metrics"
      }
    }
  ]
}
```

---

## 🚨 Troubleshooting

### Error: "User is not authorized to perform: dynamodb:PutItem"

**Causa**: Lambda no tiene permisos IAM

**Solución**:
```yaml
# En template.yaml, agregar policy explícita
Policies:
  - DynamoDBCrudPolicy:
      TableName: !Ref FinancesTable
```

### Error: "Lambda response is invalid"

**Causa**: Handler no retorna estructura correcta

**Solución**: Verificar que `build_response()` esté siendo usado

```python
return build_response(200, {"mensaje": "OK"})
```

### Error: "Connection timeout"

**Causa**: Lambda tarda más de 29 segundos

**Solución**: Optimizar queries, aumentar MemorySize en template.yaml

```yaml
MemorySize: 256  # Aumentar de 128 a 256
Timeout: 60      # Aumentar timeout
```

### Error: "ValidationException" en DynamoDB

**Causa**: Atributos faltantes o tipos incorrectos

**Solución**: Verificar que PK y SK sean strings (S) en AttributeDefinitions

---

## ✅ Checklist Post-Deploy

- [ ] Crear pareja exitosamente (POST /finances)
- [ ] Obtener pareja (GET /finances/{parejaId})
- [ ] Crear gasto (POST /finances/{parejaId}/gastos)
- [ ] Listar gastos (GET /finances/{parejaId}/gastos)
- [ ] Actualizar gasto (PUT /finances/{parejaId}/gastos/{gastoId})
- [ ] Eliminar gasto (DELETE /finances/{parejaId}/gastos/{gastoId})
- [ ] Establecer presupuesto (POST /finances/{parejaId}/presupuesto/2026-07)
- [ ] Obtener histórico (GET /finances/{parejaId}/historico)
- [ ] Obtener resumen (GET /finances/{parejaId}/resumen)
- [ ] Verificar logs en CloudWatch (sin errores 🔴)
- [ ] Conectar frontend Flutter

---

## 📞 Soporte

Para issues:
1. Revisar logs: `sam logs -n FinancesFunction --tail`
2. Verificar tabla existe: `aws dynamodb describe-table --table-name FinancesTable`
3. Testear localmente con `sam local start-api`
4. Revisar IAM permissions en AWS Console

---

## 🎉 ¡Listo!

Tu CRUD de finanzas está en producción. El frontend Flutter puede conectarse usando el `FinancesEndpoint` que obtuviste.

**Próximo paso**: Implementar autenticación Cognito para asociar parejas reales.
