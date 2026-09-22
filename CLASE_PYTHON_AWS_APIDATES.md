# Clase: Python, AWS y arquitectura de APIDates

## Propósito

Esta clase explica cómo funciona el proyecto APIDates usando únicamente su código, su plantilla AWS SAM, sus scripts y su documentación. El objetivo no es aprender Python o AWS de forma aislada, sino entender cómo ambas tecnologías se combinan para construir una API REST serverless para parejas.

Al terminar, el estudiante podrá:

- Leer un `handler.py` de AWS Lambda.
- Seguir una petición desde API Gateway hasta DynamoDB, S3, Cognito o Spotify.
- Entender los patrones Python que se repiten en el proyecto.
- Explicar la diferencia entre una tabla DynamoDB de clave simple y una de clave compuesta.
- Identificar autenticación, autorización, variables de entorno, capas y permisos IAM.
- Construir y desplegar el proyecto con AWS SAM.
- Detectar diferencias entre documentación histórica y configuración actual.

## 1. Qué problema resuelve APIDates

APIDates es un backend REST serverless para administrar experiencias y actividades de pareja. El proyecto reúne varios dominios:

| Dominio | Función o funciones | Persistencia o servicio |
|---|---|---|
| Autenticación | `AuthCRUD` | Amazon Cognito |
| Planes y citas | `DatesCRUD`, `CitasCRUD` | DynamoDB |
| Retos y dados | `ChallengesCRUD`, `DiceCRUD` | DynamoDB |
| Finanzas | `FinancesCRUD` | DynamoDB |
| Bodas | `BodasCRUD` | DynamoDB |
| Checklists | `ChecklistsCRUD` | DynamoDB |
| Frases y entretenimiento | `PhrasesCRUD`, `KamasutraCRUD`, `RandomDates` | DynamoDB o lógica de Lambda |
| Imágenes y audio | `ImagesManager` | Amazon S3 y URLs prefirmadas |
| Spotify | `SpotifyAPI` | API externa de Spotify y DynamoDB para tokens |

El flujo general es:

```text
Cliente web o móvil
        |
        v
API Gateway HTTP API
        |
        v
Lambda Python: handler.lambda_handler
        |
        +--> Cognito
        +--> DynamoDB
        +--> S3
        +--> Spotify
        |
        v
Respuesta HTTP JSON
```

## 2. Cómo leer el proyecto

La carpeta raíz contiene documentación, diagramas y configuración de análisis. La carpeta `lambdas/` contiene la infraestructura y el código ejecutable.

```text
APIDates/
├── README.md
├── CLASE_PYTHON_AWS_APIDATES.md
├── DEPLOYMENT_GUIDE.md
├── *_CRUD_DOCUMENTATION.md
├── *.mmd                         # Diagramas Mermaid
└── lambdas/
    ├── template.yaml             # Infraestructura AWS SAM
    ├── deploy.ps1                # Despliegue PowerShell
    ├── functions/
    │   └── <Dominio>/handler.py  # Código de cada Lambda
    └── layers/
        └── baselayer/python/common/
            ├── __init__.py
            └── utils.py          # Código Python compartido
```

Regla de lectura práctica:

1. Empezar por la ruta HTTP en `template.yaml`.
2. Localizar el `CodeUri` de esa función.
3. Entrar en `handler.py` y buscar `lambda_handler`.
4. Seguir las funciones de validación, persistencia y respuesta.
5. Revisar `utils.py` cuando aparezca una función importada desde `common`.

## 3. Python dentro de una Lambda

### 3.1 El handler

Cada Lambda expone una función parecida a esta idea:

```python
def lambda_handler(event, context):
    method = (event.get("requestContext", {})
              .get("http", {})
              .get("method") or "").upper()
    path = event.get("rawPath", "")

    # Validar, ejecutar la operación y responder
```

`event` es un diccionario recibido desde API Gateway. En este proyecto se usan principalmente:

- `event["requestContext"]`: método HTTP y datos del autorizador.
- `event["rawPath"]`: ruta recibida.
- `event["pathParameters"]`: valores como un `id` de la URL.
- `event["queryStringParameters"]`: filtros y opciones.
- `event["body"]`: cuerpo de una petición `POST`, `PUT` o `PATCH`.

`context` es el segundo parámetro estándar de Lambda. Los handlers del proyecto no dependen normalmente de él, pero AWS lo entrega para información de ejecución.

### 3.2 Diccionarios y acceso seguro

El código usa `dict.get()` para evitar errores cuando una clave no existe:

```python
path_parameters = event.get("pathParameters") or {}
item_id = (path_parameters.get("id") or "").strip()
```

La expresión `or {}` transforma un valor ausente en un diccionario vacío. Después, `or ""` permite tratar un parámetro inexistente como texto vacío y `strip()` elimina espacios innecesarios.

### 3.3 Validación y excepciones

Los handlers separan errores de entrada de errores inesperados. En los patrones del proyecto aparecen:

- `ValueError` para parámetros o cuerpos inválidos.
- `PermissionError` cuando falta autenticación.
- `HTTPError` y `URLError` al comunicarse con servicios externos.
- `try/except` para convertir esos problemas en respuestas HTTP comprensibles.

La validación evita que datos incompletos lleguen a DynamoDB o a una API externa. Ejemplos del proyecto incluyen campos obligatorios, valores permitidos como niveles de reto, límites numéricos y tipos de Spotify.

### 3.4 Type hints y valores opcionales

El proyecto usa anotaciones modernas de Python:

```python
def _spotify_user_request(
    method: str,
    path: str,
    user_id: str,
    params: dict | None = None,
    body: dict | None = None,
) -> dict:
    ...
```

Esto documenta que `method`, `path` y `user_id` son textos, que `params` y `body` pueden no existir, y que la función devuelve un diccionario.

También se usa `Any` en integraciones como el recurso global de DynamoDB, cuando el tipo concreto proviene de `boto3` y no se conoce con precisión durante el análisis estático.

### 3.5 Comprensiones, funciones y transformación de datos

En Spotify se transforman respuestas grandes de la API externa a objetos pequeños para el frontend:

```python
artists = [
    artist.get("name")
    for artist in track.get("artists", [])
    if artist.get("name")
]
```

La comprensión recorre una lista, extrae solo el nombre y descarta entradas sin nombre. Funciones como `_map_track`, `_map_artist`, `_map_album` y `_map_playlist` son una frontera importante: aíslan el formato externo de Spotify del formato que expone APIDates.

## 4. La capa común de Python

`lambdas/layers/baselayer/python/common/utils.py` se publica como `CommonLayer` y se agrega a las funciones desde `Globals.Function.Layers` en `template.yaml`.

Sus responsabilidades principales son:

### Respuestas HTTP

`build_response(status_code, body)` crea un diccionario con:

- `statusCode`.
- Headers de contenido JSON y CORS.
- `body` serializado como texto JSON.

Esto evita que cada Lambda repita la misma estructura de salida.

### Serialización de DynamoDB

DynamoDB puede devolver valores `Decimal`. `DecimalEncoder` los convierte a `int` cuando no tienen decimales o a `float` cuando sí los tienen, para que `json.dumps` pueda serializarlos.

### Lectura del body

`parse_body(event)` acepta un body ya convertido en diccionario o lista y también un body textual JSON. Si no se puede analizar, lanza `ValueError`.

### Paginación

`scan_all` y `query_all` repiten llamadas mientras DynamoDB entregue `LastEvaluatedKey`. Esto es necesario porque una sola operación puede devolver solo una página de resultados.

### Helpers CRUD

Funciones como `generic_get_item`, `generic_delete_item` y `generic_update_item` reúnen operaciones repetidas. `build_update_expression` construye de forma dinámica una expresión `SET`, excluyendo campos que no deben modificarse como `id` y `createdAt`.

## 5. Cómo entra una petición a AWS

En `template.yaml`, un recurso `AWS::Serverless::Function` conecta tres cosas:

```yaml
DatesFunction:
  Type: AWS::Serverless::Function
  Properties:
    CodeUri: functions/DatesCRUD/
    Handler: handler.lambda_handler
    Events:
      GetDates:
        Type: HttpApi
        Properties:
          ApiId: !Ref CitasApi
          Method: GET
          Path: /planes
```

La lectura conceptual es:

1. API Gateway recibe `GET /planes`.
2. SAM lo asocia con `DatesFunction`.
3. AWS ejecuta `handler.lambda_handler` dentro de `functions/DatesCRUD/`.
4. El handler lee el evento.
5. El handler consulta o modifica DynamoDB.
6. `build_response` devuelve JSON y el código HTTP.

Una Lambda es serverless porque el proyecto no administra servidores permanentes. AWS ejecuta la función cuando llega una petición y administra la infraestructura de ejecución.

## 6. API Gateway, rutas y CORS

`CitasApi` es un `AWS::Serverless::HttpApi`. El template declara métodos `GET`, `POST`, `PUT`, `DELETE`, `PATCH` y `OPTIONS`.

CORS permite que el frontend haga peticiones desde otro origen. En la configuración actual se permite `*`; el propio template indica que en producción conviene limitar los orígenes al dominio real.

Los handlers distinguen rutas y métodos mediante `event`. Por ejemplo, `SpotifyAPI` primero comprueba el método y después la ruta para derivar la operación correcta.

Una ruta bien diseñada valida siempre:

- Método esperado.
- Parámetros obligatorios.
- Formato de los datos.
- Autenticación cuando corresponde.
- Resultado de la operación externa o de base de datos.

## 7. Autenticación y autorización

La configuración actual usa Amazon Cognito y un JWT authorizer de API Gateway. El authorizer es el predeterminado para la API, por lo que las rutas quedan protegidas salvo que una ruta indique explícitamente `Authorizer: NONE`.

### Cognito

El template define:

- Un User Pool.
- Email como atributo de usuario.
- Verificación por email.
- Reglas de contraseña.
- Un App Client sin secreto para el frontend.
- Flujos de contraseña, SRP y refresh token.
- Hosted UI con URLs de callback y logout.

`AuthCRUD/handler.py` llama a Cognito para registro, confirmación, login, refresh, recuperación de contraseña y logout. Las contraseñas no se guardan en DynamoDB.

### Cómo obtiene el usuario una Lambda

En las rutas protegidas, el `sub` del JWT aparece en:

```python
claims = (
    event.get("requestContext", {})
    .get("authorizer", {})
    .get("jwt", {})
    .get("claims", {})
)
user_id = (claims.get("sub") or "").strip()
```

La función `_require_user_id` centraliza esta comprobación y lanza `PermissionError` si el usuario no está autenticado.

### Nota sobre la documentación

El README contiene una referencia antigua a “API Keys por implementar”, pero el `template.yaml` actual define Cognito JWT y `AuthCRUD` usa Cognito. Para estudiar el comportamiento actual, la fuente principal es `template.yaml` junto con los handlers.

## 8. DynamoDB: cómo se modelan los datos

DynamoDB es una base de datos NoSQL. En este proyecto los modelos se diseñan según las consultas que necesita cada dominio.

### Clave simple

Recursos como retos, dados, Kamasutra y frases pueden usar una clave `id`:

```text
id = UUID del elemento
```

La operación habitual es `get_item(Key={"id": item_id})`.

### Clave compuesta

Finanzas, bodas y checklists usan agregados con partición y ordenación:

```text
PK = PAREJA#DEFAULT
SK = GASTO#<gastoId>
```

O:

```text
PK = BODA#<bodaId>
SK = META
```

La clave permite agrupar entidades relacionadas y después consultarlas por su partición. Los prefijos de `PK` y `SK` hacen explícito el tipo de relación.

### Índices secundarios

El template define GSI para consultas alternativas, por ejemplo por nivel, tipo, categoría, mes o tipo de entidad. Un índice evita tener que recorrer toda la tabla cuando la consulta no empieza por la clave primaria principal.

### Operaciones y seguridad de datos

Las tablas usan `PAY_PER_REQUEST`, y las tablas principales tienen cifrado del lado del servidor y, en varios casos, Point-in-Time Recovery. Algunas tienen `DeletionPolicy: Retain`, para evitar que una eliminación del stack destruya automáticamente datos importantes.

En las actualizaciones se usan `ConditionExpression` o atributos de condición para distinguir “actualizado” de “elemento inexistente”.

## 9. S3 y URLs prefirmadas

`ImagesManager` genera URLs prefirmadas para que el cliente pueda subir imágenes o audio directamente a S3 durante un tiempo limitado.

El flujo es:

1. El cliente solicita `/images/upload-url` o `/audio/upload-url`.
2. Lambda valida la petición y usa el nombre del bucket recibido por variable de entorno.
3. S3 devuelve una URL prefirmada.
4. El cliente usa esa URL para cargar el archivo sin enviar el contenido a través de Lambda.
5. El objeto queda almacenado en el bucket configurado por `ImagesBucket`.

El template actual permite lectura pública de objetos y CORS amplio para ese bucket. Son decisiones visibles del proyecto que deben revisarse antes de un entorno de producción.

## 10. Spotify como integración externa

`SpotifyAPI` contiene dos tipos de autenticación:

### Catálogo público

Usa Client Credentials para buscar tracks, artistas, álbumes y playlists. El token se guarda temporalmente en `TOKEN_CACHE`, memoria del contenedor Lambda, hasta que está cerca de expirar.

### Reproductor del usuario

Usa Authorization Code. El flujo es:

1. El usuario autenticado pide `/spotify/login`.
2. Lambda firma un `state` con HMAC y lo asocia al `user_id`.
3. Spotify redirige a `/spotify/callback` con `code` y `state`.
4. Lambda verifica la firma, intercambia el code por tokens y guarda los tokens en `SpotifyUserTokens`.
5. Cuando el access token expira, Lambda utiliza el refresh token.
6. El frontend obtiene un token para el Web Playback SDK.

El `state` evita aceptar un callback alterado. Los logs del proyecto registran metadatos como si existe un usuario o un device, pero no imprimen tokens.

## 11. Logs y observabilidad

El proyecto usa CloudWatch Logs y activa `Tracing: Active`, que permite el rastreo de AWS X-Ray desde la plantilla.

La capa común define semáforos fijos para los logs:

| Símbolo | Uso |
|---|---|
| ⚪️ | Entrada o información poco relevante |
| 🟢 | Operación exitosa |
| 🔵 | Cálculo o validación importante |
| 🟡 | Advertencia o validación fallida |
| 🔴 | Error |
| 🟤 | Debug |

Los logs deben registrar metadatos útiles y evitar payloads sensibles. En Spotify, por ejemplo, se registra `user_id_present` o `device_present`, no el token.

## 12. IAM y permisos

Cada función recibe permisos definidos en `template.yaml`. Algunos ejemplos del proyecto son:

- Cognito: acciones de registro, login, confirmación y recuperación.
- DynamoDB: políticas CRUD sobre las tablas de cada dominio.
- S3: lectura y escritura para `ImagesFunction`.

El principio práctico es que una Lambda debe recibir solo los permisos que necesita para su responsabilidad. La función de imágenes no necesita administrar usuarios de Cognito, y la función de autenticación no necesita escribir archivos en S3.

## 13. Variables de entorno y parámetros

El template usa parámetros de CloudFormation y variables de entorno para evitar poner nombres o secretos directamente en el código.

Ejemplos:

- `TABLE_NAME` y `CITAS_TABLE_NAME`.
- `FINANCES_TABLE_NAME` y `BODAS_TABLE_NAME`.
- `COGNITO_USER_POOL_ID` y `COGNITO_CLIENT_ID`.
- `SPOTIFY_CLIENT_ID` y `SPOTIFY_CLIENT_SECRET`.
- `SPOTIFY_REDIRECT_URI` y `SPOTIFY_STATE_SECRET`.
- `BUCKET_NAME`.

`NoEcho: true` se usa para parámetros secretos de CloudFormation como el secreto de Spotify y el secreto de firma del estado.

## 14. Construcción y despliegue

Desde la carpeta `lambdas/`:

```text
sam build
sam deploy --guided
sam deploy
sam local start-api
```

`sam build` prepara el código y las capas. `sam deploy` empaqueta y despliega la plantilla mediante CloudFormation. `sam local start-api` sirve para probar la API localmente.

El script `lambdas/deploy.ps1` automatiza un despliegue con PowerShell. Hay que revisar sus parámetros antes de usarlo porque conserva nombres y una región de una versión anterior (`planes-crud-stack`, `us-east-2`), mientras que el template actual es más amplio. La configuración real de cada entorno debe confirmarse antes de desplegar.

## 15. Práctica guiada

### Práctica A: seguir un CRUD

1. Abrir `ChallengesCRUD/handler.py`.
2. Localizar `lambda_handler`.
3. Identificar cómo lee método, ruta, body y parámetros.
4. Localizar la creación de un reto.
5. Seguir la llamada a DynamoDB.
6. Confirmar que la salida usa `build_response`.
7. Comparar el código con `CRUD_DOCUMENTATION.md`.

### Práctica B: seguir un agregado

1. Abrir `FinancesCRUD/handler.py`.
2. Identificar `PK`, `SK`, `monthYear` y `category`.
3. Revisar cómo se registra un gasto.
4. Revisar cómo se calcula el resumen.
5. Comparar el handler con `FINANCES_CRUD_DOCUMENTATION.md`.

### Práctica C: seguir autenticación

1. Abrir `AuthCRUD/handler.py`.
2. Relacionar cada operación con Cognito.
3. Leer la sección de `CognitoUserPool` en `template.yaml`.
4. Revisar cómo API Gateway valida el JWT.
5. Explicar por qué una contraseña no aparece en DynamoDB.

### Práctica D: seguir una integración externa

1. Abrir `SpotifyAPI/handler.py`.
2. Comparar `_get_access_token` con `_get_user_access_token`.
3. Explicar cuándo se usa Client Credentials.
4. Explicar cuándo se usa Authorization Code.
5. Identificar el almacenamiento del refresh token.
6. Revisar `SPOTIFY_PLAYER_FRONTEND_GUIDE.md`.

## 16. Preguntas de evaluación

1. ¿Qué función recibe la petición de API Gateway?
2. ¿Qué diferencia hay entre `pathParameters` y `queryStringParameters`?
3. ¿Por qué existe `common/utils.py`?
4. ¿Qué problema resuelve `DecimalEncoder`?
5. ¿Cuándo se usa `scan_all` y cuándo `query_all`?
6. ¿Qué servicio valida el JWT de la API?
7. ¿Qué datos de identidad lee `_require_user_id`?
8. ¿Por qué Finanzas usa `PK` y `SK`?
9. ¿Qué ventaja tiene una URL prefirmada de S3?
10. ¿Qué diferencia hay entre el token de catálogo y el token del reproductor de Spotify?
11. ¿Qué ocurre cuando una respuesta externa no contiene JSON?
12. ¿Qué datos nunca deberían escribirse en los logs?
13. ¿Qué recurso define la infraestructura completa?
14. ¿Qué diferencia hay entre `sam build` y `sam deploy`?
15. ¿Qué documentación debe considerarse histórica si contradice a `template.yaml`?

## 17. Resumen final

APIDates combina Python y AWS con una frontera clara:

- Python implementa validación, reglas de negocio, transformación y manejo de errores.
- Lambda ejecuta los handlers sin servidores administrados por el equipo.
- API Gateway expone las rutas HTTP.
- Cognito autentica usuarios y emite JWT.
- DynamoDB guarda los agregados y sus relaciones mediante claves e índices.
- S3 almacena archivos y entrega URLs prefirmadas.
- Spotify se integra mediante tokens públicos y tokens vinculados a usuarios.
- AWS SAM y CloudFormation describen y despliegan la infraestructura.
- La capa común elimina duplicación entre Lambdas.

La forma más fiable de entender el sistema es contrastar siempre tres niveles: la ruta declarada en `template.yaml`, el `handler.py` que ejecuta la operación y la utilidad o servicio externo que completa el trabajo.
