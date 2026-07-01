"""
citas_handler.py — CRUD de Citas  (v2 + cancion_semana)
─────────────────────────────────────────────────────────
Tipos soportados:
  ✅ recuerdo       — recuerdo con imagen
  ✅ carta          — se abre en una fecha futura
  ✅ evento         — actividad con icono personalizable
  ✅ cancion_semana — canción semanal con link a Spotify  ← NUEVO

Rutas nuevas:
  POST /citas/cancion-semana              → registrar canción (una por semana)
  GET  /citas/cancion-semana              → listar todas (más reciente primero)
  GET  /citas/cancion-semana/current      → canción de la semana en curso
  PUT  /citas/{id}  / DELETE /citas/{id}  → ya funciona para cualquier tipo
"""
import json
import logging
import os
import re
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
TABLE_NAME = os.environ.get('CITAS_TABLE_NAME', 'CitasTable')
table = dynamodb.Table(TABLE_NAME)


# ─── Constantes del modelo ────────────────────────────────────────────────────

VALID_TYPES = {'recuerdo', 'carta', 'evento', 'cancion_semana'}

REQUIRED_FIELDS: dict[str, list[str]] = {
    'recuerdo':       ['title', 'description', 'date', 'imagePath'],
    'carta':          ['title', 'description', 'date'],
    'evento':         ['title', 'description', 'date'],
    'cancion_semana': ['title', 'artista', 'link', 'setBy', 'weekKey'],  # se valida que date sea una fecha, aunque no se use directamente
}

# ISO week key: YYYY-WNN  →  "2026-W24"
WEEK_KEY_RE = re.compile(r'^\d{4}-W\d{2}$')


# ─── Serialización ────────────────────────────────────────────────────────────

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super().default(obj)


def build_response(status_code: int, body: dict | list) -> dict:
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, PATCH, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type',
        },
        'body': json.dumps(body, cls=DecimalEncoder),
    }


# ─── Validación del modelo ────────────────────────────────────────────────────

def validate_cita(data: dict) -> tuple[bool, str]:
    """
    Valida tipo y campos requeridos.
    Retorna (True, '') si es válido, o (False, mensaje) si no.
    """
    event_type = data.get('type', '')
    if event_type not in VALID_TYPES:
        return False, f"Tipo inválido: '{event_type}'. Opciones: {', '.join(sorted(VALID_TYPES))}"

    missing = [f for f in REQUIRED_FIELDS[event_type] if not data.get(f)]
    if missing:
        return False, f"Campos requeridos faltantes para '{event_type}': {', '.join(missing)}"

    if event_type == 'cancion_semana':
        # Validar formato YYYY-WNN
        if not WEEK_KEY_RE.match(data.get('weekKey', '')):
            return False, (
                f"weekKey inválido: '{data.get('weekKey')}'. "
                "Formato esperado: YYYY-WNN (ej: 2026-W24)"
            )
        # Validar que el link sea de Spotify (flexible: URL o URI)
        link = data.get('link', '')
        if link and 'spotify.com' not in link and not link.startswith('spotify:'):
            return False, "El link debe ser de Spotify (https://open.spotify.com/...)"
    else:
        # Validar formato de fecha dd-mm-yyyy para los otros tipos
        try:
            datetime.strptime(data.get('date', ''), '%d-%m-%Y')
        except ValueError:
            return False, f"Fecha inválida: '{data.get('date')}'. Formato esperado: dd-mm-yyyy"

    return True, ''


# ─── Normalización del modelo ─────────────────────────────────────────────────

def normalize_cita(data: dict) -> dict:
    """
    Construye el item completo con valores por defecto según el tipo.
    Genera UUID automáticamente si no viene id.
    """
    event_type = data['type']

    # ── cancion_semana tiene campos distintos a los otros tipos ───────────────
    if event_type == 'cancion_semana':
        return {
            'id':      data.get('id') or str(uuid.uuid4()),
            'type':    event_type,
            'title':  data['title'].strip(),
            'artista': data['artista'].strip(),
            'link':    data['link'].strip(),
            'setBy':   data['setBy'].strip(),
            'weekKey': data['weekKey'].strip(),
            'description': data.get('description', '').strip(),
        }

    # ── Campos base compartidos ────────────────────────────────────────────────
    base = {
        'id':          data.get('id') or str(uuid.uuid4()),
        'type':        event_type,
        'title':       data['title'].strip(),
        'description': data['description'].strip(),
        'date':        data['date'],
    }

    if event_type == 'recuerdo':
        base['imagePath'] = data.get('imagePath', '').strip()
    elif event_type == 'carta':
        base['abierta'] = bool(data.get('abierta', False))
        if audio_url := data.get('audioUrl', '').strip():
            base['audioUrl'] = audio_url
        if image_url := data.get('imageUrl', '').strip():
            base['imageUrl'] = image_url
    elif event_type == 'evento':
        base['icon'] = data.get('icon', 'backpack_outlined')

    return base


# ─── Paginación en scan ───────────────────────────────────────────────────────

def scan_all(filter_expression=None) -> list:
    """Recorre todas las páginas de DynamoDB para evitar el límite de 1 MB."""
    kwargs: dict = {}
    if filter_expression is not None:
        kwargs['FilterExpression'] = filter_expression

    items: list = []
    while True:
        response = table.scan(**kwargs)
        items.extend(response.get('Items', []))
        if not (last_key := response.get('LastEvaluatedKey')):
            break
        kwargs['ExclusiveStartKey'] = last_key
    return items


# ─── Helpers ──────────────────────────────────────────────────────────────────

def current_week_key() -> str:
    """Devuelve la weekKey ISO de la semana actual. Ej: '2026-W24'."""
    iso = datetime.now(timezone.utc).isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


# ─── CRUD: Citas generales ────────────────────────────────────────────────────

def get_item(item_id: str):
    result = table.get_item(Key={'id': item_id})
    if 'Item' in result:
        return build_response(200, result['Item'])
    return build_response(404, {'message': f"Cita '{item_id}' no encontrada"})


def get_all_items(event_type: str | None = None):
    """GET /citas?type=<tipo> — Devuelve todos o filtrados por tipo."""
    filter_expr = (
        Attr('type').eq(event_type)
        if (event_type and event_type in VALID_TYPES)
        else None
    )
    items = scan_all(filter_expr)

    def sort_key(item: dict) -> str:
        # cancion_semana se ordena cronológicamente por weekKey (string ISO)
        if item.get('type') == 'cancion_semana':
            return item.get('weekKey', '')
        try:
            return datetime.strptime(item.get('date', '01-01-1970'), '%d-%m-%Y').isoformat()
        except ValueError:
            return ''

    items.sort(key=sort_key)
    return build_response(200, items)


def create_item(data: dict):
    valid, msg = validate_cita(data)
    if not valid:
        return build_response(400, {'message': msg})
    item = normalize_cita(data)
    table.put_item(Item=item)
    return build_response(201, {'message': 'Cita creada', 'id': item['id'], 'type': item['type']})


def update_item(item_id: str, data: dict):
    """PUT /citas/{id} — Actualiza solo los campos enviados (UpdateExpression)."""
    update_fields = {k: v for k, v in data.items() if k != 'id'}
    if not update_fields:
        return build_response(400, {'error': 'No hay campos para actualizar'})

    expr_parts, expr_values, expr_names = [], {}, {}
    for key, value in update_fields.items():
        sk, vk = f"#f_{key}", f":v_{key}"
        expr_parts.append(f"{sk} = {vk}")
        expr_values[vk] = value
        expr_names[sk] = key

    try:
        table.update_item(
            Key={'id': item_id},
            UpdateExpression='SET ' + ', '.join(expr_parts),
            ExpressionAttributeValues=expr_values,
            ExpressionAttributeNames=expr_names,
            ConditionExpression=Attr('id').exists(),   # 404 si no existe
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            return build_response(404, {'error': f"Cita '{item_id}' no encontrada"})
        raise

    return build_response(200, {'message': 'Cita actualizada', 'id': item_id})


def delete_item(item_id: str):
    if 'Item' not in table.get_item(Key={'id': item_id}):
        return build_response(404, {'message': f"Cita '{item_id}' no encontrada"})
    table.delete_item(Key={'id': item_id})
    return build_response(200, {'message': 'Cita eliminada'})


def open_carta(item_id: str):
    """PATCH /citas/{id}/abrir — Marca una carta como abierta si la fecha ya llegó."""
    result = table.get_item(Key={'id': item_id})
    if 'Item' not in result:
        return build_response(404, {'message': 'Carta no encontrada'})

    item = result['Item']
    if item.get('type') != 'carta':
        return build_response(400, {'message': 'Este item no es una carta'})

    # Comparación naive vs naive (ambos sin tzinfo) — corrige bug original
    fecha_carta = datetime.strptime(item['date'], '%d-%m-%Y')
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    if now < fecha_carta:
        return build_response(403, {
            'message': f"La carta aún no puede abrirse. Faltan {(fecha_carta - now).days} días.",
            'openDate': item['date'],
        })

    item['abierta'] = True
    table.put_item(Item=item)
    return build_response(200, {'message': 'Carta abierta', 'item': item})


# ─── CRUD: Canción de la Semana ───────────────────────────────────────────────

def get_canciones_semana():
    """
    GET /citas/cancion-semana
    Lista todas las canciones ordenadas por weekKey descendente (más reciente primero).
    """
    items = scan_all(Attr('type').eq('cancion_semana'))
    items.sort(key=lambda x: x.get('weekKey', ''), reverse=True)
    return build_response(200, {'items': items, 'count': len(items)})


def get_cancion_semana_actual():
    """
    GET /citas/cancion-semana/current
    Devuelve la canción registrada para la semana ISO actual.
    """
    week_key = current_week_key()
    items = scan_all(
        Attr('type').eq('cancion_semana') & Attr('weekKey').eq(week_key)
    )
    if not items:
        return build_response(404, {
            'message': f"No hay canción registrada para {week_key}",
            'weekKey': week_key,
        })
    return build_response(200, items[0])


def create_cancion_semana(data: dict):
    """
    POST /citas/cancion-semana
    ─ El campo 'type' se inyecta automáticamente por ruta.
    ─ Garantiza unicidad: solo una canción por weekKey (409 si ya existe).
    ─ El Flutter puede omitir el tipo en el body.

    Body esperado:
    {
      "title":  "nombre de la canción",
      "artista": "nombre del artista",
      "link":    "https://open.spotify.com/track/...",
      "setBy":   "nombre de quien la elige",
      "weekKey": "2026-W24"          ← semana ISO actual
      "date":    "dd-mm-yyyy"         ← se valida formato, aunque no se use directamente
      "description": "opcional, puede incluir letra o comentario"  ← NUEVO
    }
    """
    data['type'] = 'cancion_semana'   # se inyecta, no lo envía el cliente

    valid, msg = validate_cita(data)
    if not valid:
        return build_response(400, {'message': msg})

    # Unicidad por semana — evita duplicados
    week_key = data['weekKey']
    existing = scan_all(
        Attr('type').eq('cancion_semana') & Attr('weekKey').eq(week_key)
    )
    if existing:
        return build_response(409, {
            'message': f"Ya existe una canción para la semana {week_key}",
            'existing_id': existing[0]['id'],
        })

    item = normalize_cita(data)
    table.put_item(Item=item)
    logger.info(json.dumps({
        'event':   'cancion_semana_created',
        'id':      item['id'],
        'title':  item['title'],
        'weekKey': week_key,
    }))
    return build_response(201, {
        'message': 'Canción de la semana registrada',
        'id':      item['id'],
        'weekKey': week_key,
    })


# ─── Handler principal ────────────────────────────────────────────────────────

def lambda_handler(event, context):
    method  = event.get('requestContext', {}).get('http', {}).get('method', '')
    path    = event.get('rawPath', '')
    params  = event.get('pathParameters') or {}
    qparams = event.get('queryStringParameters') or {}
    item_id = params.get('id')

    logger.info(json.dumps({'method': method, 'path': path, 'id': item_id}))

    if method == 'OPTIONS':
        return build_response(200, {})

    # Parseo de body una sola vez
    body: dict | list = {}
    if raw := event.get('body'):
        try:
            body = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return build_response(400, {'message': 'Body JSON inválido'})

    try:
        # ── /citas/cancion-semana ─────────────────────────────────────────────
        # IMPORTANTE: estas rutas deben evaluarse ANTES que /citas/{id}
        # para que "cancion-semana" no sea interpretado como un {id}.
        # API Gateway HTTP las prioriza automáticamente (ruta literal > parámetro),
        # pero el match en código también lo garantiza.
        if 'cancion-semana' in path:
            match method:
                case 'GET':
                    if path.endswith('/current'):
                        return get_cancion_semana_actual()
                    return get_canciones_semana()
                case 'POST':
                    return create_cancion_semana(body)
                case _:
                    return build_response(405, {'message': f'Método {method} no permitido en /cancion-semana'})

        # ── PATCH especial para cartas ─────────────────────────────────────────
        if method == 'PATCH' and item_id and path.endswith('/abrir'):
            return open_carta(item_id)

        # ── Citas generales ────────────────────────────────────────────────────
        match method:
            case 'GET':
                return get_item(item_id) if item_id else get_all_items(qparams.get('type'))

            case 'POST':
                if isinstance(body, list):
                    ok, errs = [], []
                    for i, d in enumerate(body):
                        r = create_item(d)
                        parsed = json.loads(r['body'])
                        if r['statusCode'] == 201:
                            ok.append(parsed['id'])
                        else:
                            errs.append({'index': i, 'error': parsed.get('message')})
                    return build_response(
                        207 if errs else 201,
                        {'created': ok, 'errors': errs}
                    )
                return create_item(body)

            case 'PUT':
                if not item_id:
                    return build_response(400, {'message': 'Se requiere id en la ruta'})
                return update_item(item_id, body)

            case 'DELETE':
                if not item_id:
                    return build_response(400, {'message': 'Se requiere id en la ruta'})
                return delete_item(item_id)

            case _:
                return build_response(405, {'message': f'Método {method} no permitido'})

    except ClientError as e:
        logger.error(json.dumps({'dynamo_error': e.response['Error']}))
        return build_response(502, {'error': 'Error de base de datos'})
    except Exception:
        logger.exception('Error inesperado')
        return build_response(500, {'error': 'Error interno del servidor'})