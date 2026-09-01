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
from decimal import Decimal, InvalidOperation

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

from common.utils import build_response, get_path_param, parse_body, scan_all

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
TABLE_NAME = os.environ.get('CITAS_TABLE_NAME', 'CitasTable')
table = dynamodb.Table(TABLE_NAME) # type: ignore


# ─── Constantes del modelo ────────────────────────────────────────────────────

VALID_TYPES = {'recuerdo', 'carta', 'evento', 'cancion_semana'}

REQUIRED_FIELDS: dict[str, list[str]] = {
    'recuerdo':       ['title', 'description', 'date'],
    'carta':          ['title', 'description', 'date'],
    'evento':         ['title', 'description', 'date'],
    'cancion_semana': ['title', 'artista', 'link', 'setBy', 'weekKey'],  # se valida que date sea una fecha, aunque no se use directamente
}

# ISO week key: YYYY-WNN  →  "2026-W24"
WEEK_KEY_RE = re.compile(r'^\d{4}-W\d{2}$')


def clean_str(value) -> str:
    return value.strip() if isinstance(value, str) else ''


def normalize_numeric_string(value: str) -> str:
    text = value.strip()
    if not text:
        return text

    if ',' in text and '.' in text:
        if text.rfind(',') > text.rfind('.'):
            text = text.replace('.', '').replace(',', '.')
        else:
            text = text.replace(',', '')
    elif ',' in text:
        text = text.replace(',', '.')
    return text


def to_decimal(value, field_name: str) -> Decimal:
    try:
        if isinstance(value, Decimal):
            return value
        if isinstance(value, str):
            normalized = normalize_numeric_string(value)
            return Decimal(normalized) if normalized else Decimal('0')
        return Decimal(str(value))
    except (TypeError, ValueError, InvalidOperation) as exc:
        raise ValueError(f"El campo '{field_name}' debe ser numérico") from exc


def normalize_image_paths(value, legacy_value=None) -> list[str]:
    if value is None:
        raw_paths = []
    elif isinstance(value, list):
        raw_paths = value
    else:
        raise ValueError("El campo 'imagesPath' debe ser una lista de links")

    image_paths: list[str] = []
    for index, path in enumerate(raw_paths):
        if not isinstance(path, str):
            raise ValueError(f"La imagen en la posición {index} debe ser texto")
        if cleaned := path.strip():
            image_paths.append(cleaned)

    if image_paths or legacy_value is None:
        return image_paths

    if not isinstance(legacy_value, str):
        raise ValueError("El campo 'imagePath' debe ser texto")

    legacy_path = legacy_value.strip()
    return [legacy_path] if legacy_path else []


def normalize_event_documents(value) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("El campo 'documentos' debe ser una lista de links")

    documentos: list[str] = []
    for index, doc in enumerate(value):
        if not isinstance(doc, str):
            raise ValueError(f"El documento en la posición {index} debe ser texto")
        if cleaned := doc.strip():
            documentos.append(cleaned)
    return documentos


def normalize_event_itinerary(value) -> dict:
    if value is None:
        return {'actividades': []}
    if not isinstance(value, dict):
        raise ValueError("El campo 'itinerario' debe ser un objeto")

    raw_activities = value.get('actividades', [])
    if raw_activities is None:
        raw_activities = []
    if not isinstance(raw_activities, list):
        raise ValueError("El campo 'itinerario.actividades' debe ser una lista")

    actividades: list[dict] = []
    for index, activity in enumerate(raw_activities):
        if not isinstance(activity, dict):
            raise ValueError(f"La actividad en la posición {index} debe ser un objeto")

        fecha = clean_str(activity.get('fecha'))
        tiempo = clean_str(activity.get('tiempo'))
        actividad_nombre = clean_str(activity.get('actividad'))

        if not any((fecha, tiempo, actividad_nombre)):
            continue

        if not all((fecha, tiempo, actividad_nombre)):
            raise ValueError(
                f"La actividad en la posición {index} debe incluir fecha, tiempo y actividad"
            )

        actividades.append({
            'fecha': fecha,
            'tiempo': tiempo,
            'actividad': actividad_nombre,
        })

    return {'actividades': actividades}


def normalize_event_budget(value) -> dict:
    if value is None:
        return {
            'gastado': Decimal('0'),
            'limite': Decimal('0'),
            'conceptos': [],
        }
    if not isinstance(value, dict):
        raise ValueError("El campo 'presupuesto' debe ser un objeto")

    raw_concepts = value.get('conceptos', [])
    if raw_concepts is None:
        raw_concepts = []
    if not isinstance(raw_concepts, list):
        raise ValueError("El campo 'presupuesto.conceptos' debe ser una lista")

    conceptos: list[dict] = []
    for index, concept in enumerate(raw_concepts):
        if not isinstance(concept, dict):
            raise ValueError(f"El concepto en la posición {index} debe ser un objeto")

        concepto = clean_str(concept.get('concepto'))
        monto_raw = concept.get('monto', 0)
        monto = to_decimal(monto_raw, f'presupuesto.conceptos[{index}].monto')

        if not concepto and monto == 0:
            continue
        if not concepto:
            raise ValueError(f"El concepto en la posición {index} debe incluir 'concepto'")

        conceptos.append({'concepto': concepto, 'monto': monto})

    return {
        'gastado': to_decimal(value.get('gastado', 0), 'presupuesto.gastado'),
        'limite': to_decimal(value.get('limite', 0), 'presupuesto.limite'),
        'conceptos': conceptos,
    }


def normalize_event_checklist(value) -> dict:
    if value is None:
        return {'items': []}
    if not isinstance(value, dict):
        raise ValueError("El campo 'checklist' debe ser un objeto")

    raw_items = value.get('items', [])
    if raw_items is None:
        raw_items = []
    if not isinstance(raw_items, list):
        raise ValueError("El campo 'checklist.items' debe ser una lista")

    items: list[dict] = []
    for index, item in enumerate(raw_items):
        if not isinstance(item, dict):
            raise ValueError(f"El item del checklist en la posición {index} debe ser un objeto")

        nombre = clean_str(item.get('nombre'))
        incluido = item.get('incluido') is True

        if not nombre and not incluido:
            continue
        if not nombre:
            raise ValueError(
                f"El item del checklist en la posición {index} debe incluir 'nombre'"
            )

        items.append({'nombre': nombre, 'incluido': incluido})

    return {'items': items}


def validate_event_shape(data: dict) -> tuple[bool, str]:
    try:
        normalize_event_documents(data.get('documentos'))
        normalize_event_itinerary(data.get('itinerario'))
        normalize_event_budget(data.get('presupuesto'))
        normalize_event_checklist(data.get('checklist'))
    except ValueError as exc:
        return False, str(exc)
    return True, ''


def deep_merge(base: dict, updates: dict) -> dict:
    merged = dict(base)
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


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

    if event_type == 'recuerdo':
        try:
            image_paths = normalize_image_paths(data.get('imagesPath'), data.get('imagePath'))
        except ValueError as exc:
            return False, str(exc)
        if not image_paths:
            return False, "Campos requeridos faltantes para 'recuerdo': imagesPath"

    if event_type == 'evento':
        return validate_event_shape(data)

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
        base['imagesPaths'] = normalize_image_paths(data.get('imagesPath'), data.get('imagePath'))
    elif event_type == 'carta':
        base['abierta'] = bool(data.get('abierta', False))
        if audio_url := data.get('audioUrl', '').strip():
            base['audioUrl'] = audio_url
        if image_url := data.get('imageUrl', '').strip():
            base['imageUrl'] = image_url
    elif event_type == 'evento':
        base['icon'] = data.get('icon', 'backpack_outlined')
        base['documentos'] = normalize_event_documents(data.get('documentos'))
        base['itinerario'] = normalize_event_itinerary(data.get('itinerario'))
        base['presupuesto'] = normalize_event_budget(data.get('presupuesto'))
        base['checklist'] = normalize_event_checklist(data.get('checklist'))

    return base


def serialize_cita(item: dict) -> dict:
    response_item = dict(item)

    if response_item.get('type') == 'recuerdo':
        response_item['imagesPaths'] = normalize_image_paths(
            response_item.get('imagesPath'),
            response_item.get('imagePath'),
        )
        response_item.pop('imagePath', None)

    return response_item


# ─── Helpers ──────────────────────────────────────────────────────────────────

def current_week_key() -> str:
    """Devuelve la weekKey ISO de la semana actual. Ej: '2026-W24'."""
    iso = datetime.now(timezone.utc).isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


# ─── CRUD: Citas generales ────────────────────────────────────────────────────

def get_item(item_id: str):
    result = table.get_item(Key={'id': item_id})
    if 'Item' in result:
        return build_response(200, serialize_cita(result['Item']))
    return build_response(404, {'message': f"Cita '{item_id}' no encontrada"})


def get_all_items(event_type: str | None = None):
    """GET /citas?type=<tipo> — Devuelve todos o filtrados por tipo."""
    filter_expr = (
        Attr('type').eq(event_type)
        if (event_type and event_type in VALID_TYPES)
        else None
    )
    items = scan_all(table, filter_expr)

    def sort_key(item: dict) -> str:
        # cancion_semana se ordena cronológicamente por weekKey (string ISO)
        if item.get('type') == 'cancion_semana':
            return item.get('weekKey', '')
        try:
            return datetime.strptime(item.get('date', '01-01-1970'), '%d-%m-%Y').isoformat()
        except ValueError:
            return ''

    items.sort(key=sort_key)
    return build_response(200, [serialize_cita(item) for item in items])


def create_item(data: dict):
    valid, msg = validate_cita(data)
    if not valid:
        return build_response(400, {'message': msg})
    item = normalize_cita(data)
    table.put_item(Item=item)
    return build_response(201, {'message': 'Cita creada', 'id': item['id'], 'type': item['type']})


def update_item(item_id: str, data: dict):
    """PUT /citas/{id} — Actualiza los campos enviados y revalida el item completo."""
    update_fields = {k: v for k, v in data.items() if k != 'id'}
    if not update_fields:
        return build_response(400, {'error': 'No hay campos para actualizar'})

    result = table.get_item(Key={'id': item_id})
    existing = result.get('Item')
    if not existing:
        return build_response(404, {'error': f"Cita '{item_id}' no encontrada"})

    merged = deep_merge(existing, update_fields)
    merged['id'] = item_id

    valid, msg = validate_cita(merged)
    if not valid:
        return build_response(400, {'message': msg})

    normalized_item = normalize_cita(merged)

    expr_parts, expr_values, expr_names = [], {}, {}
    for key, value in normalized_item.items():
        if key == 'id':
            continue
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

    table.update_item(
        Key={'id': item_id},
        UpdateExpression='SET #abierta = :abierta',
        ExpressionAttributeNames={'#abierta': 'abierta'},
        ExpressionAttributeValues={':abierta': True},
        ConditionExpression=Attr('id').exists(),
    )
    item['abierta'] = True
    return build_response(200, {'message': 'Carta abierta', 'item': item})


# ─── CRUD: Canción de la Semana ───────────────────────────────────────────────

def get_canciones_semana():
    """
    GET /citas/cancion-semana
    Lista todas las canciones ordenadas por weekKey descendente (más reciente primero).
    """
    items = scan_all(table, Attr('type').eq('cancion_semana'))
    items.sort(key=lambda x: x.get('weekKey', ''), reverse=True)
    return build_response(200, {'items': items, 'count': len(items)})


def get_cancion_semana_actual():
    """
    GET /citas/cancion-semana/current
    Devuelve la canción registrada para la semana ISO actual.
    """
    week_key = current_week_key()
    items = scan_all(table,
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
        table,
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
    item_id = get_path_param(event, 'id')

    logger.info(json.dumps({
        'level': '⚪️',
        'message': 'Solicitud CitasCRUD',
        'method': method,
        'path': path,
        'has_id': bool(item_id),
        'function': getattr(context, 'function_name', 'unknown'),
    }, ensure_ascii=False))

    if method == 'OPTIONS':
        logger.info(json.dumps({'level': '🟢', 'message': 'Preflight CORS CitasCRUD'}, ensure_ascii=False))
        return build_response(200, {})

    # Parseo de body una sola vez
    try:
        body: dict | list = parse_body(event)

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
                    return create_cancion_semana(body) # type: ignore
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
                return update_item(item_id, body) # type: ignore

            case 'DELETE':
                if not item_id:
                    return build_response(400, {'message': 'Se requiere id en la ruta'})
                return delete_item(item_id)

            case _:
                return build_response(405, {'message': f'Método {method} no permitido'})

    except ClientError as e:
        logger.error(json.dumps({
            'level': '🔴',
            'message': 'Error DynamoDB en CitasCRUD',
            'code': e.response.get('Error', {}).get('Code', 'Unknown'),
        }, ensure_ascii=False))
        return build_response(502, {'error': 'Error de base de datos'})
    except Exception:
        logger.exception('🔴 Error inesperado en CitasCRUD')
        return build_response(500, {'error': 'Error interno del servidor'})