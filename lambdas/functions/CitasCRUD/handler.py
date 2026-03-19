import json
import boto3
import os
import uuid
from decimal import Decimal
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
TABLE_NAME = os.environ.get('CITAS_TABLE_NAME', 'CitasTable')
table = dynamodb.Table(TABLE_NAME)

# ─── Tipos válidos y sus campos requeridos ────────────────────────────────────
VALID_TYPES = {'recuerdo', 'carta', 'evento'}

REQUIRED_FIELDS = {
    'recuerdo': ['title', 'description', 'date', 'imagePath'],
    'carta':    ['title', 'description', 'date'],
    'evento':   ['title', 'description', 'date'],
}

# ─── Serializador de Decimal para json.dumps ──────────────────────────────────
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)

# ─── Helper de respuesta ─────────────────────────────────────────────────────
def build_response(status_code, body):
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
    Valida que el item tenga el tipo correcto y los campos requeridos.
    Retorna (True, '') si es válido, o (False, mensaje_error) si no.
    """
    event_type = data.get('type', '')
    if event_type not in VALID_TYPES:
        return False, f"Tipo inválido: '{event_type}'. Debe ser uno de: {', '.join(VALID_TYPES)}"

    required = REQUIRED_FIELDS[event_type]
    missing = [f for f in required if not data.get(f)]
    if missing:
        return False, f"Campos requeridos faltantes para tipo '{event_type}': {', '.join(missing)}"

    # Validar formato de fecha dd-mm-yyyy
    date_str = data.get('date', '')
    try:
        datetime.strptime(date_str, '%d-%m-%Y')
    except ValueError:
        return False, f"Formato de fecha inválido: '{date_str}'. Se espera dd-mm-yyyy"

    return True, ''

# ─── Normalización del modelo por tipo ───────────────────────────────────────
def normalize_cita(data: dict) -> dict:
    """
    Asegura que cada tipo tenga sus campos con valores por defecto
    y elimina campos que no corresponden al tipo.
    """
    event_type = data['type']
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

    elif event_type == 'evento':
        # icon se guarda como string (nombre del IconData de Flutter)
        # Si no viene, usamos el default del modelo Dart
        base['icon'] = data.get('icon', 'backpack_outlined')

    return base

# ─── Paginación en scan ───────────────────────────────────────────────────────
def scan_all(filter_expression=None, filter_kwargs=None) -> list:
    """
    Recorre todas las páginas de DynamoDB para evitar el límite de 1MB.
    """
    kwargs = {}
    if filter_expression:
        kwargs['FilterExpression'] = filter_expression
    if filter_kwargs:
        kwargs.update(filter_kwargs)

    items = []
    while True:
        response = table.scan(**kwargs)
        items.extend(response.get('Items', []))
        last_key = response.get('LastEvaluatedKey')
        if not last_key:
            break
        kwargs['ExclusiveStartKey'] = last_key
    return items

# ─── CRUD ─────────────────────────────────────────────────────────────────────
def get_item(item_id: str):
    result = table.get_item(Key={'id': item_id})
    if 'Item' in result:
        return build_response(200, result['Item'])
    return build_response(404, {'message': f"Cita con id '{item_id}' no encontrada"})

def get_all_items(event_type: str | None = None):
    """
    Obtiene todos los items. Si se pasa ?type=recuerdo filtra por tipo.
    """
    if event_type and event_type in VALID_TYPES:
        from boto3.dynamodb.conditions import Attr
        items = scan_all(
            filter_expression=Attr('type').eq(event_type)
        )
    else:
        items = scan_all()

    # Ordenar por fecha (dd-mm-yyyy → comparable)
    def sort_key(item):
        try:
            return datetime.strptime(item.get('date', '01-01-1970'), '%d-%m-%Y')
        except ValueError:
            return datetime.min

    items.sort(key=sort_key)
    return build_response(200, items)

def create_item(data: dict):
    valid, error_msg = validate_cita(data)
    if not valid:
        return build_response(400, {'message': error_msg})

    item = normalize_cita(data)
    table.put_item(Item=item)
    return build_response(201, {'message': 'Cita creada con éxito', 'id': item['id'], 'type': item['type']})

def update_item(item_id: str, data: dict):
    # Verificar que existe
    existing = table.get_item(Key={'id': item_id})
    if 'Item' not in existing:
        return build_response(404, {'message': f"Cita con id '{item_id}' no encontrada"})

    data['id'] = item_id
    valid, error_msg = validate_cita(data)
    if not valid:
        return build_response(400, {'message': error_msg})

    item = normalize_cita(data)
    table.put_item(Item=item)
    return build_response(200, {'message': 'Cita actualizada con éxito'})

def delete_item(item_id: str):
    existing = table.get_item(Key={'id': item_id})
    if 'Item' not in existing:
        return build_response(404, {'message': f"Cita con id '{item_id}' no encontrada"})

    table.delete_item(Key={'id': item_id})
    return build_response(200, {'message': 'Cita eliminada con éxito'})

def open_carta(item_id: str):
    """
    PATCH /{id}/abrir — Marca una carta como abierta si la fecha ya llegó.
    """
    result = table.get_item(Key={'id': item_id})
    if 'Item' not in result:
        return build_response(404, {'message': 'Carta no encontrada'})

    item = result['Item']
    if item.get('type') != 'carta':
        return build_response(400, {'message': 'Este item no es una carta'})

    fecha_carta = datetime.strptime(item['date'], '%d-%m-%Y')
    if datetime.now() < fecha_carta:
        days_left = (fecha_carta - datetime.now()).days
        return build_response(403, {
            'message': f'La carta aún no puede abrirse. Faltan {days_left} días.',
            'openDate': item['date']
        })

    item['abierta'] = True
    table.put_item(Item=item)
    return build_response(200, {'message': 'Carta abierta', 'item': item})

# ─── Handler principal ────────────────────────────────────────────────────────
def lambda_handler(event, context):
    http_method  = event.get('requestContext', {}).get('http', {}).get('method', '')
    path         = event.get('rawPath', '')
    path_params  = event.get('pathParameters') or {}
    query_params = event.get('queryStringParameters') or {}
    item_id      = path_params.get('id')

    print(f"[{http_method}] {path} | id={item_id} | query={query_params}")

    # Parsear body una sola vez
    body = {}
    raw_body = event.get('body', '{}')
    if raw_body:
        try:
            body = json.loads(raw_body)
        except (json.JSONDecodeError, TypeError):
            return build_response(400, {'message': 'Body JSON inválido'})

    try:
        # Ruta especial: PATCH /{id}/abrir → abre una carta
        if http_method == 'PATCH' and item_id and path.endswith('/abrir'):
            return open_carta(item_id)

        if http_method == 'GET':
            if item_id:
                return get_item(item_id)
            return get_all_items(event_type=query_params.get('type'))

        elif http_method == 'POST':
            if isinstance(body, list):
                results = []
                errors  = []
                for i, item_data in enumerate(body):
                    resp = create_item(item_data)
                    if resp['statusCode'] != 201:
                        errors.append({'index': i, 'error': json.loads(resp['body'])['message']})
                    else:
                        results.append(json.loads(resp['body'])['id'])
                if errors:
                    return build_response(207, {'created': results, 'errors': errors})
                return build_response(201, {'message': f'{len(results)} citas creadas', 'ids': results})
            return create_item(body)

        elif http_method == 'PUT':
            if not item_id:
                return build_response(400, {'message': 'Se requiere id en la ruta para PUT'})
            return update_item(item_id, body)

        elif http_method == 'DELETE':
            if not item_id:
                return build_response(400, {'message': 'Se requiere id en la ruta para DELETE'})
            return delete_item(item_id)

        else:
            return build_response(405, {'message': f'Método {http_method} no permitido'})

    except Exception as e:
        print(f"[ERROR] {e}")
        return build_response(500, {'error': str(e)})