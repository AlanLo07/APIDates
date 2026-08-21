"""
utils.py — Utilidades compartidas entre Lambdas
Mejoras aplicadas:
- Serialización de Decimal para JSON
- build_response centralizado con JSON body serializado
- Constantes y helpers reutilizables
"""
import json
import logging
from decimal import Decimal


LOG_LEVELS = {
    "⚪️": logging.INFO,
    "🟢": logging.INFO,
    "🔵": logging.INFO,
    "🟡": logging.WARNING,
    "🔴": logging.ERROR,
    "🟤": logging.DEBUG,
}


def log_event(logger, indicator: str, message: str, **fields) -> None:
    """Emite logs JSON con semáforo y metadatos no sensibles."""
    if indicator not in LOG_LEVELS:
        raise ValueError(f"Indicador de log no soportado: {indicator}")

    payload = {"level": indicator, "message": message, **fields}
    logger.log(LOG_LEVELS[indicator], json.dumps(payload, ensure_ascii=False))


def scan_all(table, filter_expression=None, **scan_kwargs) -> list[dict]:
    """Recorre todas las páginas de un Scan y devuelve sus items."""
    items = []
    request = dict(scan_kwargs)
    if filter_expression is not None:
        request["FilterExpression"] = filter_expression

    while True:
        response = table.scan(**request)
        items.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            return items
        request["ExclusiveStartKey"] = last_key


def query_all(table, **query_kwargs) -> list[dict]:
    """Recorre todas las páginas de una Query y devuelve sus items."""
    items = []
    request = dict(query_kwargs)
    total_limit = request.get("Limit")

    while True:
        if total_limit is not None:
            request["Limit"] = min(total_limit - len(items), total_limit)
        response = table.query(**request)
        items.extend(response.get("Items", []))
        if total_limit is not None and len(items) >= total_limit:
            return items[:total_limit]
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            return items
        request["ExclusiveStartKey"] = last_key


class DecimalEncoder(json.JSONEncoder):
    """Serializa Decimal de DynamoDB a float/int para JSON."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            # Conserva entero si no tiene decimales
            return int(obj) if obj % 1 == 0 else float(obj)
        return super().default(obj)


def build_response(status_code: int, body: dict | list) -> dict:
    """
    Construye una respuesta HTTP estándar con CORS.
    El body se serializa a JSON string (requerido por Lambda Function URL y API Gateway).
    """
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,PATCH,OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,X-Api-Key",
        },
        "body": json.dumps(body, cls=DecimalEncoder),
    }


def parse_body(event: dict) -> dict | list:
    """Parsea el body del evento de forma segura."""
    raw = event.get("body") or "{}"
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"Body inválido: {e}") from e


def get_path_param(event: dict, key: str) -> str | None:
    """Obtiene un path parameter de forma segura."""
    return (event.get("pathParameters") or {}).get(key)


# ─── CRUD Genérico (reutilizable entre handlers) ─────────────────────────────

def generic_get_item(table, item_id: str, pk_name: str = "id") -> dict:
    """
    GET item genérico por ID.
    
    Parámetros:
    - table: recurso DynamoDB table
    - item_id: ID del item
    - pk_name: nombre de la clave primaria (default: "id")
    
    Retorna: Response HTTP
    """
    result = table.get_item(Key={pk_name: item_id})
    if "Item" not in result:
        return build_response(404, {"error": f"Item '{item_id}' no encontrado"})
    return build_response(200, result["Item"])


def generic_delete_item(table, item_id: str, pk_name: str = "id", entity_name: str = "Item") -> dict:
    """
    DELETE item genérico.
    
    Parámetros:
    - table: recurso DynamoDB table
    - item_id: ID del item
    - pk_name: nombre de la clave primaria (default: "id")
    - entity_name: nombre de la entidad para el mensaje (default: "Item")
    
    Retorna: Response HTTP
    """
    from boto3.dynamodb.conditions import Attr
    from botocore.exceptions import ClientError
    
    try:
        table.delete_item(
            Key={pk_name: item_id},
            ConditionExpression=Attr(pk_name).exists(),
        )
        return build_response(200, {
            "message": f"{entity_name} eliminado con éxito",
            "id": item_id
        })
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return build_response(404, {"error": f"{entity_name} '{item_id}' no encontrado"})
        raise


def build_update_expression(data: dict, excluded_fields: set = None) -> tuple:
    """
    Construye UpdateExpression, ExpressionAttributeValues y ExpressionAttributeNames.
    
    Parámetros:
    - data: diccionario con campos a actualizar
    - excluded_fields: campos a excluir (ej: "id", "createdAt")
    
    Retorna: (UpdateExpression, ExpressionAttributeValues, ExpressionAttributeNames)
    """
    if excluded_fields is None:
        excluded_fields = {"id", "createdAt"}
    
    update_fields = {k: v for k, v in data.items() if k not in excluded_fields}
    if not update_fields:
        raise ValueError("No hay campos para actualizar")
    
    # Agregar timestamp de actualización
    update_fields["updatedAt"] = _get_iso_timestamp()
    
    expr_parts = []
    expr_values = {}
    expr_names = {}
    
    for key, value in update_fields.items():
        safe_key = f"#f_{key}"
        val_key = f":v_{key}"
        expr_parts.append(f"{safe_key} = {val_key}")
        expr_values[val_key] = value
        expr_names[safe_key] = key
    
    update_expr = "SET " + ", ".join(expr_parts)
    return update_expr, expr_values, expr_names


def generic_update_item(
    table,
    item_id: str,
    data: dict,
    pk_name: str = "id",
    entity_name: str = "Item"
) -> dict:
    """
    UPDATE item genérico con UpdateExpression.
    
    Parámetros:
    - table: recurso DynamoDB table
    - item_id: ID del item
    - data: diccionario con campos a actualizar
    - pk_name: nombre de la clave primaria (default: "id")
    - entity_name: nombre de la entidad para mensajes
    
    Retorna: Response HTTP
    """
    from boto3.dynamodb.conditions import Attr
    from botocore.exceptions import ClientError
    
    try:
        update_expr, expr_values, expr_names = build_update_expression(data)
        
        table.update_item(
            Key={pk_name: item_id},
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expr_values,
            ExpressionAttributeNames=expr_names,
            ConditionExpression=Attr(pk_name).exists(),
        )
        return build_response(200, {"message": f"{entity_name} actualizado", "id": item_id})
    except ValueError as e:
        return build_response(400, {"error": str(e)})
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return build_response(404, {"error": f"{entity_name} '{item_id}' no encontrado"})
        raise


def generic_create_item(
    table,
    data: dict,
    pk_name: str = "id",
    entity_name: str = "Item"
) -> dict:
    """
    CREATE item genérico con normalización.
    
    Parámetros:
    - table: recurso DynamoDB table
    - data: diccionario del item
    - pk_name: nombre de la clave primaria (default: "id")
    - entity_name: nombre de la entidad para mensajes
    
    Retorna: Response HTTP
    """
    import uuid
    
    item = data.copy()
    
    # Generar ID si no existe
    if pk_name not in item or not item[pk_name]:
        item[pk_name] = str(uuid.uuid4())
    
    # Agregar timestamps
    now = _get_iso_timestamp()
    if "createdAt" not in item:
        item["createdAt"] = now
    if "updatedAt" not in item:
        item["updatedAt"] = now
    
    table.put_item(Item=item)
    return build_response(201, {
        "message": f"{entity_name} creado con éxito",
        "id": item[pk_name]
    })


def generic_bulk_create(
    table,
    items: list,
    pk_name: str = "id",
    entity_name: str = "Items"
) -> dict:
    """
    BULK CREATE con batch_writer.
    
    Parámetros:
    - table: recurso DynamoDB table
    - items: lista de diccionarios
    - pk_name: nombre de la clave primaria
    - entity_name: nombre de la entidad para mensajes
    
    Retorna: Response HTTP
    """
    import uuid
    
    created = []
    errors = []
    now = _get_iso_timestamp()
    
    with table.batch_writer() as batch:
        for i, item_data in enumerate(items):
            try:
                item = item_data.copy()
                
                # Generar ID si no existe
                if pk_name not in item or not item[pk_name]:
                    item[pk_name] = str(uuid.uuid4())
                
                # Agregar timestamps
                if "createdAt" not in item:
                    item["createdAt"] = now
                if "updatedAt" not in item:
                    item["updatedAt"] = now
                
                batch.put_item(Item=item)
                created.append(item[pk_name])
            except (ValueError, KeyError) as e:
                errors.append({"index": i, "error": str(e)})
    
    status = 207 if errors else 201
    return build_response(status, {
        "message": f"{len(created)} {entity_name} creados",
        "created": created,
        "errors": errors,
    })


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _get_iso_timestamp() -> str:
    """Retorna timestamp ISO 8601 en UTC."""
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_string_fields(item: dict, fields: set = None) -> dict:
    """
    Normaliza campos string: trimming y lowercase opcional.
    
    Parámetros:
    - item: diccionario con datos
    - fields: conjunto de campos a normalizar (None = todos los strings)
    
    Retorna: item normalizado
    """
    normalized = item.copy()
    
    if fields is None:
        # Normalizar todos los campos string
        for key, value in normalized.items():
            if isinstance(value, str):
                normalized[key] = value.strip()
    else:
        # Normalizar solo los campos especificados
        for field in fields:
            if field in normalized and isinstance(normalized[field], str):
                normalized[field] = normalized[field].strip()
    
    return normalized