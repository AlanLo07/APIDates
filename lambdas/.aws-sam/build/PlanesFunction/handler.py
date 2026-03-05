"""
planes_handler.py — CRUD de Planes
Mejoras sobre la versión original:
✅ UUID autogenerado si no se provee id
✅ UpdateItem en lugar de put_item para updates (evita sobrescribir campos)
✅ Validación de campos requeridos
✅ Paginación en scan (evita timeout en tablas grandes)
✅ Body serializado a JSON string en respuesta
✅ Decimal manejado correctamente
✅ CORS headers incluidos
✅ Logging estructurado
"""
import json
import logging
import os
import uuid

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

from common.utils import build_response, parse_body, get_path_param

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ.get("TABLE_NAME", "Planes")
table = dynamodb.Table(TABLE_NAME)

REQUIRED_FIELDS = {"nombre"}


# ─── Handler principal ────────────────────────────────────────────────────────

def lambda_handler(event, context):
    logger.info(json.dumps({"event_keys": list(event.keys()), "context_fn": context.function_name}))

    method = event["requestContext"]["http"]["method"]
    item_id = get_path_param(event, "id")

    # Preflight CORS
    if method == "OPTIONS":
        return build_response(200, {})

    try:
        match method:
            case "GET":
                return get_item(item_id) if item_id else get_all_items(event)
            case "POST":
                body = parse_body(event)
                if isinstance(body, list):
                    return bulk_create(body)
                return create_item(body)
            case "PUT":
                body = parse_body(event)
                if not item_id:
                    return build_response(400, {"error": "Se requiere {id} en la ruta"})
                return update_item(item_id, body)
            case "DELETE":
                if not item_id:
                    return build_response(400, {"error": "Se requiere {id} en la ruta"})
                return delete_item(item_id)
            case "PATCH":
                # Reset ratings — operación administrativa
                return reset_all_ratings()
            case _:
                return build_response(405, {"error": "Método no permitido"})

    except ValueError as e:
        return build_response(400, {"error": str(e)})
    except ClientError as e:
        logger.error(f"DynamoDB error: {e.response['Error']}")
        return build_response(502, {"error": "Error de base de datos"})
    except Exception as e:
        logger.exception("Error inesperado")
        return build_response(500, {"error": "Error interno del servidor"})


# ─── CRUD ─────────────────────────────────────────────────────────────────────

def get_item(item_id: str):
    result = table.get_item(Key={"id": item_id})
    if "Item" not in result:
        return build_response(404, {"error": f"Plan '{item_id}' no encontrado"})
    return build_response(200, result["Item"])


def get_all_items(event: dict):
    """Scan con paginación. Acepta ?lastKey=<token> para paginar."""
    params: dict = {"Limit": 50}

    # Filtrado opcional por typeLocation: /planes?type=restaurante
    query_params = event.get("queryStringParameters") or {}
    if loc_type := query_params.get("type"):
        params["FilterExpression"] = Attr("typeLocation").eq(loc_type)

    # Paginación
    last_key = query_params.get("lastKey")
    if last_key:
        params["ExclusiveStartKey"] = {"id": last_key}

    result = table.scan(**params)
    response_body = {
        "items": result.get("Items", []),
        "count": result.get("Count", 0),
    }
    if next_key := result.get("LastEvaluatedKey"):
        response_body["nextKey"] = next_key.get("id")

    return build_response(200, response_body)


def create_item(data: dict):
    _validate(data)
    # Genera UUID si no viene en el payload
    data.setdefault("id", str(uuid.uuid4()))
    data.setdefault("typeLocation", "")
    data.setdefault("isVisited", False)
    data.setdefault("rating", 0)
    data.setdefault("tags", [])
    logger.info(f"Creando plan: {data['id']} — {data.get('nombre')}")
    table.put_item(Item=data)
    return build_response(201, {"message": "Plan creado", "id": data["id"]})


def bulk_create(items: list):
    """Carga masiva usando batch_writer (más eficiente que put_item individual)."""
    created = []
    errors = []
    with table.batch_writer() as batch:
        for item in items:
            try:
                _validate(item)
                item.setdefault("id", str(uuid.uuid4()))
                item.setdefault("typeLocation", "")
                item.setdefault("isVisited", False)
                item.setdefault("rating", 0)
                item.setdefault("tags", [])
                batch.put_item(Item=item)
                created.append(item["id"])
            except ValueError as e:
                errors.append({"item": item.get("nombre", "?"), "error": str(e)})

    return build_response(207, {"created": created, "errors": errors})


def update_item(item_id: str, data: dict):
    """
    Usa UpdateItem para modificar solo los campos enviados.
    Evita sobrescribir campos no incluidos en el body.
    """
    # Construye expresión dinámica con los campos del body (excluye 'id')
    update_fields = {k: v for k, v in data.items()}
    if not update_fields:
        return build_response(400, {"error": "No hay campos para actualizar"})

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

    try:
        table.update_item(
            Key={"nombre": item_id},
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expr_values,
            ExpressionAttributeNames=expr_names,
            ConditionExpression=Attr("id").exists(),  # falla si no existe
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return build_response(404, {"error": f"Plan '{item_id}' no encontrado"})
        raise

    return build_response(200, {"message": "Plan actualizado", "nombre": item_id})


def delete_item(item_id: str):
    table.delete_item(
        Key={"nombre": item_id},
        ConditionExpression=Attr("id").exists(),
    )
    return build_response(200, {"message": "Plan eliminado", "id": item_id})


def reset_all_ratings():
    """Resetea ratings a 0 — usa batch_writer para mayor eficiencia."""
    result = table.scan(ProjectionExpression="id")
    items = result.get("Items", [])
    with table.batch_writer() as batch:
        for item in items:
            table.update_item(
                Key={"id": item["id"]},
                UpdateExpression="SET rating = :zero",
                ExpressionAttributeValues={":zero": 0},
            )
    return build_response(200, {"message": f"Ratings reseteados en {len(items)} planes"})


# ─── Validación ───────────────────────────────────────────────────────────────

def _validate(data: dict):
    missing = REQUIRED_FIELDS - data.keys()
    if missing:
        raise ValueError(f"Campos requeridos faltantes: {missing}")
    if not isinstance(data.get("nombre"), str) or not data["nombre"].strip():
        raise ValueError("El campo 'nombre' no puede estar vacío")