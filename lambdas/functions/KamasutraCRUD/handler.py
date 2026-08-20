"""
kamasutra_handler.py — CRUD de Posiciones de Kamasutra
Funcionalidad:
✅ Modelo alineado con KamasutraPosition de Dart
✅ Niveles válidos: facil, medio, avanzado
✅ Validación de campos requeridos
✅ UUID autogenerado si no se provee id
✅ Paginación en scan
✅ Filtrado por ?level=facil
✅ GET /random — devuelve una posición aleatoria
✅ Batch import (POST con lista)
✅ CORS headers incluidos
"""
import json
import logging
import os
import random
import uuid

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

from common.utils import build_response, parse_body, get_path_param

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ.get("KAMASUTRA_TABLE_NAME", "KamasutraTable")
table = dynamodb.Table(TABLE_NAME)

# ─── Modelo ───────────────────────────────────────────────────────────────────

VALID_LEVELS = {"facil", "medio", "avanzado"}

REQUIRED_FIELDS = {"name", "emoji", "shortDesc", "fullDesc", "tips", "level"}


# ─── Handler principal ────────────────────────────────────────────────────────

def lambda_handler(event, context):
    method      = event.get("requestContext", {}).get("http", {}).get("method", "")
    raw_path    = event.get("rawPath", "")
    item_id     = get_path_param(event, "id")
    query_params = event.get("queryStringParameters") or {}
    logger.info(json.dumps({"level": "⚪️", "message": "Solicitud KamasutraCRUD", "method": method, "path": raw_path, "has_id": bool(item_id), "function": getattr(context, "function_name", "unknown")}, ensure_ascii=False))

    if method == "OPTIONS":
        logger.info(json.dumps({"level": "🟢", "message": "Preflight CORS KamasutraCRUD"}, ensure_ascii=False))
        return build_response(200, {})

    try:
        match method:
            case "GET":
                # GET /kamasutra/random — posición aleatoria
                if raw_path.endswith("/random"):
                    return get_random(query_params)
                if item_id:
                    return get_item(item_id)
                return get_all_items(query_params)

            case "POST":
                body = parse_body(event)
                if isinstance(body, list):
                    return bulk_create(body)
                return create_item(body)

            case "PUT":
                if not item_id:
                    return build_response(400, {"error": "Se requiere {id} en la ruta"})
                body = parse_body(event)
                return update_item(item_id, body)

            case "DELETE":
                if not item_id:
                    return build_response(400, {"error": "Se requiere {id} en la ruta"})
                return delete_item(item_id)

            case _:
                return build_response(405, {"error": f"Método {method} no permitido"})

    except ValueError as e:
        logger.warning(json.dumps({"level": "🟡", "message": "Validación fallida en KamasutraCRUD", "error": str(e)}, ensure_ascii=False))
        return build_response(400, {"error": str(e)})
    except ClientError as e:
        logger.error(json.dumps({"level": "🔴", "message": "Error DynamoDB en KamasutraCRUD", "code": e.response.get("Error", {}).get("Code", "Unknown")}, ensure_ascii=False))
        return build_response(502, {"error": "Error de base de datos"})
    except Exception:
        logger.exception("🔴 Error inesperado en KamasutraCRUD")
        return build_response(500, {"error": "Error interno del servidor"})


# ─── CRUD ─────────────────────────────────────────────────────────────────────

def get_item(item_id: str):
    result = table.get_item(Key={"id": item_id})
    if "Item" not in result:
        return build_response(404, {"error": f"Posición '{item_id}' no encontrada"})
    return build_response(200, result["Item"])


def get_all_items(query_params: dict):
    """Scan con paginación completa. Filtra por ?level=facil si se pasa."""
    params: dict = {}

    if level := query_params.get("level"):
        if level not in VALID_LEVELS:
            return build_response(
                400,
                {"error": f"Nivel inválido: '{level}'. Válidos: {', '.join(VALID_LEVELS)}"},
            )
        params["FilterExpression"] = Attr("level").eq(level)

    if last_key := query_params.get("lastKey"):
        params["ExclusiveStartKey"] = {"id": last_key}

    # Scan paginado — recorre todas las páginas
    items = []
    while True:
        result = table.scan(**params)
        items.extend(result.get("Items", []))
        last_evaluated = result.get("LastEvaluatedKey")
        if not last_evaluated:
            break
        params["ExclusiveStartKey"] = last_evaluated

    return build_response(200, {"items": items, "count": len(items)})


def get_random(query_params: dict):
    """Devuelve una posición aleatoria, opcionalmente filtrada por ?level=medio."""
    params: dict = {}

    if level := query_params.get("level"):
        if level not in VALID_LEVELS:
            return build_response(400, {"error": f"Nivel inválido: '{level}'"})
        params["FilterExpression"] = Attr("level").eq(level)

    items = []
    while True:
        result = table.scan(**params)
        items.extend(result.get("Items", []))
        if not result.get("LastEvaluatedKey"):
            break
        params["ExclusiveStartKey"] = result["LastEvaluatedKey"]

    if not items:
        return build_response(
            404,
            {"error": "No hay posiciones disponibles con los filtros aplicados"},
        )

    chosen = random.choice(items)
    logger.info(f"Posición aleatoria: {chosen.get('id')} — {chosen.get('name')}")
    return build_response(200, chosen)


def create_item(data: dict):
    _validate(data)
    item = _normalize(data)
    logger.info(f"Creando posición: {item['id']} — {item['name']}")
    table.put_item(Item=item)
    return build_response(201, {"message": "Posición creada con éxito", "id": item["id"]})


def bulk_create(items: list):
    """Carga masiva usando batch_writer."""
    created = []
    errors  = []

    with table.batch_writer() as batch:
        for i, item_data in enumerate(items):
            try:
                _validate(item_data)
                item = _normalize(item_data)
                batch.put_item(Item=item)
                created.append(item["id"])
            except ValueError as e:
                errors.append({"index": i, "name": item_data.get("name", "?"), "error": str(e)})

    status = 207 if errors else 201
    return build_response(status, {
        "message": f"{len(created)} posiciones creadas",
        "created": created,
        "errors":  errors,
    })


def update_item(item_id: str, data: dict):
    """Usa UpdateItem — solo modifica los campos enviados, no sobrescribe el item."""
    update_fields = {k: v for k, v in data.items() if k != "id"}
    if not update_fields:
        return build_response(400, {"error": "No hay campos para actualizar"})

    # Validar level si se está actualizando
    if "level" in update_fields and update_fields["level"] not in VALID_LEVELS:
        return build_response(
            400,
            {"error": f"Nivel inválido: '{update_fields['level']}'. Válidos: {', '.join(VALID_LEVELS)}"},
        )

    expr_parts  = []
    expr_values = {}
    expr_names  = {}

    for key, value in update_fields.items():
        safe_key = f"#f_{key}"
        val_key  = f":v_{key}"
        expr_parts.append(f"{safe_key} = {val_key}")
        expr_values[val_key] = value
        expr_names[safe_key] = key

    update_expr = "SET " + ", ".join(expr_parts)

    try:
        table.update_item(
            Key={"id": item_id},
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expr_values,
            ExpressionAttributeNames=expr_names,
            ConditionExpression=Attr("id").exists(),  # 404 si no existe
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return build_response(404, {"error": f"Posición '{item_id}' no encontrada"})
        raise

    return build_response(200, {"message": "Posición actualizada", "id": item_id})


def delete_item(item_id: str):
    existing = table.get_item(Key={"id": item_id})
    if "Item" not in existing:
        return build_response(404, {"error": f"Posición '{item_id}' no encontrada"})

    table.delete_item(Key={"id": item_id})
    return build_response(200, {"message": "Posición eliminada con éxito", "id": item_id})


# ─── Validación y Normalización ───────────────────────────────────────────────

def _validate(data: dict):
    missing = REQUIRED_FIELDS - data.keys()
    if missing:
        raise ValueError(f"Campos requeridos faltantes: {missing}")

    for field in ("name", "shortDesc", "fullDesc", "tips"):
        if not isinstance(data.get(field), str) or not data[field].strip():
            raise ValueError(f"El campo '{field}' no puede estar vacío")

    if data.get("level") not in VALID_LEVELS:
        raise ValueError(
            f"Nivel inválido: '{data.get('level')}'. Debe ser uno de: {', '.join(VALID_LEVELS)}"
        )


def _normalize(data: dict) -> dict:
    """Construye el item completo con valores por defecto."""
    return {
        "id":        data.get("id") or str(uuid.uuid4()),
        "name":      data["name"].strip(),
        "emoji":     data.get("emoji", "💫").strip(),
        "shortDesc": data["shortDesc"].strip(),
        "fullDesc":  data["fullDesc"].strip(),
        "tips":      data["tips"].strip(),
        "level":     data["level"],          # facil | medio | avanzado
        "link":      data.get("link", "").strip(),
    }