"""
challenges_handler.py — CRUD de Retos/Challenges
Funcionalidad:
✅ Modelo alineado con ChallengeItem de Dart
✅ Niveles válidos: suave, picante, atrevido
✅ Validación de campos requeridos
✅ UUID autogenerado si no se provee id
✅ Timestamps de auditoría (createdAt, updatedAt)
✅ Paginación en scan
✅ Filtrado por ?level=suave
✅ GET /random — devuelve un reto aleatorio
✅ Batch import (POST con lista)
✅ CORS headers incluidos
"""
import json
import logging
import os
import random
import uuid
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

from common.utils import build_response, get_path_param, log_event, parse_body, scan_all

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ.get("CHALLENGES_TABLE_NAME", "ChallengesTable")
table = dynamodb.Table(TABLE_NAME)

# ─── Modelo ───────────────────────────────────────────────────────────────────

VALID_LEVELS = {"suave", "picante", "atrevido"}

REQUIRED_FIELDS = {"text", "emoji", "level"}


# ─── Handler principal ────────────────────────────────────────────────────────

def lambda_handler(event, context):
    method      = event.get("requestContext", {}).get("http", {}).get("method", "")
    raw_path    = event.get("rawPath", "")
    item_id     = get_path_param(event, "id")
    query_params = event.get("queryStringParameters") or {}
    logger.info(json.dumps({"level": "⚪️", "message": "Solicitud ChallengesCRUD", "method": method, "path": raw_path, "has_id": bool(item_id), "function": getattr(context, "function_name", "unknown")}, ensure_ascii=False))

    if method == "OPTIONS":
        logger.info(json.dumps({"level": "🟢", "message": "Preflight CORS ChallengesCRUD"}, ensure_ascii=False))
        return build_response(200, {})

    try:
        match method:
            case "GET":
                # GET /challenges/random — reto aleatorio
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
        logger.warning(json.dumps({"level": "🟡", "message": "Validación fallida en ChallengesCRUD", "error": str(e)}, ensure_ascii=False))
        return build_response(400, {"error": str(e)})
    except ClientError as e:
        logger.error(json.dumps({"level": "🔴", "message": "Error DynamoDB en ChallengesCRUD", "code": e.response.get("Error", {}).get("Code", "Unknown")}, ensure_ascii=False))
        return build_response(502, {"error": "Error de base de datos"})
    except Exception:
        logger.exception("🔴 Error inesperado en ChallengesCRUD")
        return build_response(500, {"error": "Error interno del servidor"})


# ─── CRUD ─────────────────────────────────────────────────────────────────────

def get_item(item_id: str):
    result = table.get_item(Key={"id": item_id})
    if "Item" not in result:
        return build_response(404, {"error": f"Reto '{item_id}' no encontrado"})
    return build_response(200, result["Item"])


def get_all_items(query_params: dict):
    """Scan con paginación completa. Filtra por ?level=suave si se pasa."""
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

    items = scan_all(table, **params)

    return build_response(200, {"items": items, "count": len(items)})


def get_random(query_params: dict):
    """Devuelve un reto aleatorio, opcionalmente filtrado por ?level=suave."""
    params: dict = {}

    if level := query_params.get("level"):
        if level not in VALID_LEVELS:
            return build_response(400, {"error": f"Nivel inválido: '{level}'"})
        params["FilterExpression"] = Attr("level").eq(level)

    items = scan_all(table, **params)

    if not items:
        return build_response(
            404,
            {"error": "No hay retos disponibles con los filtros aplicados"},
        )

    chosen = random.choice(items)
    log_event(logger, "🔵", "Reto aleatorio", item_id=chosen.get("id"))
    return build_response(200, chosen)


def create_item(data: dict):
    _validate(data)
    item = _normalize(data)
    log_event(logger, "🟢", "Reto creado", item_id=item["id"])
    table.put_item(Item=item)
    return build_response(201, {"message": "Reto creado con éxito", "id": item["id"]})


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
                errors.append({"index": i, "error": str(e)})

    status = 207 if errors else 201
    return build_response(status, {
        "message": f"{len(created)} retos creados",
        "created": created,
        "errors":  errors,
    })


def update_item(item_id: str, data: dict):
    """Usa UpdateItem — solo modifica los campos enviados, no sobrescribe el item."""
    update_fields = {k: v for k, v in data.items() if k != "id" and k != "createdAt"}
    if not update_fields:
        return build_response(400, {"error": "No hay campos para actualizar"})

    # Validar level si se está actualizando
    if "level" in update_fields and update_fields["level"] not in VALID_LEVELS:
        return build_response(
            400,
            {"error": f"Nivel inválido: '{update_fields['level']}'. Válidos: {', '.join(VALID_LEVELS)}"},
        )

    # Agregar timestamp de actualización
    update_fields["updatedAt"] = datetime.utcnow().isoformat() + "Z"

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
            return build_response(404, {"error": f"Reto '{item_id}' no encontrado"})
        raise

    return build_response(200, {"message": "Reto actualizado", "id": item_id})


def delete_item(item_id: str):
    existing = table.get_item(Key={"id": item_id})
    if "Item" not in existing:
        return build_response(404, {"error": f"Reto '{item_id}' no encontrado"})

    table.delete_item(Key={"id": item_id})
    return build_response(200, {"message": "Reto eliminado con éxito", "id": item_id})


# ─── Validación y Normalización ───────────────────────────────────────────────

def _validate(data: dict):
    missing = REQUIRED_FIELDS - data.keys()
    if missing:
        raise ValueError(f"Campos requeridos faltantes: {missing}")

    if not isinstance(data.get("text"), str) or not data["text"].strip():
        raise ValueError("El campo 'text' no puede estar vacío")

    if not isinstance(data.get("emoji"), str) or not data["emoji"].strip():
        raise ValueError("El campo 'emoji' no puede estar vacío")

    if data.get("level") not in VALID_LEVELS:
        raise ValueError(
            f"Nivel inválido: '{data.get('level')}'. Debe ser uno de: {', '.join(VALID_LEVELS)}"
        )


def _normalize(data: dict) -> dict:
    """Construye el item completo con valores por defecto."""
    now = datetime.utcnow().isoformat() + "Z"
    return {
        "id":        data.get("id") or str(uuid.uuid4()),
        "text":      data["text"].strip(),
        "emoji":     data["emoji"].strip(),
        "level":     data["level"],          # suave | picante | atrevido
        "createdAt": data.get("createdAt", now),
        "updatedAt": data.get("updatedAt", now),
    }
