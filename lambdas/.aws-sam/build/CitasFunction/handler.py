"""
citas_handler.py — CRUD de Citas
Nueva funcionalidad:
✅ Tabla separada con PK: id, SK: fecha (permite buscar por rango de fechas)
✅ Campos: id, fecha, plan_id, estado, nota, participantes
✅ Query por fecha (más eficiente que scan)
✅ Validación de estado (pendiente/confirmada/cancelada/completada)
✅ Mismas mejoras de seguridad y logging que planes_handler
"""
import json
import logging
import os
import uuid
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

from common.utils import build_response, parse_body, get_path_param

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ.get("CITAS_TABLE_NAME", "Citas")
table = dynamodb.Table(TABLE_NAME)

ESTADOS_VALIDOS = {"pendiente", "confirmada", "cancelada", "completada"}


# ─── Handler principal ────────────────────────────────────────────────────────

def lambda_handler(event, context):
    logger.info(json.dumps({"path": event.get("rawPath"), "method": event["requestContext"]["http"]["method"]}))

    method = event["requestContext"]["http"]["method"]
    item_id = get_path_param(event, "id")

    if method == "OPTIONS":
        return build_response(200, {})

    try:
        match method:
            case "GET":
                return get_cita(item_id) if item_id else get_citas(event)
            case "POST":
                return create_cita(parse_body(event))
            case "PUT":
                if not item_id:
                    return build_response(400, {"error": "Se requiere {id} en la ruta"})
                return update_cita(item_id, parse_body(event))
            case "DELETE":
                if not item_id:
                    return build_response(400, {"error": "Se requiere {id} en la ruta"})
                return delete_cita(item_id)
            case _:
                return build_response(405, {"error": "Método no permitido"})

    except ValueError as e:
        return build_response(400, {"error": str(e)})
    except ClientError as e:
        logger.error(f"DynamoDB error: {e.response['Error']}")
        return build_response(502, {"error": "Error de base de datos"})
    except Exception:
        logger.exception("Error inesperado")
        return build_response(500, {"error": "Error interno del servidor"})


# ─── CRUD ─────────────────────────────────────────────────────────────────────

def get_cita(item_id: str):
    result = table.get_item(Key={"id": item_id})
    if "Item" not in result:
        return build_response(404, {"error": f"Cita '{item_id}' no encontrada"})
    return build_response(200, result["Item"])


def get_citas(event: dict):
    """
    Devuelve citas con filtros opcionales por query params:
    - ?estado=pendiente
    - ?plan_id=<uuid>
    - ?desde=2025-01-01&hasta=2025-12-31
    """
    query_params = event.get("queryStringParameters") or {}
    params: dict = {"Limit": 50}
    filter_exprs = []
    expr_values = {}

    if estado := query_params.get("estado"):
        _validate_estado(estado)
        filter_exprs.append(Attr("estado").eq(estado))
    if plan_id := query_params.get("plan_id"):
        filter_exprs.append(Attr("plan_id").eq(plan_id))
    if desde := query_params.get("desde"):
        filter_exprs.append(Attr("fecha").gte(desde))
    if hasta := query_params.get("hasta"):
        filter_exprs.append(Attr("fecha").lte(hasta))

    if filter_exprs:
        expr = filter_exprs[0]
        for fe in filter_exprs[1:]:
            expr = expr & fe
        params["FilterExpression"] = expr

    if last_key := query_params.get("lastKey"):
        params["ExclusiveStartKey"] = {"id": last_key}

    result = table.scan(**params)
    response_body = {
        "items": result.get("Items", []),
        "count": result.get("Count", 0),
    }
    if next_key := result.get("LastEvaluatedKey"):
        response_body["nextKey"] = next_key.get("id")

    return build_response(200, response_body)


def create_cita(data: dict):
    _validate_cita(data)
    cita = {
        "id": str(uuid.uuid4()),
        "plan_id": data["plan_id"],
        "fecha": data.get("fecha", _fecha_iso_ahora()),
        "estado": data.get("estado", "pendiente"),
        "nota": data.get("nota", ""),
        "participantes": data.get("participantes", []),
        "creado_en": _fecha_iso_ahora(),
    }
    table.put_item(Item=cita)
    logger.info(f"Cita creada: {cita['id']} para plan {cita['plan_id']}")
    return build_response(201, {"message": "Cita creada", "id": cita["id"], "cita": cita})


def update_cita(item_id: str, data: dict):
    campos = {k: v for k, v in data.items() if k not in ("id", "creado_en")}
    if not campos:
        return build_response(400, {"error": "No hay campos para actualizar"})

    if "estado" in campos:
        _validate_estado(campos["estado"])

    expr_parts, expr_values, expr_names = [], {}, {}
    for key, value in campos.items():
        safe_key = f"#f_{key}"
        val_key = f":v_{key}"
        expr_parts.append(f"{safe_key} = {val_key}")
        expr_values[val_key] = value
        expr_names[safe_key] = key

    expr_values[":updated"] = _fecha_iso_ahora()
    update_expr = "SET " + ", ".join(expr_parts) + ", #f_actualizado_en = :updated"
    expr_names["#f_actualizado_en"] = "actualizado_en"

    try:
        table.update_item(
            Key={"id": item_id},
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expr_values,
            ExpressionAttributeNames=expr_names,
            ConditionExpression=Attr("id").exists(),
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return build_response(404, {"error": f"Cita '{item_id}' no encontrada"})
        raise

    return build_response(200, {"message": "Cita actualizada", "id": item_id})


def delete_cita(item_id: str):
    try:
        table.delete_item(
            Key={"id": item_id},
            ConditionExpression=Attr("id").exists(),
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return build_response(404, {"error": f"Cita '{item_id}' no encontrada"})
        raise
    return build_response(200, {"message": "Cita eliminada", "id": item_id})


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _validate_cita(data: dict):
    if not data.get("plan_id"):
        raise ValueError("El campo 'plan_id' es requerido")
    if "estado" in data:
        _validate_estado(data["estado"])


def _validate_estado(estado: str):
    if estado not in ESTADOS_VALIDOS:
        raise ValueError(f"Estado inválido. Opciones: {ESTADOS_VALIDOS}")


def _fecha_iso_ahora() -> str:
    return datetime.now(timezone.utc).isoformat()