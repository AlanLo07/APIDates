"""
random_handler.py — Generador de Planes Aleatorios
Funcionalidad:
✅ Lee el catálogo de Planes de DynamoDB
✅ Filtra por typeLocation si se pasa ?tipo=restaurante
✅ Excluye planes ya visitados si ?soloNuevos=true
✅ Genera una Cita sugerida para 7 días en el futuro
✅ Opcionalmente la guarda en Citas con estado "sugerida"
"""
import json
import logging
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

from common.utils import build_response, DecimalEncoder

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
PLANES_TABLE = os.environ.get("TABLE_NAME", "Planes")
CITAS_TABLE = os.environ.get("CITAS_TABLE_NAME", "Citas")

planes_table = dynamodb.Table(PLANES_TABLE)
citas_table = dynamodb.Table(CITAS_TABLE)


def lambda_handler(event, context):
    if event["requestContext"]["http"]["method"] == "OPTIONS":
        return build_response(200, {})

    query_params = event.get("queryStringParameters") or {}
    tipo = query_params.get("tipo")               # Ej: "restaurante", "parque"
    solo_nuevos = query_params.get("soloNuevos", "false").lower() == "true"
    guardar_cita = query_params.get("guardarCita", "true").lower() == "true"

    try:
        # 1. Obtener planes del catálogo
        planes = _fetch_planes(tipo=tipo, solo_nuevos=solo_nuevos)

        if not planes:
            return build_response(404, {
                "error": "No hay planes disponibles con los filtros aplicados",
                "filtros": {"tipo": tipo, "soloNuevos": solo_nuevos},
            })

        # 2. Elegir plan aleatorio
        plan_elegido = random.choice(planes)
        logger.info(f"Plan elegido: {plan_elegido.get('id')} — {plan_elegido.get('nombre')}")

        # 3. Generar fecha sugerida (+7 días, hora del mediodía UTC)
        fecha_sugerida = (
            datetime.now(timezone.utc) + timedelta(days=7)
        ).replace(hour=12, minute=0, second=0, microsecond=0).isoformat()

        # 4. Guardar cita sugerida en DynamoDB (opcional)
        cita_id = None
        if guardar_cita:
            cita_id = _crear_cita_sugerida(plan_elegido["id"], fecha_sugerida)

        return build_response(200, {
            "plan": plan_elegido,
            "fecha_sugerida": fecha_sugerida,
            "cita_id": cita_id,
            "message": "¡Plan aleatorio generado! 🎉",
        })

    except ClientError as e:
        logger.error(f"DynamoDB error: {e.response['Error']}")
        return build_response(502, {"error": "Error de base de datos"})
    except Exception:
        logger.exception("Error inesperado en random-plan")
        return build_response(500, {"error": "Error interno del servidor"})


def _fetch_planes(tipo: str | None, solo_nuevos: bool) -> list:
    """Escanea planes con filtros. Para catálogos pequeños (<1000 items) esto es suficiente."""
    from boto3.dynamodb.conditions import Attr

    filter_exprs = []

    if tipo:
        filter_exprs.append(Attr("typeLocation").eq(tipo))
    if solo_nuevos:
        filter_exprs.append(Attr("isVisited").eq(False))

    params: dict = {}
    if filter_exprs:
        expr = filter_exprs[0]
        for fe in filter_exprs[1:]:
            expr = expr & fe
        params["FilterExpression"] = expr

    # Maneja paginación automáticamente (por si hay muchos planes)
    items = []
    while True:
        result = planes_table.scan(**params)
        items.extend(result.get("Items", []))
        last_key = result.get("LastEvaluatedKey")
        if not last_key:
            break
        params["ExclusiveStartKey"] = last_key

    return items


def _crear_cita_sugerida(plan_id: str, fecha: str) -> str:
    """Crea una cita con estado 'sugerida' en la tabla Citas."""
    cita_id = str(uuid.uuid4())
    citas_table.put_item(Item={
        "id": cita_id,
        "plan_id": plan_id,
        "fecha": fecha,
        "estado": "sugerida",
        "nota": "Generado automáticamente",
        "participantes": [],
        "creado_en": datetime.now(timezone.utc).isoformat(),
    })
    return cita_id