"""
love_phrases_handler.py — CRUD de Frases de Amor
Funcionalidad:
✅ Modelo alineado con LovePhrase de Dart
✅ Tipos válidos: pelicula, cancion, libro, serie, pareja
✅ Validación de campos requeridos por tipo
✅ UUID autogenerado si no se provee id
✅ Paginación en scan
✅ Filtrado por ?type=cancion
✅ GET /random — devuelve una frase aleatoria
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
TABLE_NAME = os.environ.get("LOVE_PHRASES_TABLE_NAME", "LovePhrasesTable")
table = dynamodb.Table(TABLE_NAME)

# ─── Modelo ───────────────────────────────────────────────────────────────────

VALID_TYPES = {"pelicula", "cancion", "libro", "serie", "pareja"}

REQUIRED_FIELDS = {"text", "type", "title", "emoji"}


# ─── Handler principal ────────────────────────────────────────────────────────

def lambda_handler(event, context):
    logger.info(json.dumps({"event_keys": list(event.keys())}))

    method = event.get("requestContext", {}).get("http", {}).get("method", "")
    raw_path = event.get("rawPath", "")
    item_id = get_path_param(event, "id")
    query_params = event.get("queryStringParameters") or {}

    if method == "OPTIONS":
        return build_response(200, {})

    try:
        match method:
            case "GET":
                # GET /love-phrases/random — frase aleatoria
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
        return build_response(400, {"error": str(e)})
    except ClientError as e:
        logger.error(f"DynamoDB error: {e.response['Error']}")
        return build_response(502, {"error": "Error de base de datos"})
    except Exception:
        logger.exception("Error inesperado")
        return build_response(500, {"error": "Error interno del servidor"})


# ─── CRUD ─────────────────────────────────────────────────────────────────────

def get_item(item_id: str):
    result = table.get_item(Key={"id": item_id})
    if "Item" not in result:
        return build_response(404, {"error": f"Frase '{item_id}' no encontrada"})
    return build_response(200, result["Item"])


def get_all_items(query_params: dict):
    """Scan con paginación. Filtra por ?type=cancion si se pasa."""
    params: dict = {}

    if phrase_type := query_params.get("type"):
        if phrase_type not in VALID_TYPES:
            return build_response(400, {"error": f"Tipo inválido: '{phrase_type}'. Válidos: {', '.join(VALID_TYPES)}"})
        params["FilterExpression"] = Attr("type").eq(phrase_type)

    if last_key := query_params.get("lastKey"):
        params["ExclusiveStartKey"] = {"id": last_key}

    # Scan paginado
    items = []
    while True:
        result = table.scan(**params)
        items.extend(result.get("Items", []))
        last_evaluated = result.get("LastEvaluatedKey")
        if not last_evaluated:
            break
        params["ExclusiveStartKey"] = last_evaluated

    response_body = {
        "items": items,
        "count": len(items),
    }
    return build_response(200, response_body)


def get_random(query_params: dict):
    """Devuelve una frase aleatoria, opcionalmente filtrada por ?type=cancion."""
    params: dict = {}

    if phrase_type := query_params.get("type"):
        if phrase_type not in VALID_TYPES:
            return build_response(400, {"error": f"Tipo inválido: '{phrase_type}'"})
        params["FilterExpression"] = Attr("type").eq(phrase_type)

    # Scan completo para el random (tabla pequeña ~100 items)
    items = []
    while True:
        result = table.scan(**params)
        items.extend(result.get("Items", []))
        if not result.get("LastEvaluatedKey"):
            break
        params["ExclusiveStartKey"] = result["LastEvaluatedKey"]

    if not items:
        return build_response(404, {"error": "No hay frases disponibles con los filtros aplicados"})

    chosen = random.choice(items)
    logger.info(f"Frase aleatoria elegida: {chosen.get('id')} — {chosen.get('title')}")
    return build_response(200, chosen)


def create_item(data: dict):
    _validate(data)
    item = _normalize(data)
    logger.info(f"Creando frase: {item['id']} — {item['title']}")
    table.put_item(Item=item)
    return build_response(201, {"message": "Frase creada con éxito", "id": item["id"]})


def bulk_create(items: list):
    """Carga masiva usando batch_writer."""
    created = []
    errors = []

    with table.batch_writer() as batch:
        for i, item_data in enumerate(items):
            try:
                _validate(item_data)
                item = _normalize(item_data)
                batch.put_item(Item=item)
                created.append(item["id"])
            except ValueError as e:
                errors.append({"index": i, "text": item_data.get("text", "?"), "error": str(e)})

    status = 207 if errors else 201
    return build_response(status, {
        "message": f"{len(created)} frases creadas",
        "created": created,
        "errors": errors,
    })


def update_item(item_id: str, data: dict):
    # Verificar que existe
    existing = table.get_item(Key={"id": item_id})
    if "Item" not in existing:
        return build_response(404, {"error": f"Frase '{item_id}' no encontrada"})

    data["id"] = item_id
    _validate(data)
    item = _normalize(data)
    table.put_item(Item=item)
    return build_response(200, {"message": "Frase actualizada con éxito", "id": item_id})


def delete_item(item_id: str):
    existing = table.get_item(Key={"id": item_id})
    if "Item" not in existing:
        return build_response(404, {"error": f"Frase '{item_id}' no encontrada"})

    table.delete_item(Key={"id": item_id})
    return build_response(200, {"message": "Frase eliminada con éxito", "id": item_id})


# ─── Validación y Normalización ───────────────────────────────────────────────

def _validate(data: dict):
    missing = REQUIRED_FIELDS - data.keys()
    if missing:
        raise ValueError(f"Campos requeridos faltantes: {missing}")

    if not isinstance(data.get("text"), str) or not data["text"].strip():
        raise ValueError("El campo 'text' no puede estar vacío")

    if data.get("type") not in VALID_TYPES:
        raise ValueError(f"Tipo inválido: '{data.get('type')}'. Válidos: {', '.join(VALID_TYPES)}")

    if not isinstance(data.get("title"), str) or not data["title"].strip():
        raise ValueError("El campo 'title' no puede estar vacío")


def _normalize(data: dict) -> dict:
    """Construye el item completo con valores por defecto."""
    return {
        "id":      data.get("id") or str(uuid.uuid4()),
        "text":    data["text"].strip().upper(),   # Siempre en mayúsculas (como en Flutter)
        "type":    data["type"],
        "title":   data["title"].strip(),
        "minute":  data.get("minute", "").strip(),
        "credits": data.get("credits", "").strip(),
        "emoji":   data.get("emoji", "💬").strip(),
        "link":    data.get("link", "").strip(),
    }