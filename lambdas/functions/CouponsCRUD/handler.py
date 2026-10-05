"""CRUD de cuponeras de regalos por usuario y reinicio semestral programado."""
import json
import logging
import os
from datetime import datetime, timezone
from urllib.parse import unquote

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

from common.utils import build_response, get_path_param, parse_body

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ.get("COUPONS_TABLE_NAME", "CouponsTable")
table = dynamodb.Table(TABLE_NAME)

GIFT_FIELDS = {"nombre", "descripcion", "canjeado"}


def lambda_handler(event, context):
    if _is_scheduled_event(event):
        return reset_all_coupons()

    method = event.get("requestContext", {}).get("http", {}).get("method", "")
    raw_path = event.get("rawPath", "")
    gift_name = get_path_param(event, "nombre")
    if gift_name:
        gift_name = unquote(gift_name)

    logger.info(
        json.dumps(
            {
                "level": "⚪️",
                "message": "Solicitud CouponsCRUD",
                "method": method,
                "path": raw_path,
                "has_gift_name": bool(gift_name),
                "function": getattr(context, "function_name", "unknown"),
            },
            ensure_ascii=False,
        )
    )

    if method == "OPTIONS":
        return build_response(200, {})

    try:
        user_id = _require_user_id(event)

        if "/regalos" not in raw_path:
            return _handle_coupon_book(method, user_id, event)

        if not gift_name:
            if method == "GET":
                return list_gifts(user_id)
            if method == "POST":
                return create_gift(user_id, parse_body(event))
            return build_response(405, {"error": f"Método {method} no permitido"})

        if method == "PATCH" and raw_path.endswith("/canjear"):
            data = parse_body(event)
            redeemed = data.get("canjeado", True) if isinstance(data, dict) else True
            return set_gift_redeemed(user_id, gift_name, redeemed)

        match method:
            case "GET":
                return get_gift(user_id, gift_name)
            case "PUT":
                return update_gift(user_id, gift_name, parse_body(event))
            case "DELETE":
                return delete_gift(user_id, gift_name)
            case _:
                return build_response(405, {"error": f"Método {method} no permitido"})

    except PermissionError as exc:
        logger.warning(json.dumps({"level": "🟡", "message": str(exc)}, ensure_ascii=False))
        return build_response(401, {"error": str(exc)})
    except ValueError as exc:
        logger.warning(json.dumps({"level": "🟡", "message": str(exc)}, ensure_ascii=False))
        return build_response(400, {"error": str(exc)})
    except ClientError as exc:
        logger.error(
            json.dumps(
                {
                    "level": "🔴",
                    "message": "Error DynamoDB en CouponsCRUD",
                    "code": exc.response.get("Error", {}).get("Code", "Unknown"),
                },
                ensure_ascii=False,
            )
        )
        return build_response(502, {"error": "Error de base de datos"})
    except Exception:
        logger.exception("🔴 Error inesperado en CouponsCRUD")
        return build_response(500, {"error": "Error interno del servidor"})


def _handle_coupon_book(method: str, user_id: str, event: dict):
    match method:
        case "GET":
            return get_coupon_book(user_id)
        case "POST":
            return create_coupon_book(user_id, parse_body(event))
        case "PUT":
            return replace_coupon_book(user_id, parse_body(event))
        case "DELETE":
            return delete_coupon_book(user_id)
        case _:
            return build_response(405, {"error": f"Método {method} no permitido"})


def get_coupon_book(user_id: str):
    coupon_book = _load_coupon_book(user_id)
    if coupon_book is None:
        return build_response(404, {"error": "Cuponera no encontrada"})
    return build_response(200, _public_coupon_book(coupon_book))


def create_coupon_book(user_id: str, data: dict):
    gifts = _validate_coupon_book_payload(data)
    timestamp = _utc_now()
    item = {
        "userId": user_id,
        "regalos": gifts,
        "createdAt": timestamp,
        "updatedAt": timestamp,
    }
    try:
        table.put_item(Item=item, ConditionExpression=Attr("userId").not_exists())
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return build_response(409, {"error": "El usuario ya tiene una cuponera"})
        raise

    _log_success("Cuponera creada", user_id, gift_count=len(gifts))
    return build_response(201, _public_coupon_book(item))


def replace_coupon_book(user_id: str, data: dict):
    gifts = _validate_coupon_book_payload(data)
    try:
        result = table.update_item(
            Key={"userId": user_id},
            UpdateExpression="SET regalos = :g, updatedAt = :u",
            ExpressionAttributeValues={":g": gifts, ":u": _utc_now()},
            ConditionExpression=Attr("userId").exists(),
            ReturnValues="ALL_NEW",
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return build_response(404, {"error": "Cuponera no encontrada"})
        raise
    _log_success("Cuponera reemplazada", user_id, gift_count=len(gifts))
    return build_response(200, _public_coupon_book(result["Attributes"]))


def delete_coupon_book(user_id: str):
    try:
        table.delete_item(
            Key={"userId": user_id},
            ConditionExpression=Attr("userId").exists(),
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return build_response(404, {"error": "Cuponera no encontrada"})
        raise
    _log_success("Cuponera eliminada", user_id)
    return build_response(200, {"message": "Cuponera eliminada"})


def list_gifts(user_id: str):
    coupon_book = _load_coupon_book(user_id)
    if coupon_book is None:
        return build_response(404, {"error": "Cuponera no encontrada"})
    gifts = coupon_book.get("regalos", [])
    return build_response(200, {"items": gifts, "count": len(gifts)})


def get_gift(user_id: str, gift_name: str):
    coupon_book = _load_coupon_book(user_id)
    if coupon_book is None:
        return build_response(404, {"error": "Cuponera no encontrada"})
    gift = _find_gift(coupon_book["regalos"], gift_name)
    if gift is None:
        return build_response(404, {"error": f"Regalo '{gift_name}' no encontrado"})
    return build_response(200, gift)


def create_gift(user_id: str, data: dict):
    gift = _validate_gift(data)
    coupon_book = _load_coupon_book(user_id)
    if coupon_book is None:
        return build_response(404, {"error": "Cuponera no encontrada"})
    gifts = coupon_book.get("regalos", [])
    if _find_gift(gifts, gift["nombre"]):
        return build_response(409, {"error": "Ya existe un regalo con ese nombre"})
    gifts.append(gift)
    _save_gifts(user_id, gifts)
    _log_success("Regalo creado", user_id, gift_name=gift["nombre"])
    return build_response(201, gift)


def update_gift(user_id: str, gift_name: str, data: dict):
    gift = _validate_gift(data)
    coupon_book = _load_coupon_book(user_id)
    if coupon_book is None:
        return build_response(404, {"error": "Cuponera no encontrada"})
    gifts = coupon_book.get("regalos", [])
    index = _find_gift_index(gifts, gift_name)
    if index is None:
        return build_response(404, {"error": f"Regalo '{gift_name}' no encontrado"})
    duplicate = _find_gift_index(gifts, gift["nombre"])
    if duplicate is not None and duplicate != index:
        return build_response(409, {"error": "Ya existe un regalo con ese nombre"})
    gifts[index] = gift
    _save_gifts(user_id, gifts)
    _log_success("Regalo actualizado", user_id, gift_name=gift["nombre"])
    return build_response(200, gift)


def delete_gift(user_id: str, gift_name: str):
    coupon_book = _load_coupon_book(user_id)
    if coupon_book is None:
        return build_response(404, {"error": "Cuponera no encontrada"})
    gifts = coupon_book.get("regalos", [])
    index = _find_gift_index(gifts, gift_name)
    if index is None:
        return build_response(404, {"error": f"Regalo '{gift_name}' no encontrado"})
    gifts.pop(index)
    _save_gifts(user_id, gifts)
    _log_success("Regalo eliminado", user_id, gift_name=gift_name)
    return build_response(200, {"message": "Regalo eliminado", "nombre": gift_name})


def set_gift_redeemed(user_id: str, gift_name: str, redeemed):
    if not isinstance(redeemed, bool):
        raise ValueError("El campo 'canjeado' debe ser booleano")
    coupon_book = _load_coupon_book(user_id)
    if coupon_book is None:
        return build_response(404, {"error": "Cuponera no encontrada"})
    gifts = coupon_book.get("regalos", [])
    index = _find_gift_index(gifts, gift_name)
    if index is None:
        return build_response(404, {"error": f"Regalo '{gift_name}' no encontrado"})
    gifts[index]["canjeado"] = redeemed
    _save_gifts(user_id, gifts)
    _log_success("Estado de regalo actualizado", user_id, gift_name=gift_name, redeemed=redeemed)
    return build_response(200, gifts[index])


def reset_all_coupons():
    updated_books = 0
    updated_gifts = 0
    scan_request = {"ProjectionExpression": "userId, regalos"}

    while True:
        result = table.scan(**scan_request)
        for coupon_book in result.get("Items", []):
            gifts = coupon_book.get("regalos", [])
            redeemed_count = sum(1 for gift in gifts if gift.get("canjeado", False))
            if not redeemed_count:
                continue
            for gift in gifts:
                gift["canjeado"] = False
            _save_gifts(coupon_book["userId"], gifts)
            updated_books += 1
            updated_gifts += redeemed_count

        last_key = result.get("LastEvaluatedKey")
        if not last_key:
            break
        scan_request["ExclusiveStartKey"] = last_key

    logger.info(
        json.dumps(
            {
                "level": "🔵",
                "message": "Reinicio semestral de cuponeras completado",
                "updated_books": updated_books,
                "updated_gifts": updated_gifts,
            },
            ensure_ascii=False,
        )
    )
    return {"updatedBooks": updated_books, "updatedGifts": updated_gifts}


def _validate_coupon_book_payload(data) -> list[dict]:
    if not isinstance(data, dict):
        raise ValueError("El body debe ser un objeto JSON")
    gifts = data.get("regalos", [])
    if not isinstance(gifts, list):
        raise ValueError("El campo 'regalos' debe ser una lista")
    normalized = [_validate_gift(gift) for gift in gifts]
    names = [gift["nombre"].casefold() for gift in normalized]
    if len(names) != len(set(names)):
        raise ValueError("Los nombres de los regalos no pueden repetirse")
    return normalized


def _validate_gift(data) -> dict:
    if not isinstance(data, dict):
        raise ValueError("El regalo debe ser un objeto JSON")
    extra_fields = set(data) - GIFT_FIELDS
    if extra_fields:
        raise ValueError(f"Campos no permitidos en regalo: {', '.join(sorted(extra_fields))}")
    name = data.get("nombre")
    description = data.get("descripcion")
    if not isinstance(name, str) or not name.strip():
        raise ValueError("El campo 'nombre' es requerido")
    if not isinstance(description, str) or not description.strip():
        raise ValueError("El campo 'descripcion' es requerido")
    redeemed = data.get("canjeado", False)
    if not isinstance(redeemed, bool):
        raise ValueError("El campo 'canjeado' debe ser booleano")
    return {"nombre": name.strip(), "descripcion": description.strip(), "canjeado": redeemed}


def _load_coupon_book(user_id: str):
    return table.get_item(Key={"userId": user_id}).get("Item")


def _save_gifts(user_id: str, gifts: list[dict]):
    table.update_item(
        Key={"userId": user_id},
        UpdateExpression="SET regalos = :g, updatedAt = :u",
        ExpressionAttributeValues={":g": gifts, ":u": _utc_now()},
        ConditionExpression=Attr("userId").exists(),
    )


def _find_gift(gifts: list[dict], gift_name: str):
    index = _find_gift_index(gifts, gift_name)
    return gifts[index] if index is not None else None


def _find_gift_index(gifts: list[dict], gift_name: str):
    normalized_name = gift_name.strip().casefold()
    return next(
        (index for index, gift in enumerate(gifts) if gift.get("nombre", "").casefold() == normalized_name),
        None,
    )


def _public_coupon_book(coupon_book: dict) -> dict:
    return {
        "regalos": coupon_book.get("regalos", []),
        "createdAt": coupon_book.get("createdAt"),
        "updatedAt": coupon_book.get("updatedAt"),
    }


def _require_user_id(event: dict) -> str:
    claims = (
        event.get("requestContext", {})
        .get("authorizer", {})
        .get("jwt", {})
        .get("claims", {})
    )
    user_id = (claims.get("sub") or "").strip()
    if not user_id:
        raise PermissionError("Usuario no autenticado")
    return user_id


def _is_scheduled_event(event: dict) -> bool:
    return event.get("source") == "aws.events" and event.get("detail-type") == "Scheduled Event"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _log_success(message: str, user_id: str, **metadata):
    logger.info(
        json.dumps(
            {
                "level": "🟢",
                "message": message,
                "user_ref": user_id[-8:],
                **metadata,
            },
            ensure_ascii=False,
        )
    )