"""
checklists_handler.py — CRUD de checklists genéricos (supermercado, viaje,
lista de deseos/compras y checklists personalizados).

Modelo:
- Una sola tabla DynamoDB con PK/SK compuesta por checklist.
- Item raíz del checklist: pk=CHECKLIST#{checklistId}, sk=META
- Colecciones hijas: GRUPO (departamentos/categorías), ITEM (elementos)

Rutas principales:
- GET/POST /checklists
- POST /checklists/seed-defaults
- GET/PUT/DELETE /checklists/{checklistId}
- PATCH /checklists/{checklistId}/reset
- GET/POST /checklists/{checklistId}/{collection}          (grupos | items)
- GET/PUT/DELETE /checklists/{checklistId}/{collection}/{itemId}
- PATCH /checklists/{checklistId}/items/{itemId}/comprado
"""
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

from common.utils import build_response, get_path_param, parse_body, query_all  # type: ignore

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ.get("CHECKLISTS_TABLE_NAME", "ChecklistsTable")
table = dynamodb.Table(TABLE_NAME)  # type: ignore

VALID_KINDS = {"supermercado", "viaje", "deseos", "personalizado"}
VALID_PRIORITIES = {"alta", "media", "baja"}

KIND_DEFAULTS = {
    "supermercado": {"emoji": "🛒", "colorValue": 0xFF4CAF50, "usaGrupos": True},
    "viaje": {"emoji": "🧳", "colorValue": 0xFFFF8A65, "usaGrupos": True},
    "deseos": {"emoji": "⭐", "colorValue": 0xFFAB47BC, "usaGrupos": False},
    "personalizado": {"emoji": "📋", "colorValue": 0xFF6A88D6, "usaGrupos": False},
}

DEFAULT_GROUPS_BY_KIND = {
    "supermercado": [
        {"nombre": "Frutas y verduras", "emoji": "🥦"},
        {"nombre": "Lácteos", "emoji": "🥛"},
        {"nombre": "Carnes y pescados", "emoji": "🍗"},
        {"nombre": "Panadería", "emoji": "🍞"},
        {"nombre": "Despensa", "emoji": "🥫"},
        {"nombre": "Bebidas", "emoji": "🥤"},
        {"nombre": "Congelados", "emoji": "🧊"},
        {"nombre": "Limpieza", "emoji": "🧴"},
        {"nombre": "Cuidado personal", "emoji": "🧼"},
        {"nombre": "Otros", "emoji": "📦"},
    ],
    "viaje": [
        {"nombre": "Ropa", "emoji": "👕"},
        {"nombre": "Documentos", "emoji": "🛂"},
        {"nombre": "Dinero y tarjetas", "emoji": "💳"},
        {"nombre": "Cosméticos y aseo", "emoji": "🧴"},
        {"nombre": "Electrónica", "emoji": "🔌"},
        {"nombre": "Salud", "emoji": "💊"},
        {"nombre": "Otros", "emoji": "🎒"},
    ],
}

# Checklists principales, creadas una sola vez vía POST /checklists/seed-defaults
DEFAULT_CHECKLISTS = [
    {"id": "default-supermercado", "titulo": "Supermercado", "kind": "supermercado"},
    {"id": "default-viaje", "titulo": "Viaje", "kind": "viaje"},
    {"id": "default-deseos", "titulo": "Cosas por comprar", "kind": "deseos"},
]

COLLECTIONS = {
    "grupos": {
        "prefix": "GRUPO",
        "entity_type": "checklist_grupo",
        "required": {"nombre"},
        "sort_field": "orden",
    },
    "items": {
        "prefix": "ITEM",
        "entity_type": "checklist_item",
        "required": {"nombre"},
        "sort_field": "nombre",
    },
}


def lambda_handler(event, context):
    method = event.get("requestContext", {}).get("http", {}).get("method", "")
    raw_path = event.get("rawPath", "")
    checklist_id = get_path_param(event, "checklistId")
    collection = get_path_param(event, "collection")
    item_id = get_path_param(event, "itemId")

    logger.info(
        json.dumps(
            {
                "level": "🔵",
                "message": "Solicitud ChecklistsCRUD",
                "method": method,
                "path": raw_path,
                "has_checklist_id": bool(checklist_id),
                "collection": collection,
                "has_item_id": bool(item_id),
                "function": getattr(context, "function_name", "unknown"),
            }
        )
    )

    if method == "OPTIONS":
        return build_response(200, {})

    try:
        if method == "GET" and not checklist_id:
            return list_checklists()

        if method == "POST" and not checklist_id and raw_path.endswith("/seed-defaults"):
            return seed_default_checklists()

        if method == "POST" and not checklist_id:
            return create_checklist(parse_body(event))  # type: ignore

        if not checklist_id:
            return build_response(400, {"error": "Se requiere {checklistId} en la ruta"})

        if method == "PATCH" and not collection and raw_path.endswith("/reset"):
            return reset_checklist(checklist_id)

        if method == "PATCH" and item_id and raw_path.endswith("/comprado"):
            return update_item_comprado(checklist_id, item_id, parse_body(event))  # type: ignore

        if not collection:
            match method:
                case "GET":
                    return get_checklist(checklist_id)
                case "PUT":
                    return update_checklist(checklist_id, parse_body(event))  # type: ignore
                case "DELETE":
                    return delete_checklist(checklist_id)
                case _:
                    return build_response(405, {"error": f"Método {method} no permitido"})

        if collection not in COLLECTIONS:
            return build_response(
                400,
                {
                    "error": (
                        f"Colección inválida: '{collection}'. "
                        f"Opciones: {', '.join(sorted(COLLECTIONS))}"
                    )
                },
            )

        if not item_id:
            match method:
                case "GET":
                    return list_collection_items(checklist_id, collection)
                case "POST":
                    return create_collection_item(checklist_id, collection, parse_body(event))  # type: ignore
                case _:
                    return build_response(405, {"error": f"Método {method} no permitido"})

        match method:
            case "GET":
                return get_collection_item(checklist_id, collection, item_id)
            case "PUT":
                return update_collection_item(checklist_id, collection, item_id, parse_body(event))  # type: ignore
            case "DELETE":
                return delete_collection_item(checklist_id, collection, item_id)
            case _:
                return build_response(405, {"error": f"Método {method} no permitido"})

    except ValueError as exc:
        logger.warning(json.dumps({"level": "🟡", "message": str(exc)}))
        return build_response(400, {"error": str(exc)})
    except ClientError as exc:
        logger.error(
            json.dumps(
                {
                    "level": "🔴",
                    "message": "Error DynamoDB en ChecklistsCRUD",
                    "code": exc.response.get("Error", {}).get("Code", "Unknown"),
                }
            )
        )
        return build_response(502, {"error": "Error de base de datos"})
    except Exception:
        logger.exception("🔴 Error inesperado en ChecklistsCRUD")
        return build_response(500, {"error": "Error interno del servidor"})


# ─── Checklists (boards) ──────────────────────────────────────────────────────


def list_checklists():
    items = query_all(
        table,
        IndexName="entityType-index",
        KeyConditionExpression=Key("entityType").eq("checklist_board") & Key("sk").begins_with("META"),
    )
    items.sort(key=lambda item: item.get("createdAt", ""))
    return build_response(
        200,
        {"items": [_serialize(item) for item in items], "count": len(items)},
    )


def seed_default_checklists():
    created = []
    skipped = []
    for default in DEFAULT_CHECKLISTS:
        try:
            create_checklist(
                {"id": default["id"], "titulo": default["titulo"], "kind": default["kind"]},
                is_default=True,
            )
            created.append(default["id"])
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                skipped.append(default["id"])
            else:
                raise
    logger.info(
        json.dumps({"level": "🟢", "message": "Seed de checklists por defecto", "created": created, "skipped": skipped})
    )
    return build_response(200, {"message": "Checklists por defecto verificados", "created": created, "skipped": skipped})


def create_checklist(data: dict, is_default: bool = False):
    if not isinstance(data, dict):
        raise ValueError("El body debe ser un objeto JSON")
    if not _clean_str(data.get("titulo")):
        raise ValueError("El campo 'titulo' es requerido")

    kind = data.get("kind") or "personalizado"
    if kind not in VALID_KINDS:
        raise ValueError(f"'kind' inválido. Opciones: {', '.join(sorted(VALID_KINDS))}")

    defaults = KIND_DEFAULTS[kind]
    checklist_id = data.get("id") or data.get("checklistId") or str(uuid.uuid4())
    usa_grupos = bool(data["usaGrupos"]) if "usaGrupos" in data else defaults["usaGrupos"]
    timestamp = _utc_now()

    item = {
        "pk": _pk(checklist_id),
        "sk": "META",
        "id": checklist_id,
        "checklistId": checklist_id,
        "entityType": "checklist_board",
        "titulo": _clean_str(data.get("titulo")),
        "kind": kind,
        "emoji": _clean_str(data.get("emoji")) or defaults["emoji"],
        "colorValue": _to_int(data.get("colorValue", defaults["colorValue"]), "colorValue"),
        "usaGrupos": usa_grupos,
        "isDefault": bool(is_default),
        "createdAt": timestamp,
        "updatedAt": timestamp,
    }

    table.put_item(
        Item=item,
        ConditionExpression="attribute_not_exists(pk) AND attribute_not_exists(sk)",
    )

    grupos_creadas = []
    grupos_payload = data.get("grupos")
    if not isinstance(grupos_payload, list) or not grupos_payload:
        grupos_payload = DEFAULT_GROUPS_BY_KIND.get(kind, []) if usa_grupos else []

    for orden, grupo_data in enumerate(grupos_payload):
        grupo_item = _create_group_item(checklist_id, grupo_data, orden)
        grupos_creadas.append(grupo_item["id"])

    logger.info(
        json.dumps(
            {
                "level": "🟢",
                "message": "Checklist creado",
                "checklistId": checklist_id,
                "kind": kind,
                "gruposCreados": len(grupos_creadas),
            }
        )
    )
    return build_response(
        201, {"message": "Checklist creado", "checklistId": checklist_id, "grupos": grupos_creadas}
    )


def get_checklist(checklist_id: str):
    result = table.get_item(Key={"pk": _pk(checklist_id), "sk": "META"})
    if "Item" not in result:
        return build_response(404, {"error": f"Checklist '{checklist_id}' no encontrado"})

    board = result["Item"]
    grupos = _sort_items("grupos", _query_partition(_pk(checklist_id), "GRUPO#"))
    items = _sort_items("items", _query_partition(_pk(checklist_id), "ITEM#"))

    return build_response(
        200,
        {
            "checklist": _serialize(board),
            "grupos": [_serialize(g) for g in grupos],
            "items": [_serialize(i) for i in items],
            "resumen": _build_summary(items),
        },
    )


def update_checklist(checklist_id: str, data: dict):
    if not isinstance(data, dict):
        raise ValueError("El body debe ser un objeto JSON")
    if "titulo" in data and not _clean_str(data.get("titulo")):
        raise ValueError("El campo 'titulo' no puede estar vacío")
    if "kind" in data and data["kind"] not in VALID_KINDS:
        raise ValueError(f"'kind' inválido. Opciones: {', '.join(sorted(VALID_KINDS))}")

    update_fields = {
        key: value
        for key, value in data.items()
        if key not in {"pk", "sk", "id", "checklistId", "entityType", "createdAt", "grupos"}
    }
    if "colorValue" in update_fields:
        update_fields["colorValue"] = _to_int(update_fields["colorValue"], "colorValue")
    if "usaGrupos" in update_fields:
        update_fields["usaGrupos"] = bool(update_fields["usaGrupos"])
    if not update_fields:
        return build_response(400, {"error": "No hay campos para actualizar"})

    update_fields["updatedAt"] = _utc_now()
    return _run_update(
        key={"pk": _pk(checklist_id), "sk": "META"},
        update_fields=update_fields,
        not_found_message=f"Checklist '{checklist_id}' no encontrado",
    )


def delete_checklist(checklist_id: str):
    partition_key = _pk(checklist_id)
    items = _query_partition(partition_key)
    if not items:
        return build_response(404, {"error": f"Checklist '{checklist_id}' no encontrado"})

    with table.batch_writer() as batch:
        for item in items:
            batch.delete_item(Key={"pk": item["pk"], "sk": item["sk"]})

    logger.info(
        json.dumps({"level": "🟢", "message": "Checklist eliminado", "checklistId": checklist_id, "items": len(items)})
    )
    return build_response(200, {"message": "Checklist eliminado", "checklistId": checklist_id, "itemsEliminados": len(items)})


def reset_checklist(checklist_id: str):
    if not _checklist_exists(checklist_id):
        return build_response(404, {"error": f"Checklist '{checklist_id}' no encontrado"})

    items = _query_partition(_pk(checklist_id), "ITEM#")
    now = _utc_now()
    for item in items:
        table.update_item(
            Key={"pk": item["pk"], "sk": item["sk"]},
            UpdateExpression="SET comprado = :c, updatedAt = :u",
            ExpressionAttributeValues={":c": False, ":u": now},
        )

    logger.info(
        json.dumps({"level": "🟢", "message": "Checklist reiniciado", "checklistId": checklist_id, "items": len(items)})
    )
    return build_response(200, {"message": "Checklist reiniciado", "checklistId": checklist_id, "itemsActualizados": len(items)})


# ─── Colecciones (grupos / items) ─────────────────────────────────────────────


def list_collection_items(checklist_id: str, collection: str):
    if not _checklist_exists(checklist_id):
        return build_response(404, {"error": f"Checklist '{checklist_id}' no encontrado"})

    prefix = _collection_meta(collection)["prefix"]
    items = _query_partition(_pk(checklist_id), f"{prefix}#")
    items = _sort_items(collection, items)
    return build_response(200, {"items": [_serialize(i) for i in items], "count": len(items)})


def create_collection_item(checklist_id: str, collection: str, data: dict):
    if not isinstance(data, dict):
        raise ValueError("El body debe ser un objeto JSON")
    if not _checklist_exists(checklist_id):
        return build_response(404, {"error": f"Checklist '{checklist_id}' no encontrado"})

    _validate_collection_payload(collection, data, is_update=False)

    if collection == "grupos":
        orden = data.get("orden")
        if orden is None:
            orden = len(_query_partition(_pk(checklist_id), "GRUPO#"))
        item = _create_group_item(checklist_id, data, orden)
    else:
        item = _create_item_item(checklist_id, data)

    logger.info(
        json.dumps(
            {
                "level": "🟢",
                "message": "Item de checklist creado",
                "checklistId": checklist_id,
                "collection": collection,
                "itemId": item["id"],
            }
        )
    )
    return build_response(201, {"message": "Item creado", "checklistId": checklist_id, "id": item["id"]})


def get_collection_item(checklist_id: str, collection: str, item_id: str):
    result = table.get_item(Key={"pk": _pk(checklist_id), "sk": _sk(collection, item_id)})
    if "Item" not in result:
        return build_response(404, {"error": f"Item '{item_id}' no encontrado en '{collection}'"})
    return build_response(200, _serialize(result["Item"]))


def update_collection_item(checklist_id: str, collection: str, item_id: str, data: dict):
    if not isinstance(data, dict):
        raise ValueError("El body debe ser un objeto JSON")

    _validate_collection_payload(collection, data, is_update=True)
    payload = _normalize_payload_for_dynamo(collection, data)

    update_fields = {
        key: value
        for key, value in payload.items()
        if key not in {"pk", "sk", "id", "checklistId", "entityType", "type", "createdAt"}
    }
    if not update_fields:
        return build_response(400, {"error": "No hay campos para actualizar"})

    update_fields["updatedAt"] = _utc_now()
    return _run_update(
        key={"pk": _pk(checklist_id), "sk": _sk(collection, item_id)},
        update_fields=update_fields,
        not_found_message=f"Item '{item_id}' no encontrado en '{collection}'",
    )


def delete_collection_item(checklist_id: str, collection: str, item_id: str):
    key = {"pk": _pk(checklist_id), "sk": _sk(collection, item_id)}
    existing = table.get_item(Key=key)
    if "Item" not in existing:
        return build_response(404, {"error": f"Item '{item_id}' no encontrado en '{collection}'"})

    table.delete_item(Key=key)

    # Si se borra un grupo, los items que apuntaban a él quedan "sin categoría".
    if collection == "grupos":
        _unassign_group_from_items(checklist_id, item_id)

    logger.info(
        json.dumps(
            {
                "level": "🟢",
                "message": "Item de checklist eliminado",
                "checklistId": checklist_id,
                "collection": collection,
                "itemId": item_id,
            }
        )
    )
    return build_response(200, {"message": "Item eliminado", "id": item_id, "collection": collection})


def update_item_comprado(checklist_id: str, item_id: str, data: dict):
    if not isinstance(data, dict):
        raise ValueError("El body debe ser un objeto JSON")
    if "comprado" not in data or not isinstance(data.get("comprado"), bool):
        raise ValueError("El campo 'comprado' (booleano) es requerido")

    return _run_update(
        key={"pk": _pk(checklist_id), "sk": _sk("items", item_id)},
        update_fields={"comprado": data["comprado"], "updatedAt": _utc_now()},
        not_found_message=f"Item '{item_id}' no encontrado",
        success_message="Estado de compra actualizado",
    )


# ─── Helpers de creación / normalización ──────────────────────────────────────


def _create_group_item(checklist_id: str, data: dict, orden: int) -> dict:
    if not _clean_str(data.get("nombre")):
        raise ValueError("El campo 'nombre' es requerido para el grupo")

    group_id = data.get("id") or str(uuid.uuid4())
    timestamp = _utc_now()
    item = {
        "pk": _pk(checklist_id),
        "sk": _sk("grupos", group_id),
        "id": group_id,
        "checklistId": checklist_id,
        "entityType": "checklist_grupo",
        "type": "checklist_grupo",
        "nombre": _clean_str(data.get("nombre")),
        "emoji": _nullable_str(data.get("emoji")),
        "orden": _to_int(data.get("orden", orden), "orden"),
        "createdAt": timestamp,
        "updatedAt": timestamp,
    }
    table.put_item(
        Item=item,
        ConditionExpression="attribute_not_exists(pk) AND attribute_not_exists(sk)",
    )
    return item


def _create_item_item(checklist_id: str, data: dict) -> dict:
    if not _clean_str(data.get("nombre")):
        raise ValueError("El campo 'nombre' es requerido para el item")

    prioridad = data.get("prioridad") or "media"
    if prioridad not in VALID_PRIORITIES:
        raise ValueError(f"'prioridad' inválida. Opciones: {', '.join(sorted(VALID_PRIORITIES))}")
    prioridad_orden = _to_positive_int(data.get("prioridadOrden", 999), "prioridadOrden")

    item_id = data.get("id") or str(uuid.uuid4())
    timestamp = _utc_now()
    item = {
        "pk": _pk(checklist_id),
        "sk": _sk("items", item_id),
        "id": item_id,
        "checklistId": checklist_id,
        "entityType": "checklist_item",
        "type": "checklist_item",
        "nombre": _clean_str(data.get("nombre")),
        "groupId": _nullable_str(data.get("groupId")),
        "prioridad": prioridad,
        "prioridadOrden": prioridad_orden,
        "precio": _to_float(data["precio"], "precio") if data.get("precio") is not None else None,
        "emoji": _nullable_str(data.get("emoji")),
        "comprado": bool(data.get("comprado", False)),
        "nota": _nullable_str(data.get("nota")),
        "createdAt": timestamp,
        "updatedAt": timestamp,
    }
    table.put_item(
        Item=item,
        ConditionExpression="attribute_not_exists(pk) AND attribute_not_exists(sk)",
    )
    return item


def _normalize_payload_for_dynamo(collection: str, data: dict) -> dict:
    payload = dict(data)

    if collection == "grupos":
        if "orden" in payload:
            payload["orden"] = _to_int(payload["orden"], "orden")
    elif collection == "items":
        if "prioridadOrden" in payload:
            payload["prioridadOrden"] = _to_positive_int(payload["prioridadOrden"], "prioridadOrden")
        if "precio" in payload:
            payload["precio"] = _to_float(payload["precio"], "precio") if payload["precio"] is not None else None
        if "groupId" in payload:
            payload["groupId"] = _nullable_str(payload["groupId"])
        if "comprado" in payload and not isinstance(payload["comprado"], bool):
            raise ValueError("El campo 'comprado' debe ser booleano")

    return payload


def _validate_collection_payload(collection: str, data: dict, is_update: bool):
    meta = _collection_meta(collection)
    if not is_update:
        missing = meta["required"] - data.keys()
        if missing:
            raise ValueError(f"Campos requeridos faltantes en '{collection}': {sorted(missing)}")

    if collection == "grupos":
        if "nombre" in data and not _clean_str(data.get("nombre")):
            raise ValueError("El campo 'nombre' no puede estar vacío")
    elif collection == "items":
        if "nombre" in data and not _clean_str(data.get("nombre")):
            raise ValueError("El campo 'nombre' no puede estar vacío")
        if "prioridad" in data and data["prioridad"] not in VALID_PRIORITIES:
            raise ValueError(f"'prioridad' inválida. Opciones: {', '.join(sorted(VALID_PRIORITIES))}")
        if "prioridadOrden" in data:
            _to_positive_int(data["prioridadOrden"], "prioridadOrden")
        if "comprado" in data and not isinstance(data.get("comprado"), bool):
            raise ValueError("El campo 'comprado' debe ser booleano")


def _unassign_group_from_items(checklist_id: str, group_id: str):
    items = _query_partition(_pk(checklist_id), "ITEM#")
    now = _utc_now()
    for item in items:
        if item.get("groupId") == group_id:
            table.update_item(
                Key={"pk": item["pk"], "sk": item["sk"]},
                UpdateExpression="REMOVE groupId SET updatedAt = :u",
                ExpressionAttributeValues={":u": now},
            )


def _build_summary(items: list) -> dict:
    total = len(items)
    comprados = sum(1 for i in items if i.get("comprado"))
    precio_total = sum((i.get("precio") or 0) for i in items)
    precio_pendiente = sum((i.get("precio") or 0) for i in items if not i.get("comprado"))
    return {
        "totalItems": total,
        "compradosCount": comprados,
        "progreso": (comprados / total) if total else 0,
        "precioTotal": precio_total,
        "precioPendiente": precio_pendiente,
    }


def _run_update(key: dict, update_fields: dict, not_found_message: str, success_message: str = "Item actualizado"):
    expr_parts = []
    expr_values = {}
    expr_names = {}

    for field, value in update_fields.items():
        name_key = f"#f_{field}"
        value_key = f":v_{field}"
        expr_parts.append(f"{name_key} = {value_key}")
        expr_values[value_key] = value
        expr_names[name_key] = field

    try:
        table.update_item(
            Key=key,
            UpdateExpression="SET " + ", ".join(expr_parts),
            ExpressionAttributeValues=expr_values,
            ExpressionAttributeNames=expr_names,
            ConditionExpression=Attr("pk").exists() & Attr("sk").exists(),
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            logger.info(json.dumps({"level": "🔵", "message": "Item no encontrado", "key": key}))
            return build_response(404, {"error": not_found_message})
        raise

    return build_response(200, {"message": success_message})


def _serialize(value):
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, dict):
        return {key: _serialize(item_value) for key, item_value in value.items()}
    if isinstance(value, list):
        return [_serialize(item_value) for item_value in value]
    return value


def _checklist_exists(checklist_id: str) -> bool:
    result = table.get_item(Key={"pk": _pk(checklist_id), "sk": "META"}, ProjectionExpression="pk")
    return "Item" in result


def _collection_meta(collection: str) -> dict:
    return COLLECTIONS[collection]


def _query_partition(partition_key: str, begins_with_prefix: str | None = None) -> list:
    if begins_with_prefix:
        kwargs = {"KeyConditionExpression": Key("pk").eq(partition_key) & Key("sk").begins_with(begins_with_prefix)}
    else:
        kwargs = {"KeyConditionExpression": Key("pk").eq(partition_key)}
    return query_all(table, **kwargs)


def _sort_items(collection: str, items: list[dict]) -> list[dict]:
    if collection == "grupos":
        return sorted(items, key=lambda item: _as_number(item.get("orden", 0)))
    if collection == "items":
        return sorted(
            items,
            key=lambda item: (
                bool(item.get("comprado")),
                _as_number(item.get("prioridadOrden", Decimal("Infinity"))),
                str(item.get("nombre", "")).lower(),
            ),
        )
    return items


def _as_number(value):
    if isinstance(value, Decimal):
        return float(value)
    return value if isinstance(value, (int, float)) else 0


def _pk(checklist_id: str) -> str:
    return f"CHECKLIST#{checklist_id}"


def _sk(collection: str, item_id: str) -> str:
    return f"{_collection_meta(collection)['prefix']}#{item_id}"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _clean_str(value) -> str:
    return value.strip() if isinstance(value, str) else ""


def _nullable_str(value):
    if value is None:
        return None
    return _clean_str(value)


def _to_int(value, field_name: str) -> Decimal:
    try:
        if isinstance(value, Decimal):
            return value.to_integral_value()
        return Decimal(str(value)).to_integral_value()
    except (TypeError, ValueError, InvalidOperation) as exc:
        raise ValueError(f"El campo '{field_name}' debe ser numérico") from exc


def _to_positive_int(value, field_name: str) -> Decimal:
    try:
        number = Decimal(str(value))
    except (TypeError, ValueError, InvalidOperation) as exc:
        raise ValueError(f"El campo '{field_name}' debe ser un entero mayor o igual a 1") from exc

    if not number.is_finite() or number < 1 or number != number.to_integral_value():
        raise ValueError(f"El campo '{field_name}' debe ser un entero mayor o igual a 1")
    return number


def _to_float(value, field_name: str) -> Decimal:
    try:
        if isinstance(value, Decimal):
            return value
        return Decimal(str(value))
    except (TypeError, ValueError, InvalidOperation) as exc:
        raise ValueError(f"El campo '{field_name}' debe ser numérico") from exc
