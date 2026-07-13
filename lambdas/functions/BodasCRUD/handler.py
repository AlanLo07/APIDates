"""
bodas_handler.py — CRUD para administración de bodas e invitados.

Modelo:
- Una sola tabla DynamoDB con PK/SK compuesta por boda.
- Item raíz de la boda: pk=BODA#{bodaId}, sk=META
- Colecciones hijas: INVITADO, TAREA, PASO, GASTO, CANCION, PROVEEDOR, LOOK

Rutas principales:
- GET/POST /bodas
- GET/PUT/DELETE /bodas/{bodaId}
- GET /bodas/{bodaId}/public
- GET/POST /bodas/{bodaId}/{collection}
- GET/PUT/DELETE /bodas/{bodaId}/{collection}/{itemId}
- PATCH /bodas/{bodaId}/invitados/{itemId}/rsvp
"""
import json
import logging
import os
import uuid
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError

from common.utils import build_response, get_path_param, parse_body

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ.get("BODAS_TABLE_NAME", "BodasTable")
table = dynamodb.Table(TABLE_NAME)

VALID_RSVP = {"confirmado", "pendiente", "noVa"}
VALID_ESTADOS_PROVEEDOR = {"pendiente", "confirmado", "pagado"}

COLLECTIONS = {
    "invitados": {
        "prefix": "INVITADO",
        "entity_type": "invitado",
        "required": {"nombre"},
        "sort_field": "nombre",
    },
    "tareas": {
        "prefix": "TAREA",
        "entity_type": "tarea_boda",
        "required": {"titulo"},
        "sort_field": "titulo",
    },
    "itinerario": {
        "prefix": "PASO",
        "entity_type": "paso_boda",
        "required": {"titulo", "hora"},
        "sort_field": "hora",
    },
    "gastos": {
        "prefix": "GASTO",
        "entity_type": "gasto_boda",
        "required": {"concepto"},
        "sort_field": "concepto",
    },
    "canciones": {
        "prefix": "CANCION",
        "entity_type": "cancion_boda",
        "required": {"titulo", "artista"},
        "sort_field": "titulo",
    },
    "proveedores": {
        "prefix": "PROVEEDOR",
        "entity_type": "proveedor_boda",
        "required": {"nombre", "categoria"},
        "sort_field": "nombre",
    },
    "looks": {
        "prefix": "LOOK",
        "entity_type": "look_boda",
        "required": {"persona", "prenda"},
        "sort_field": "prenda",
    },
}


def lambda_handler(event, context):
    method = event.get("requestContext", {}).get("http", {}).get("method", "")
    raw_path = event.get("rawPath", "")
    boda_id = get_path_param(event, "bodaId")
    collection = get_path_param(event, "collection")
    item_id = get_path_param(event, "itemId")

    logger.info(
        json.dumps(
            {
                "level": "🔵",
                "message": "Solicitud BodasCRUD",
                "method": method,
                "path": raw_path,
                "has_boda_id": bool(boda_id),
                "collection": collection,
                "has_item_id": bool(item_id),
                "function": getattr(context, "function_name", "unknown"),
            }
        )
    )

    if method == "OPTIONS":
        return build_response(200, {})

    try:
        if method == "GET" and not boda_id:
            return list_bodas()

        if method == "POST" and not boda_id:
            return create_boda(parse_body(event))

        if not boda_id:
            return build_response(400, {"error": "Se requiere {bodaId} en la ruta"})

        if method == "GET" and raw_path.endswith("/public"):
            return get_public_snapshot(boda_id)

        if not collection:
            match method:
                case "GET":
                    return get_boda(boda_id)
                case "PUT":
                    return update_boda(boda_id, parse_body(event))
                case "DELETE":
                    return delete_boda(boda_id)
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

        if method == "PATCH" and collection == "invitados" and raw_path.endswith("/rsvp"):
            if not item_id:
                return build_response(400, {"error": "Se requiere {itemId} en la ruta"})
            return update_guest_rsvp(boda_id, item_id, parse_body(event))

        if not item_id:
            match method:
                case "GET":
                    return list_collection_items(boda_id, collection)
                case "POST":
                    return create_collection_item(boda_id, collection, parse_body(event))
                case _:
                    return build_response(405, {"error": f"Método {method} no permitido"})

        match method:
            case "GET":
                return get_collection_item(boda_id, collection, item_id)
            case "PUT":
                return update_collection_item(boda_id, collection, item_id, parse_body(event))
            case "DELETE":
                return delete_collection_item(boda_id, collection, item_id)
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
                    "message": "Error DynamoDB en BodasCRUD",
                    "code": exc.response.get("Error", {}).get("Code", "Unknown"),
                }
            )
        )
        return build_response(502, {"error": "Error de base de datos"})
    except Exception:
        logger.exception("🔴 Error inesperado en BodasCRUD")
        return build_response(500, {"error": "Error interno del servidor"})


def list_bodas():
    items = []
    query_kwargs = {
        "IndexName": "entityType-index",
        "KeyConditionExpression": Key("entityType").eq("boda") & Key("sk").begins_with("META"),
    }

    while True:
        result = table.query(**query_kwargs)
        items.extend(result.get("Items", []))
        if not result.get("LastEvaluatedKey"):
            break
        query_kwargs["ExclusiveStartKey"] = result["LastEvaluatedKey"]

    items.sort(key=lambda item: item.get("fechaEvento", "") or item.get("nombre", ""))
    return build_response(200, {"items": items, "count": len(items)})


def create_boda(data: dict):
    if not isinstance(data, dict):
        raise ValueError("El body debe ser un objeto JSON")
    if not _clean_str(data.get("nombre")):
        raise ValueError("El campo 'nombre' es requerido")

    boda_id = data.get("id") or data.get("bodaId") or str(uuid.uuid4())
    item = _normalize_boda(boda_id, data)

    table.put_item(
        Item=item,
        ConditionExpression="attribute_not_exists(pk) AND attribute_not_exists(sk)",
    )
    logger.info(json.dumps({"level": "🟢", "message": "Boda creada", "bodaId": boda_id}))
    return build_response(201, {"message": "Boda creada", "bodaId": boda_id})


def get_boda(boda_id: str):
    result = table.get_item(Key={"pk": _pk(boda_id), "sk": "META"})
    if "Item" not in result:
        return build_response(404, {"error": f"Boda '{boda_id}' no encontrada"})
    return build_response(200, result["Item"])


def update_boda(boda_id: str, data: dict):
    if not isinstance(data, dict):
        raise ValueError("El body debe ser un objeto JSON")
    if "nombre" in data and not _clean_str(data.get("nombre")):
        raise ValueError("El campo 'nombre' no puede estar vacío")

    update_fields = {
        key: value
        for key, value in data.items()
        if key not in {"pk", "sk", "id", "bodaId", "entityType", "createdAt"}
    }
    if not update_fields:
        return build_response(400, {"error": "No hay campos para actualizar"})

    update_fields["updatedAt"] = _utc_now()
    return _run_update(
        key={"pk": _pk(boda_id), "sk": "META"},
        update_fields=update_fields,
        not_found_message=f"Boda '{boda_id}' no encontrada",
    )


def delete_boda(boda_id: str):
    partition_key = _pk(boda_id)
    items = _query_partition(partition_key)
    if not items:
        return build_response(404, {"error": f"Boda '{boda_id}' no encontrada"})

    with table.batch_writer() as batch:
        for item in items:
            batch.delete_item(Key={"pk": item["pk"], "sk": item["sk"]})

    logger.info(json.dumps({"level": "🟢", "message": "Boda eliminada", "bodaId": boda_id, "items": len(items)}))
    return build_response(200, {"message": "Boda eliminada", "bodaId": boda_id, "itemsDeleted": len(items)})


def get_public_snapshot(boda_id: str):
    boda_result = table.get_item(Key={"pk": _pk(boda_id), "sk": "META"})
    if "Item" not in boda_result:
        return build_response(404, {"error": f"Boda '{boda_id}' no encontrada"})

    itinerary = list_collection_items(boda_id, "itinerario", internal=True)
    providers = list_collection_items(boda_id, "proveedores", internal=True)

    boda = boda_result["Item"]
    public_boda = {
        "bodaId": boda.get("bodaId"),
        "nombre": boda.get("nombre"),
        "fechaEvento": boda.get("fechaEvento", ""),
        "lugar": boda.get("lugar", ""),
        "direccion": boda.get("direccion", ""),
        "mensajeBienvenida": boda.get("mensajeBienvenida", ""),
        "dressCode": boda.get("dressCode", ""),
        "contacto": boda.get("contacto", ""),
        "instagramHashtag": boda.get("instagramHashtag", ""),
    }

    return build_response(
        200,
        {
            "boda": public_boda,
            "itinerario": itinerary,
            "proveedores": providers,
        },
    )


def list_collection_items(boda_id: str, collection: str, internal: bool = False):
    if not _boda_exists(boda_id):
        if internal:
            return []
        return build_response(404, {"error": f"Boda '{boda_id}' no encontrada"})

    prefix = _collection_meta(collection)["prefix"]
    items = _query_partition(_pk(boda_id), begins_with_prefix=f"{prefix}#")
    items = _sort_items(collection, items)
    if internal:
        return items
    return build_response(200, {"items": items, "count": len(items)})


def create_collection_item(boda_id: str, collection: str, data: dict):
    if not isinstance(data, dict):
        raise ValueError("El body debe ser un objeto JSON")
    if not _boda_exists(boda_id):
        return build_response(404, {"error": f"Boda '{boda_id}' no encontrada"})

    _validate_collection_payload(collection, data, is_update=False)
    item_id = data.get("id") or str(uuid.uuid4())
    item = _normalize_collection_item(boda_id, collection, item_id, data)

    table.put_item(
        Item=item,
        ConditionExpression="attribute_not_exists(pk) AND attribute_not_exists(sk)",
    )
    logger.info(
        json.dumps(
            {
                "level": "🟢",
                "message": "Item de boda creado",
                "bodaId": boda_id,
                "collection": collection,
                "itemId": item_id,
            }
        )
    )
    return build_response(201, {"message": "Item creado", "bodaId": boda_id, "id": item_id})


def get_collection_item(boda_id: str, collection: str, item_id: str):
    result = table.get_item(Key={"pk": _pk(boda_id), "sk": _sk(collection, item_id)})
    if "Item" not in result:
        return build_response(404, {"error": f"Item '{item_id}' no encontrado en '{collection}'"})
    return build_response(200, result["Item"])


def update_collection_item(boda_id: str, collection: str, item_id: str, data: dict):
    if not isinstance(data, dict):
        raise ValueError("El body debe ser un objeto JSON")

    _validate_collection_payload(collection, data, is_update=True)
    update_fields = {
        key: value
        for key, value in data.items()
        if key not in {"pk", "sk", "id", "bodaId", "entityType", "type", "createdAt"}
    }
    if not update_fields:
        return build_response(400, {"error": "No hay campos para actualizar"})

    update_fields["updatedAt"] = _utc_now()
    return _run_update(
        key={"pk": _pk(boda_id), "sk": _sk(collection, item_id)},
        update_fields=update_fields,
        not_found_message=f"Item '{item_id}' no encontrado en '{collection}'",
    )


def delete_collection_item(boda_id: str, collection: str, item_id: str):
    key = {"pk": _pk(boda_id), "sk": _sk(collection, item_id)}
    existing = table.get_item(Key=key)
    if "Item" not in existing:
        return build_response(404, {"error": f"Item '{item_id}' no encontrado en '{collection}'"})

    table.delete_item(Key=key)
    logger.info(
        json.dumps(
            {
                "level": "🟢",
                "message": "Item de boda eliminado",
                "bodaId": boda_id,
                "collection": collection,
                "itemId": item_id,
            }
        )
    )
    return build_response(200, {"message": "Item eliminado", "id": item_id, "collection": collection})


def update_guest_rsvp(boda_id: str, item_id: str, data: dict):
    if not isinstance(data, dict):
        raise ValueError("El body debe ser un objeto JSON")

    rsvp = data.get("rsvp")
    if rsvp not in VALID_RSVP:
        raise ValueError(f"'rsvp' inválido. Opciones: {', '.join(sorted(VALID_RSVP))}")

    update_fields = {"rsvp": rsvp, "updatedAt": _utc_now()}
    if "personas" in data:
        update_fields["personas"] = _to_int(data["personas"], "personas")

    return _run_update(
        key={"pk": _pk(boda_id), "sk": _sk("invitados", item_id)},
        update_fields=update_fields,
        not_found_message=f"Invitado '{item_id}' no encontrado",
        success_message="RSVP actualizado",
    )


def _normalize_boda(boda_id: str, data: dict) -> dict:
    timestamp = _utc_now()
    known_fields = {
        "nombre": _clean_str(data.get("nombre")),
        "fechaEvento": _clean_str(data.get("fechaEvento")),
        "lugar": _clean_str(data.get("lugar")),
        "direccion": _clean_str(data.get("direccion")),
        "mensajeBienvenida": _clean_str(data.get("mensajeBienvenida")),
        "dressCode": _clean_str(data.get("dressCode")),
        "contacto": _clean_str(data.get("contacto")),
        "instagramHashtag": _clean_str(data.get("instagramHashtag")),
        "coverImage": _clean_str(data.get("coverImage")),
    }
    extra_fields = {
        key: value
        for key, value in data.items()
        if key not in {"id", "bodaId", *known_fields.keys()}
    }
    return {
        "pk": _pk(boda_id),
        "sk": "META",
        "id": boda_id,
        "bodaId": boda_id,
        "entityType": "boda",
        **known_fields,
        **extra_fields,
        "createdAt": timestamp,
        "updatedAt": timestamp,
    }


def _normalize_collection_item(boda_id: str, collection: str, item_id: str, data: dict) -> dict:
    meta = _collection_meta(collection)
    timestamp = _utc_now()

    item = {
        "pk": _pk(boda_id),
        "sk": _sk(collection, item_id),
        "id": item_id,
        "bodaId": boda_id,
        "entityType": meta["entity_type"],
        "type": meta["entity_type"],
        "createdAt": timestamp,
        "updatedAt": timestamp,
    }

    if collection == "invitados":
        item.update(
            {
                "nombre": _clean_str(data.get("nombre")),
                "grupo": _clean_str(data.get("grupo")) or "Amigos",
                "personas": _to_int(data.get("personas", 1), "personas"),
                "rsvp": data.get("rsvp", "pendiente"),
            }
        )
    elif collection == "tareas":
        item.update(
            {
                "titulo": _clean_str(data.get("titulo")),
                "categoria": _clean_str(data.get("categoria")) or "General",
                "completada": bool(data.get("completada", False)),
                "fechaLimite": _nullable_str(data.get("fechaLimite")),
            }
        )
    elif collection == "itinerario":
        item.update(
            {
                "titulo": _clean_str(data.get("titulo")),
                "hora": _clean_str(data.get("hora")),
                "nota": _clean_str(data.get("nota")),
                "emoji": _clean_str(data.get("emoji")) or "💒",
            }
        )
    elif collection == "gastos":
        item.update(
            {
                "concepto": _clean_str(data.get("concepto")),
                "categoria": _clean_str(data.get("categoria")) or "General",
                "estimado": _to_float(data.get("estimado", 0), "estimado"),
                "pagado": _to_float(data.get("pagado", 0), "pagado"),
            }
        )
    elif collection == "canciones":
        item.update(
            {
                "titulo": _clean_str(data.get("titulo")),
                "artista": _clean_str(data.get("artista")),
                "momento": _clean_str(data.get("momento")) or "Fiesta",
                "link": _clean_str(data.get("link")),
            }
        )
    elif collection == "proveedores":
        item.update(
            {
                "nombre": _clean_str(data.get("nombre")),
                "categoria": _clean_str(data.get("categoria")),
                "contacto": _clean_str(data.get("contacto")),
                "link": _clean_str(data.get("link")),
                "costo": _to_float(data.get("costo", 0), "costo"),
                "estado": data.get("estado", "pendiente"),
                "notas": _clean_str(data.get("notas")),
            }
        )
    elif collection == "looks":
        item.update(
            {
                "persona": _clean_str(data.get("persona")),
                "prenda": _clean_str(data.get("prenda")),
                "tienda": _clean_str(data.get("tienda")),
                "talla": _clean_str(data.get("talla")),
                "precio": _to_float(data.get("precio", 0), "precio"),
                "comprado": bool(data.get("comprado", False)),
                "notas": _clean_str(data.get("notas")),
            }
        )

    return item


def _validate_collection_payload(collection: str, data: dict, is_update: bool):
    meta = _collection_meta(collection)
    if not is_update:
        missing = meta["required"] - data.keys()
        if missing:
            raise ValueError(f"Campos requeridos faltantes en '{collection}': {sorted(missing)}")

    if collection == "invitados":
        if "nombre" in data and not _clean_str(data.get("nombre")):
            raise ValueError("El campo 'nombre' no puede estar vacío")
        if "personas" in data:
            personas = _to_int(data.get("personas"), "personas")
            if personas < 1:
                raise ValueError("El campo 'personas' debe ser mayor o igual a 1")
        if "rsvp" in data and data["rsvp"] not in VALID_RSVP:
            raise ValueError(f"'rsvp' inválido. Opciones: {', '.join(sorted(VALID_RSVP))}")
    elif collection == "tareas":
        if "titulo" in data and not _clean_str(data.get("titulo")):
            raise ValueError("El campo 'titulo' no puede estar vacío")
    elif collection == "itinerario":
        if "titulo" in data and not _clean_str(data.get("titulo")):
            raise ValueError("El campo 'titulo' no puede estar vacío")
        if "hora" in data and not _clean_str(data.get("hora")):
            raise ValueError("El campo 'hora' no puede estar vacío")
    elif collection == "gastos":
        if "concepto" in data and not _clean_str(data.get("concepto")):
            raise ValueError("El campo 'concepto' no puede estar vacío")
        for field in ("estimado", "pagado"):
            if field in data:
                _to_float(data[field], field)
    elif collection == "canciones":
        for field in ("titulo", "artista"):
            if field in data and not _clean_str(data.get(field)):
                raise ValueError(f"El campo '{field}' no puede estar vacío")
    elif collection == "proveedores":
        for field in ("nombre", "categoria"):
            if field in data and not _clean_str(data.get(field)):
                raise ValueError(f"El campo '{field}' no puede estar vacío")
        if "estado" in data and data["estado"] not in VALID_ESTADOS_PROVEEDOR:
            raise ValueError(
                f"'estado' inválido. Opciones: {', '.join(sorted(VALID_ESTADOS_PROVEEDOR))}"
            )
        if "costo" in data:
            _to_float(data["costo"], "costo")
    elif collection == "looks":
        for field in ("persona", "prenda"):
            if field in data and not _clean_str(data.get(field)):
                raise ValueError(f"El campo '{field}' no puede estar vacío")
        if "precio" in data:
            _to_float(data["precio"], "precio")


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
            return build_response(404, {"error": not_found_message})
        raise

    return build_response(200, {"message": success_message})


def _query_partition(partition_key: str, begins_with_prefix: str | None = None) -> list:
    kwargs = {"KeyConditionExpression": Key("pk").eq(partition_key)}
    if begins_with_prefix:
        kwargs["KeyConditionExpression"] = Key("pk").eq(partition_key) & Key("sk").begins_with(
            begins_with_prefix
        )

    items = []
    while True:
        result = table.query(**kwargs)
        items.extend(result.get("Items", []))
        if not result.get("LastEvaluatedKey"):
            break
        kwargs["ExclusiveStartKey"] = result["LastEvaluatedKey"]
    return items


def _sort_items(collection: str, items: list[dict]) -> list[dict]:
    if collection == "itinerario":
        return sorted(items, key=lambda item: item.get("hora", ""))
    if collection == "tareas":
        return sorted(items, key=lambda item: (item.get("completada", False), item.get("titulo", "")))
    sort_field = _collection_meta(collection).get("sort_field")
    return sorted(items, key=lambda item: str(item.get(sort_field, "")).lower())


def _boda_exists(boda_id: str) -> bool:
    result = table.get_item(
        Key={"pk": _pk(boda_id), "sk": "META"},
        ProjectionExpression="pk",
    )
    return "Item" in result


def _collection_meta(collection: str) -> dict:
    return COLLECTIONS[collection]


def _pk(boda_id: str) -> str:
    return f"BODA#{boda_id}"


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


def _to_int(value, field_name: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"El campo '{field_name}' debe ser numérico") from exc


def _to_float(value, field_name: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"El campo '{field_name}' debe ser numérico") from exc