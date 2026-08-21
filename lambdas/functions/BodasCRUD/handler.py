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
from decimal import Decimal, InvalidOperation
from pathlib import Path

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.config import Config
from botocore.exceptions import ClientError

from common.utils import build_response, get_path_param, parse_body, query_all # type: ignore

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ.get("BODAS_TABLE_NAME", "BodasTable")
table = dynamodb.Table(TABLE_NAME) # type: ignore
BUCKET_NAME = os.environ.get("BUCKET_NAME")
s3_client = boto3.client("s3", config=Config(signature_version="s3v4"))

VALID_RSVP = {"confirmado", "pendiente", "noVa"}
VALID_ESTADOS_PROVEEDOR = {"pendiente", "confirmado", "pagado"}
ALLOWED_IMAGE_TYPES = {"image/jpeg", "image/png", "image/webp", "image/jpg", "image/heic"}

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
    "hospedaje": {
        "prefix": "HOSPEDAJE",
        "entity_type": "hospedaje_boda",
        "required": {"nombre"},
        "sort_field": "nombre",
    },
    "menu": {
        "prefix": "MENU",
        "entity_type": "menu_boda",
        "required": {"nombre"},
        "sort_field": "momento",
    },
    "album": {
        "prefix": "FOTO",
        "entity_type": "foto_boda",
        "required": {"url"},
        "sort_field": "createdAt",
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
            return create_boda(parse_body(event)) # pyright: ignore[reportArgumentType]

        if not boda_id:
            return build_response(400, {"error": "Se requiere {bodaId} en la ruta"})

        if method == "GET" and raw_path.endswith("/public"):
            return get_public_snapshot(boda_id)

        if not collection:
            match method:
                case "GET":
                    return get_boda(boda_id)
                case "PUT":
                    return update_boda(boda_id, parse_body(event)) # type: ignore
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

        if method == "POST" and collection == "album" and raw_path.endswith("/upload-url"):
            return create_album_upload_url(boda_id, parse_body(event)) # pyright: ignore[reportArgumentType]

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
    query_kwargs = {
        "IndexName": "entityType-index",
        "KeyConditionExpression": Key("entityType").eq("boda") & Key("sk").begins_with("META"),
    }
    items = query_all(table, **query_kwargs)

    items.sort(key=lambda item: item.get("fechaEvento", "") or item.get("nombre", ""))
    return build_response(
        200,
        {"items": [_serialize_item_for_response(item) for item in items], "count": len(items)},
    )


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
    return build_response(200, _serialize_item_for_response(result["Item"]))


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

    _cleanup_album_assets(items)

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
    lodging = list_collection_items(boda_id, "hospedaje", internal=True)
    menu = list_collection_items(boda_id, "menu", internal=True)
    album = list_collection_items(boda_id, "album", internal=True)

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
            "hospedaje": lodging,
            "menu": menu,
            "album": album,
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
    items = [_serialize_item_for_response(item) for item in items]
    if internal:
        return items
    return build_response(200, {"items": items, "count": len(items)})


def create_collection_item(boda_id: str, collection: str, data: dict):
    if not isinstance(data, dict):
        raise ValueError("El body debe ser un objeto JSON")
    if not _boda_exists(boda_id):
        return build_response(404, {"error": f"Boda '{boda_id}' no encontrada"})

    payload = _normalize_payload_for_dynamo(collection, data)
    _validate_collection_payload(collection, payload, is_update=False)
    item_id = data.get("id") or str(uuid.uuid4())
    item = _normalize_collection_item(boda_id, collection, item_id, payload)

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


def create_album_upload_url(boda_id: str, data: dict):
    if not isinstance(data, dict):
        raise ValueError("El body debe ser un objeto JSON")
    if not _boda_exists(boda_id):
        return build_response(404, {"error": f"Boda '{boda_id}' no encontrada"})
    if not BUCKET_NAME:
        logger.error(json.dumps({"level": "🔴", "message": "BUCKET_NAME no configurado para BodasCRUD"}))
        return build_response(500, {"error": "Configuración de bucket no disponible"})

    file_name = _clean_str(data.get("fileName")) or "image.jpg"
    file_type = (_clean_str(data.get("fileType")) or "image/jpeg").lower()
    if file_type not in ALLOWED_IMAGE_TYPES:
        raise ValueError("'fileType' inválido. Usa image/jpeg, image/png, image/webp o image/heic")

    extension = Path(file_name).suffix.lower().replace(".", "") or _extension_from_type(file_type)
    photo_id = str(uuid.uuid4())
    s3_key = f"weddings/{boda_id}/album/{photo_id}.{extension}"

    presigned_url = s3_client.generate_presigned_url(
        "put_object",
        Params={"Bucket": BUCKET_NAME, "Key": s3_key, "ContentType": file_type},
        ExpiresIn=3600,
    )

    now = _utc_now()
    item = {
        "pk": _pk(boda_id),
        "sk": _sk("album", photo_id),
        "id": photo_id,
        "bodaId": boda_id,
        "entityType": "foto_boda",
        "type": "foto_boda",
        "titulo": _clean_str(data.get("titulo")),
        "url": f"https://{BUCKET_NAME}.s3.amazonaws.com/{s3_key}",
        "s3Key": s3_key,
        "mimeType": file_type,
        "subidoPor": _clean_str(data.get("subidoPor")) or "invitado",
        "createdAt": now,
        "updatedAt": now,
    }
    if _clean_str(data.get("comentario")):
        item["comentario"] = _clean_str(data.get("comentario"))

    table.put_item(
        Item=item,
        ConditionExpression="attribute_not_exists(pk) AND attribute_not_exists(sk)",
    )

    logger.info(
        json.dumps(
            {
                "level": "🟢",
                "message": "Upload URL de album creada",
                "bodaId": boda_id,
                "photoId": photo_id,
                "has_comment": bool(item.get("comentario")),
            }
        )
    )

    return build_response(
        201,
        {
            "message": "Upload URL creada",
            "id": photo_id,
            "key": s3_key,
            "uploadUrl": presigned_url,
            "finalUrl": item["url"],
        },
    )


def get_collection_item(boda_id: str, collection: str, item_id: str):
    result = table.get_item(Key={"pk": _pk(boda_id), "sk": _sk(collection, item_id)})
    if "Item" not in result:
        return build_response(404, {"error": f"Item '{item_id}' no encontrado en '{collection}'"})
    return build_response(200, _serialize_item_for_response(result["Item"]))


def update_collection_item(boda_id: str, collection: str, item_id: str, data: dict):
    if not isinstance(data, dict):
        raise ValueError("El body debe ser un objeto JSON")

    payload = _normalize_payload_for_dynamo(collection, data)
    _validate_collection_payload(collection, payload, is_update=True)
    update_fields = {
        key: value
        for key, value in payload.items()
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

    if collection == "album":
        album_item = existing["Item"]
        delete_error = _delete_s3_object(album_item.get("s3Key"))
        if delete_error:
            return build_response(502, {"error": "No fue posible eliminar la foto del álbum"})

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


def _normalize_payload_for_dynamo(collection: str, data: dict) -> dict:
    payload = dict(data)

    if collection == "invitados":
        if "personas" in payload:
            payload["personas"] = _to_int(payload["personas"], "personas")
    elif collection == "gastos":
        for field in ("estimado", "pagado"):
            if field in payload:
                payload[field] = _to_float(payload[field], field)
    elif collection == "proveedores":
        if "costo" in payload:
            payload["costo"] = _to_float(payload["costo"], "costo")
    elif collection == "looks":
        if "precio" in payload:
            payload["precio"] = _to_float(payload["precio"], "precio")
    elif collection == "itinerario":
        if "ubicacionLat" in payload:
            payload["ubicacionLat"] = _to_float(payload["ubicacionLat"], "coordenadas.lat")
        if "ubicacionLng" in payload:
            payload["ubicacionLng"] = _to_float(payload["ubicacionLng"], "coordenadas.lng")
    elif collection == "menu":
        if "restricciones" in payload and not isinstance(payload["restricciones"], list):
            raise ValueError("El campo 'restricciones' debe ser una lista")
    elif collection == "album":
        if "mimeType" in payload:
            payload["mimeType"] = _clean_str(payload["mimeType"]).lower()
            if payload["mimeType"] and payload["mimeType"] not in ALLOWED_IMAGE_TYPES:
                raise ValueError("'mimeType' inválido para album")

    return payload


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
        coordinates = _normalize_itinerary_coordinates(data)
        item.update(
            {
                "titulo": _clean_str(data.get("titulo")),
                "hora": _clean_str(data.get("hora")),
                "nota": _clean_str(data.get("nota")),
                "localizacion": _clean_str(data.get("localizacion")),
                "emoji": _clean_str(data.get("emoji")) or "💒",
            }
        )
        if coordinates:
            item["coordenadas"] = coordinates
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
    elif collection == "hospedaje":
        item.update(
            {
                "nombre": _clean_str(data.get("nombre")),
                "direccion": _clean_str(data.get("direccion")),
                "contacto": _clean_str(data.get("contacto")),
                "checkIn": _clean_str(data.get("checkIn")),
                "checkOut": _clean_str(data.get("checkOut")),
                "mapaUrl": _clean_str(data.get("mapaUrl")),
                "nota": _clean_str(data.get("nota")),
            }
        )
    elif collection == "menu":
        item.update(
            {
                "nombre": _clean_str(data.get("nombre")),
                "momento": _clean_str(data.get("momento")) or "Recepción",
                "descripcion": _clean_str(data.get("descripcion")),
                "tipo": _clean_str(data.get("tipo")),
                "restricciones": data.get("restricciones") if isinstance(data.get("restricciones"), list) else [],
                "esVegetariano": bool(data.get("esVegetariano", False)),
            }
        )
    elif collection == "album":
        item.update(
            {
                "titulo": _clean_str(data.get("titulo")),
                "url": _clean_str(data.get("url")),
                "s3Key": _clean_str(data.get("s3Key")),
                "mimeType": _clean_str(data.get("mimeType")).lower(),
                "subidoPor": _clean_str(data.get("subidoPor")) or "invitado",
                "comentario": _clean_str(data.get("comentario")),
            }
        )

    return item


def _serialize_item_for_response(value):
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, dict):
        return {key: _serialize_item_for_response(item_value) for key, item_value in value.items()}
    if isinstance(value, list):
        return [_serialize_item_for_response(item_value) for item_value in value]
    return value


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
        _validate_itinerary_location_payload(data)
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
    elif collection == "hospedaje":
        if "nombre" in data and not _clean_str(data.get("nombre")):
            raise ValueError("El campo 'nombre' no puede estar vacío")
    elif collection == "menu":
        if "nombre" in data and not _clean_str(data.get("nombre")):
            raise ValueError("El campo 'nombre' no puede estar vacío")
        if "restricciones" in data and not isinstance(data.get("restricciones"), list):
            raise ValueError("El campo 'restricciones' debe ser una lista")
    elif collection == "album":
        if "url" in data and not _clean_str(data.get("url")):
            raise ValueError("El campo 'url' no puede estar vacío")
        if "mimeType" in data:
            mime_type = _clean_str(data.get("mimeType")).lower()
            if mime_type and mime_type not in ALLOWED_IMAGE_TYPES:
                raise ValueError("'mimeType' inválido para album")


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


def _validate_itinerary_location_payload(data: dict):
    if "coordenadas" not in data:
        return

    localizacion = _clean_str(data.get("localizacion"))
    if not localizacion:
        raise ValueError("El campo 'coordenadas' solo se permite cuando 'localizacion' está informada")

    coordinates = data.get("coordenadas")
    if not isinstance(coordinates, dict):
        raise ValueError("El campo 'coordenadas' debe ser un objeto con 'lat' y 'lng'")

    if "lat" not in coordinates or "lng" not in coordinates:
        raise ValueError("El campo 'coordenadas' debe incluir 'lat' y 'lng'")

    _to_float(coordinates.get("lat"), "coordenadas.lat")
    _to_float(coordinates.get("lng"), "coordenadas.lng")


def _normalize_itinerary_coordinates(data: dict):
    coordinates = data.get("coordenadas")
    if not isinstance(coordinates, dict):
        return None

    localizacion = _clean_str(data.get("localizacion"))
    if not localizacion:
        return None

    if "lat" not in coordinates or "lng" not in coordinates:
        return None

    return {
        "lat": _to_float(coordinates.get("lat"), "coordenadas.lat"),
        "lng": _to_float(coordinates.get("lng"), "coordenadas.lng"),
    }


def _query_partition(partition_key: str, begins_with_prefix: str | None = None) -> list:
    if begins_with_prefix:
        kwargs = {"KeyConditionExpression": Key("pk").eq(partition_key) & Key("sk").begins_with(begins_with_prefix)}
    else:
        kwargs = {"KeyConditionExpression": Key("pk").eq(partition_key)}

    return query_all(table, **kwargs)


def _sort_items(collection: str, items: list[dict]) -> list[dict]:
    if collection == "itinerario":
        return sorted(items, key=lambda item: item.get("hora", ""))
    if collection == "tareas":
        return sorted(items, key=lambda item: (item.get("completada", False), item.get("titulo", "")))
    if collection == "album":
        return sorted(items, key=lambda item: item.get("createdAt", ""), reverse=True)
    sort_field = _collection_meta(collection).get("sort_field")
    return sorted(items, key=lambda item: str(item.get(sort_field, "")).lower())


def _cleanup_album_assets(items: list[dict]):
    for item in items:
        if item.get("entityType") != "foto_boda":
            continue
        _delete_s3_object(item.get("s3Key"), swallow_errors=True)


def _delete_s3_object(s3_key: str | None, swallow_errors: bool = False) -> bool:
    if not s3_key:
        return False
    if not BUCKET_NAME:
        logger.warning(
            json.dumps({"level": "🟡", "message": "No hay BUCKET_NAME para borrar foto", "has_key": True})
        )
        return not swallow_errors
    try:
        s3_client.delete_object(Bucket=BUCKET_NAME, Key=s3_key)
        return False
    except ClientError as exc:
        logger.warning(
            json.dumps(
                {
                    "level": "🟡",
                    "message": "No se pudo borrar objeto S3 del album",
                    "code": exc.response.get("Error", {}).get("Code", "Unknown"),
                }
            )
        )
        return not swallow_errors


def _extension_from_type(file_type: str) -> str:
    if file_type in {"image/jpeg", "image/jpg"}:
        return "jpg"
    if file_type == "image/png":
        return "png"
    if file_type == "image/webp":
        return "webp"
    if file_type == "image/heic":
        return "heic"
    return "jpg"


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


def _normalize_numeric_string(value: str) -> str:
    text = value.strip()
    if not text:
        return text

    sign = ""
    if text[0] in {"+", "-"}:
        sign = text[0]
        text = text[1:]

    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        parts = text.split(",")
        if len(parts) > 1 and len(parts[-1]) <= 2:
            text = text.replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "." in text:
        parts = text.split(".")
        if len(parts) == 2 and len(parts[1]) == 3 and parts[0].isdigit() and parts[1].isdigit():
            text = "".join(parts)

    return sign + text


def _to_int(value, field_name: str) -> Decimal:
    try:
        if isinstance(value, Decimal):
            return value.to_integral_value()
        if isinstance(value, str):
            return Decimal(_normalize_numeric_string(value)).to_integral_value()
        return Decimal(str(value)).to_integral_value()
    except (TypeError, ValueError, InvalidOperation) as exc:
        raise ValueError(f"El campo '{field_name}' debe ser numérico") from exc


def _to_float(value, field_name: str) -> Decimal:
    try:
        if isinstance(value, Decimal):
            return value
        if isinstance(value, str):
            return Decimal(_normalize_numeric_string(value))
        return Decimal(str(value))
    except (TypeError, ValueError, InvalidOperation) as exc:
        raise ValueError(f"El campo '{field_name}' debe ser numérico") from exc