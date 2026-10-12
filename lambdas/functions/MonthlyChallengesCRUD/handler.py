"""
handler.py — Retos mensuales compartidos por una pareja.

Modelo (independiente de ChallengesCRUD/DiceCRUD — retos eróticos — y de
CouponsCRUD — cuponera por usuario): no comparte datos ni estados con ellos.

Dos tablas:
- MonthlyChallengesTable: estado por pareja (PK=PAREJA#{coupleId})
    SK=PAREJA                         -> membresía USUARIO#/EMAIL#
    SK=META                           -> datos de la pareja (emails/nombres)
    SK=MESESTADO#{monthKey}           -> estado del mes (reto elegido, status, version)
    SK=MESPROPUESTA#{monthKey}#{cycle}#{sub}  -> propuesta de un integrante
    SK=MESELECCION#{monthKey}#{cycle}#{sub}   -> elección/voto de un integrante
    SK=MESHISTORIAL#{monthKey}#{cycle}#{uuid} -> auditoría (cancelado/reemplazado/editado)
    SK=FAVORITO#{favoriteId}          -> retos favoritos de la pareja
    SK=CUPON#{couponId}               -> cupón compartido generado al completar
- MonthlyChallengesCatalogTable: catálogo compartido de retos (reutilizable entre parejas)
    PK=id, GSI category-index

Reglas clave implementadas (ver resumen de diseño entregado al usuario):
- Un solo reto elegido por mes natural, mismo estado visible para ambos.
- Propuestas congeladas (snapshot) aunque el catálogo cambie después.
- Acuerdo explícito requerido (silencio != consentimiento).
- Plazo configurable -> asignación aleatoria automática si no hay acuerdo.
- Edición/cancelación/completado con control optimista por versión.
- Completar + crear cupón en una sola transacción condicionada (sin duplicados).
"""
import calendar
import hashlib
import json
import logging
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, cast

import boto3
from boto3.dynamodb.conditions import Attr, Key
from boto3.dynamodb.types import TypeSerializer
from botocore.exceptions import ClientError

from common.utils import (  # type: ignore
    build_response,
    cached_scan_all,
    get_path_param,
    log_event,
    parse_body,
    query_all,
    scan_all,
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
dynamodb_client = cast(Any, dynamodb.meta).client
serializer = TypeSerializer()

TABLE_NAME = os.environ.get("MONTHLY_CHALLENGES_TABLE_NAME", "MonthlyChallengesTable")
CATALOG_TABLE_NAME = os.environ.get("MONTHLY_CHALLENGES_CATALOG_TABLE_NAME", "MonthlyChallengesCatalogTable")
table = dynamodb.Table(TABLE_NAME)  # type: ignore
catalog_table = dynamodb.Table(CATALOG_TABLE_NAME)  # type: ignore

# ─── Constantes ───────────────────────────────────────────────────────────────

VALID_CATEGORIES = {"diversion", "actividades_juntos", "comunicacion", "ahorro", "otro"}
DEADLINE_DAY = int(os.environ.get("MONTHLY_CHALLENGE_DEADLINE_DAY", "5"))
REPLACEMENT_GRACE_DAYS = int(os.environ.get("MONTHLY_CHALLENGE_REPLACEMENT_GRACE_DAYS", "3"))
RECENT_WINDOW = 6  # meses recientes a excluir en selección aleatoria cuando hay alternativas
MAX_TITLE_LEN = 120
MAX_DESCRIPTION_LEN = 600


class UnauthenticatedError(PermissionError):
    pass


class ForbiddenError(PermissionError):
    pass


class ChallengeError(ValueError):
    def __init__(self, message, status_code=400):
        super().__init__(message)
        self.status_code = status_code


# ─── Utilidades generales ─────────────────────────────────────────────────────


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def current_month_key():
    return datetime.now(timezone.utc).strftime("%Y-%m")


def _normalize_email(value):
    return str(value or "").strip().casefold()


def _database_error(message, exc):
    log_event(logger, "🔴", message, error_code=exc.response.get("Error", {}).get("Code", "Unknown"))
    return {"error": "Error de base de datos"}, 500


def _dynamo_value_map(values):
    return {key: serializer.serialize(value) for key, value in values.items()}


def _execute_transaction(actions):
    serialized_actions = []
    for action in actions:
        operation, detail = next(iter(action.items()))
        serialized_detail = dict(detail)
        for key in ("Item", "Key", "ExpressionAttributeValues"):
            if key in serialized_detail:
                serialized_detail[key] = _dynamo_value_map(serialized_detail[key])
        serialized_actions.append({operation: serialized_detail})
    dynamodb_client.transact_write_items(TransactItems=serialized_actions)


def _put_action(item, condition="attribute_not_exists(PK)"):
    return {"Put": {"TableName": TABLE_NAME, "Item": item, "ConditionExpression": condition}}


def _update_action(key, expression, values, names, condition):
    return {
        "Update": {
            "TableName": TABLE_NAME,
            "Key": key,
            "UpdateExpression": expression,
            "ExpressionAttributeValues": values,
            "ExpressionAttributeNames": names,
            "ConditionExpression": condition,
        }
    }


# ─── Autenticación y resolución de pareja (independiente de FinancesCRUD) ────


def _get_request_user(event):
    claims = event.get("requestContext", {}).get("authorizer", {}).get("jwt", {}).get("claims", {})
    user_id = str(claims.get("sub") or "").strip()
    email = _normalize_email(claims.get("email"))
    if not user_id or not email:
        raise UnauthenticatedError("Usuario no autenticado")
    return {"sub": user_id, "email": email, "name": str(claims.get("name") or "").strip()}


def _membership_key(prefix, identifier):
    return {"PK": f"{prefix}#{identifier}", "SK": "PAREJA"}


def _couple_id_for_emails(email1, email2):
    normalized = sorted((_normalize_email(email1), _normalize_email(email2)))
    digest = hashlib.sha256("|".join(normalized).encode("utf-8")).hexdigest()
    return digest[:24]


def _save_user_membership(user, couple_id):
    now = now_iso()
    try:
        table.put_item(
            Item={**_membership_key("USUARIO", user["sub"]), "coupleId": couple_id, "email": user["email"],
                  "createdAt": now, "updatedAt": now},
            ConditionExpression=Attr("PK").not_exists() | Attr("coupleId").eq(couple_id),
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            raise ForbiddenError("El usuario ya pertenece a otra pareja de retos mensuales") from exc
        raise


def _resolve_couple_context(event):
    user = _get_request_user(event)
    user_membership = table.get_item(Key=_membership_key("USUARIO", user["sub"]), ConsistentRead=True).get("Item")
    if user_membership:
        return f"PAREJA#{user_membership['coupleId']}", user

    email_membership = table.get_item(Key=_membership_key("EMAIL", user["email"]), ConsistentRead=True).get("Item")
    if email_membership:
        couple_id = email_membership["coupleId"]
        _save_user_membership(user, couple_id)
        return f"PAREJA#{couple_id}", user

    raise ForbiddenError("El usuario no pertenece a una pareja de retos mensuales")


def init_couple(user, user1_name, user2_name, user1_email, user2_email):
    now = now_iso()
    normalized_emails = {_normalize_email(user1_email), _normalize_email(user2_email)}
    if "" in normalized_emails or len(normalized_emails) != 2:
        return {"error": "Los correos de los integrantes deben ser válidos y diferentes"}, 400
    if user["email"] not in normalized_emails:
        return {"error": "El usuario autenticado debe ser uno de los integrantes"}, 403

    couple_id = _couple_id_for_emails(user1_email, user2_email)
    couple_pk = f"PAREJA#{couple_id}"
    couple_item = {
        "PK": couple_pk, "SK": "META", "coupleId": couple_id,
        "user1": {"name": user1_name, "email": _normalize_email(user1_email)},
        "user2": {"name": user2_name, "email": _normalize_email(user2_email)},
        "createdAt": now, "updatedAt": now,
    }
    membership_items = [
        {**_membership_key("EMAIL", email), "coupleId": couple_id, "createdAt": now, "updatedAt": now}
        for email in normalized_emails
    ]
    membership_items.append({**_membership_key("USUARIO", user["sub"]), "coupleId": couple_id,
                              "email": user["email"], "createdAt": now, "updatedAt": now})
    try:
        dynamodb_client.transact_write_items(TransactItems=[
            {"Put": {"TableName": TABLE_NAME, "Item": {k: serializer.serialize(v) for k, v in item.items()},
                     "ConditionExpression": "attribute_not_exists(PK)"}}
            for item in [couple_item, *membership_items]
        ])
        log_event(logger, "🟢", "Pareja de retos mensuales inicializada", couple_ref=couple_id[-8:])
        return {"coupleId": couple_id, "user1": {"name": user1_name, "email": user1_email},
                "user2": {"name": user2_name, "email": user2_email}, "createdAt": now}, 201
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"ConditionalCheckFailedException", "TransactionCanceledException"}:
            return {"error": "La pareja o uno de sus integrantes ya está registrado"}, 409
        log_event(logger, "🔴", "Error inicializando pareja", error_code=exc.response.get("Error", {}).get("Code", "Unknown"))
        raise


def get_couple_meta(couple_pk):
    return table.get_item(Key={"PK": couple_pk, "SK": "META"}).get("Item")


def _partner_email(couple_meta, user_email):
    user1_email = couple_meta.get("user1", {}).get("email")
    user2_email = couple_meta.get("user2", {}).get("email")
    return user2_email if user_email == user1_email else user1_email


def _partner_name(couple_meta, user_email):
    user1 = couple_meta.get("user1", {})
    user2 = couple_meta.get("user2", {})
    return user2.get("name") if user_email == user1.get("email") else user1.get("name")


# ─── Validación de contenido de retos ─────────────────────────────────────────


def _validate_challenge_fields(title, description, category):
    if not isinstance(title, str) or not title.strip() or len(title.strip()) > MAX_TITLE_LEN:
        raise ChallengeError(f"title es requerido (1-{MAX_TITLE_LEN} caracteres)")
    if not isinstance(description, str) or not description.strip() or len(description.strip()) > MAX_DESCRIPTION_LEN:
        raise ChallengeError(f"description es requerido (1-{MAX_DESCRIPTION_LEN} caracteres)")
    if category not in VALID_CATEGORIES:
        raise ChallengeError(f"category inválida. Válidas: {', '.join(sorted(VALID_CATEGORIES))}")
    return title.strip(), description.strip(), category


def _snapshot_from_payload(data):
    """Construye un snapshot congelado de reto a partir de un payload de propuesta/favorito.

    Acepta: {source: "custom", title, description, category}
            {source: "catalog", catalogChallengeId}
    """
    if not isinstance(data, dict):
        raise ChallengeError("El body debe ser un objeto JSON")
    source = data.get("source")
    if source == "catalog":
        catalog_id = data.get("catalogChallengeId")
        if not isinstance(catalog_id, str) or not catalog_id.strip():
            raise ChallengeError("catalogChallengeId es requerido cuando source=catalog")
        item = catalog_table.get_item(Key={"id": catalog_id.strip()}).get("Item")
        if not item:
            raise ChallengeError("El reto de catálogo indicado no existe", 404)
        title, description, category = _validate_challenge_fields(item["title"], item["description"], item["category"])
        return {"source": "catalog", "catalogChallengeId": catalog_id.strip(), "title": title,
                "description": description, "category": category}
    if source == "custom":
        title, description, category = _validate_challenge_fields(
            data.get("title"), data.get("description"), data.get("category")
        )
        return {"source": "custom", "catalogChallengeId": None, "title": title, "description": description,
                "category": category}
    raise ChallengeError("source debe ser 'custom' o 'catalog'")


# ─── Catálogo de retos (estructura para carga futura) ────────────────────────


def create_catalog_challenge(data: dict):
    title, description, category = _validate_challenge_fields(
        data.get("title"), data.get("description"), data.get("category")
    )
    challenge_id = str(uuid.uuid4())
    now = now_iso()
    item = {"id": challenge_id, "title": title, "description": description, "category": category,
            "active": True, "createdAt": now, "updatedAt": now}
    catalog_table.put_item(Item=item)
    log_event(logger, "🟢", "Reto de catálogo creado", item_id=challenge_id, category=category)
    return item, 201


def list_catalog_challenges(query_params: dict):
    category = query_params.get("category")
    if category and category not in VALID_CATEGORIES:
        raise ChallengeError(f"category inválida. Válidas: {', '.join(sorted(VALID_CATEGORIES))}")
    only_active = query_params.get("includeInactive", "false").lower() != "true"
    filter_expression = Attr("active").eq(True) if only_active else None
    if category:
        category_filter = Attr("category").eq(category)
        filter_expression = category_filter if filter_expression is None else (filter_expression & category_filter)
    params = {}
    if filter_expression is not None:
        params["FilterExpression"] = filter_expression
    items = scan_all(catalog_table, **params)
    return {"items": items, "count": len(items)}, 200


def get_catalog_challenge(challenge_id: str):
    item = catalog_table.get_item(Key={"id": challenge_id}).get("Item")
    if not item:
        return {"error": "Reto de catálogo no encontrado"}, 404
    return item, 200


def update_catalog_challenge(challenge_id: str, data: dict):
    existing = catalog_table.get_item(Key={"id": challenge_id}).get("Item")
    if not existing:
        return {"error": "Reto de catálogo no encontrado"}, 404
    allowed = {"title", "description", "category", "active"}
    unknown = set(data) - allowed
    if unknown:
        raise ChallengeError(f"Campos no editables: {', '.join(sorted(unknown))}")
    title = data.get("title", existing["title"])
    description = data.get("description", existing["description"])
    category = data.get("category", existing["category"])
    active = data.get("active", existing.get("active", True))
    if not isinstance(active, bool):
        raise ChallengeError("active debe ser booleano")
    title, description, category = _validate_challenge_fields(title, description, category)
    now = now_iso()
    catalog_table.update_item(
        Key={"id": challenge_id},
        UpdateExpression="SET #title=:title, #description=:description, #category=:category, #active=:active, #updatedAt=:now",
        ExpressionAttributeNames={"#title": "title", "#description": "description", "#category": "category",
                                   "#active": "active", "#updatedAt": "updatedAt"},
        ExpressionAttributeValues={":title": title, ":description": description, ":category": category,
                                    ":active": active, ":now": now},
        ConditionExpression=Attr("id").exists(),
    )
    log_event(logger, "🟢", "Reto de catálogo actualizado", item_id=challenge_id)
    return {**existing, "title": title, "description": description, "category": category, "active": active,
            "updatedAt": now}, 200


def deactivate_catalog_challenge(challenge_id: str):
    """Desactiva (no borra) para no romper propuestas/favoritos/meses ya elegidos."""
    try:
        catalog_table.update_item(
            Key={"id": challenge_id},
            UpdateExpression="SET #active=:active, #updatedAt=:now",
            ExpressionAttributeNames={"#active": "active", "#updatedAt": "updatedAt"},
            ExpressionAttributeValues={":active": False, ":now": now_iso()},
            ConditionExpression=Attr("id").exists(),
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return {"error": "Reto de catálogo no encontrado"}, 404
        raise
    log_event(logger, "🟢", "Reto de catálogo desactivado", item_id=challenge_id)
    return {"message": "Reto de catálogo desactivado", "id": challenge_id}, 200


def _recent_catalog_challenge_ids(couple_pk, window=RECENT_WINDOW):
    records = query_all(table, KeyConditionExpression=Key("PK").eq(couple_pk) & Key("SK").begins_with("MESESTADO#"),
                         ScanIndexForward=False, Limit=window)
    ids = set()
    for record in records:
        selected = record.get("selectedChallenge")
        if selected and selected.get("catalogChallengeId"):
            ids.add(selected["catalogChallengeId"])
    return ids


def pick_random_catalog_challenge(couple_pk, category=None, exclude_ids=None, exclude_recent=True):
    if category and category not in VALID_CATEGORIES:
        raise ChallengeError(f"category inválida. Válidas: {', '.join(sorted(VALID_CATEGORIES))}")
    params = {"FilterExpression": Attr("active").eq(True) & Attr("category").eq(category)} if category else {
        "FilterExpression": Attr("active").eq(True)}
    items = cached_scan_all(catalog_table, cache_key=f"monthlychallenges:catalog:{category or 'all'}",
                             ttl_seconds=30, **params)
    if not items:
        return None
    excluded = set(exclude_ids or set())
    if exclude_recent:
        excluded |= _recent_catalog_challenge_ids(couple_pk)
    candidates = [item for item in items if item["id"] not in excluded]
    if not candidates:
        candidates = items
    chosen = random.choice(candidates)
    log_event(logger, "🔵", "Sugerencia aleatoria de catálogo", item_id=chosen["id"])
    return chosen


def suggest_random_challenge(couple_pk, query_params: dict):
    """Sugerencia de catálogo: NO asigna el reto del mes, solo lo devuelve."""
    category = query_params.get("category")
    chosen = pick_random_catalog_challenge(couple_pk, category=category)
    if not chosen:
        return {"error": "No hay retos disponibles en el catálogo con esos filtros"}, 404
    return chosen, 200


# ─── Estado del mes ───────────────────────────────────────────────────────────


def _deadline_for(month_key, deadline_day=DEADLINE_DAY):
    year, month = (int(part) for part in month_key.split("-"))
    days_in_month = calendar.monthrange(year, month)[1]
    day = min(deadline_day, days_in_month)
    return datetime(year, month, day, 23, 59, 59, tzinfo=timezone.utc).isoformat()


def _month_sk(month_key):
    return f"MESESTADO#{month_key}"


def _get_or_create_month_record(couple_pk, month_key):
    existing = table.get_item(Key={"PK": couple_pk, "SK": _month_sk(month_key)}, ConsistentRead=True).get("Item")
    if existing:
        return existing
    now = now_iso()
    item = {
        "PK": couple_pk, "SK": _month_sk(month_key), "monthKey": month_key, "cycle": 1,
        "status": "PENDING", "selectedChallenge": None, "selectedSource": None,
        "assignedAt": None, "assignedCycle": None,
        "completedBy": None, "completedByEmail": None, "completedByName": None, "completedAt": None,
        "couponId": None, "deadlineDay": DEADLINE_DAY, "deadlineAt": _deadline_for(month_key),
        "version": 0, "createdAt": now, "updatedAt": now,
    }
    try:
        table.put_item(Item=item, ConditionExpression=Attr("PK").not_exists())
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return table.get_item(Key={"PK": couple_pk, "SK": _month_sk(month_key)}, ConsistentRead=True)["Item"]
        raise
    return item


def _apply_deadline_fallback(couple_pk, month_record):
    """Asigna un reto aleatorio si venció el plazo y sigue sin acuerdo. Perezoso (no usa cron)."""
    if month_record["status"] != "PENDING":
        return month_record
    if now_iso() < month_record["deadlineAt"]:
        return month_record

    proposals = _list_proposals(couple_pk, month_record["monthKey"], month_record["cycle"])
    exclude_ids = {p["catalogChallengeId"] for p in proposals if p.get("catalogChallengeId")}
    chosen = pick_random_catalog_challenge(couple_pk, exclude_ids=exclude_ids)
    if not chosen:
        log_event(logger, "🟡", "Plazo vencido sin catálogo disponible para asignación automática",
                   couple_ref=couple_pk[-8:])
        return month_record

    selected_challenge = {"source": "catalog", "catalogChallengeId": chosen["id"], "title": chosen["title"],
                           "description": chosen["description"], "category": chosen["category"]}
    now = now_iso()
    version = int(month_record.get("version", 0))
    try:
        result = table.update_item(
            Key={"PK": couple_pk, "SK": _month_sk(month_record["monthKey"])},
            UpdateExpression=(
                "SET #status=:assigned, #selectedChallenge=:challenge, #selectedSource=:source, "
                "#assignedAt=:now, #assignedCycle=:cycle, #version=:nextVersion, #updatedAt=:now"
            ),
            ExpressionAttributeNames={"#status": "status", "#selectedChallenge": "selectedChallenge",
                                       "#selectedSource": "selectedSource", "#assignedAt": "assignedAt",
                                       "#assignedCycle": "assignedCycle", "#version": "version",
                                       "#updatedAt": "updatedAt"},
            ExpressionAttributeValues={":assigned": "ASSIGNED", ":challenge": selected_challenge,
                                        ":source": "auto_random", ":now": now, ":cycle": month_record["cycle"],
                                        ":pending": "PENDING", ":expectedVersion": version,
                                        ":nextVersion": version + 1},
            ConditionExpression="#status = :pending AND #version = :expectedVersion",
            ReturnValues="ALL_NEW",
        )
        log_event(logger, "🔵", "Reto mensual asignado automáticamente por vencimiento de plazo",
                   couple_ref=couple_pk[-8:], month_key=month_record["monthKey"])
        return result["Attributes"]
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return table.get_item(Key={"PK": couple_pk, "SK": _month_sk(month_record["monthKey"])},
                                   ConsistentRead=True)["Item"]
        raise


def _load_current_month(couple_pk, month_key=None):
    month_key = month_key or current_month_key()
    record = _get_or_create_month_record(couple_pk, month_key)
    return _apply_deadline_fallback(couple_pk, record)


# ─── Propuestas ───────────────────────────────────────────────────────────────


def _proposal_sk(month_key, cycle, sub):
    return f"MESPROPUESTA#{month_key}#{cycle}#{sub}"


def _choice_sk(month_key, cycle, sub):
    return f"MESELECCION#{month_key}#{cycle}#{sub}"


def _list_proposals(couple_pk, month_key, cycle):
    prefix = f"MESPROPUESTA#{month_key}#{cycle}#"
    return query_all(table, KeyConditionExpression=Key("PK").eq(couple_pk) & Key("SK").begins_with(prefix))


def _list_choices(couple_pk, month_key, cycle):
    prefix = f"MESELECCION#{month_key}#{cycle}#"
    return query_all(table, KeyConditionExpression=Key("PK").eq(couple_pk) & Key("SK").begins_with(prefix))


def create_or_update_proposal(couple_pk, user, data):
    month_record = _load_current_month(couple_pk)
    if month_record["status"] != "PENDING":
        raise ChallengeError("No se pueden proponer retos: el mes ya tiene un reto asignado o completado", 409)
    snapshot = _snapshot_from_payload(data)
    now = now_iso()
    sk = _proposal_sk(month_record["monthKey"], month_record["cycle"], user["sub"])
    existing = table.get_item(Key={"PK": couple_pk, "SK": sk}).get("Item")
    item = {
        "PK": couple_pk, "SK": sk, "monthKey": month_record["monthKey"], "cycle": month_record["cycle"],
        "proposedBy": user["sub"], "proposedByEmail": user["email"], "proposedByName": user.get("name"),
        **snapshot, "createdAt": existing["createdAt"] if existing else now, "updatedAt": now,
    }
    table.put_item(Item=item)
    log_event(logger, "🟢", "Propuesta de reto mensual registrada", couple_ref=couple_pk[-8:],
               month_key=month_record["monthKey"], cycle=month_record["cycle"])
    return build_month_state(couple_pk, month_record, user), 200


def withdraw_proposal(couple_pk, user):
    month_record = _load_current_month(couple_pk)
    if month_record["status"] != "PENDING":
        raise ChallengeError("No se puede retirar una propuesta: el mes ya tiene un reto asignado o completado", 409)
    sk = _proposal_sk(month_record["monthKey"], month_record["cycle"], user["sub"])
    try:
        table.delete_item(Key={"PK": couple_pk, "SK": sk}, ConditionExpression=Attr("PK").exists())
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            raise ChallengeError("No tienes una propuesta registrada este mes", 404) from exc
        raise
    # La elección de cualquiera de los dos que apuntara a esta propuesta queda huérfana: se elimina.
    for choice in _list_choices(couple_pk, month_record["monthKey"], month_record["cycle"]):
        if choice.get("chosenProposedBy") == user["sub"]:
            table.delete_item(Key={"PK": couple_pk, "SK": choice["SK"]})
    log_event(logger, "🟢", "Propuesta de reto mensual retirada", couple_ref=couple_pk[-8:])
    return build_month_state(couple_pk, month_record, user), 200


def register_choice(couple_pk, user, data):
    month_record = _load_current_month(couple_pk)
    if month_record["status"] != "PENDING":
        raise ChallengeError("No se puede elegir: el mes ya tiene un reto asignado o completado", 409)
    if not isinstance(data, dict):
        raise ChallengeError("El body debe ser un objeto JSON")

    request_tiebreak = bool(data.get("requestRandomTiebreak", False))
    proposed_by = data.get("proposedBy")
    month_key, cycle = month_record["monthKey"], month_record["cycle"]
    proposals = {p["proposedBy"]: p for p in _list_proposals(couple_pk, month_key, cycle)}

    if not request_tiebreak:
        if not isinstance(proposed_by, str) or proposed_by not in proposals:
            raise ChallengeError("proposedBy debe referenciar una propuesta existente de este mes", 404)

    now = now_iso()
    sk = _choice_sk(month_key, cycle, user["sub"])
    existing = table.get_item(Key={"PK": couple_pk, "SK": sk}).get("Item")
    choice_item = {
        "PK": couple_pk, "SK": sk, "monthKey": month_key, "cycle": cycle, "chosenBy": user["sub"],
        "chosenByEmail": user["email"], "chosenProposedBy": None if request_tiebreak else proposed_by,
        "requestRandomTiebreak": request_tiebreak,
        "createdAt": existing["createdAt"] if existing else now, "updatedAt": now,
    }
    table.put_item(Item=choice_item)
    log_event(logger, "🟢", "Elección de reto mensual registrada", couple_ref=couple_pk[-8:],
               tiebreak=request_tiebreak)

    month_record = _resolve_agreement(couple_pk, month_record, proposals)
    return build_month_state(couple_pk, month_record, user), 200


def _resolve_agreement(couple_pk, month_record, proposals):
    month_key, cycle = month_record["monthKey"], month_record["cycle"]
    choices = _list_choices(couple_pk, month_key, cycle)
    if len(choices) < 2:
        return month_record  # falta que el otro integrante elija; nunca se asume consentimiento

    both_tiebreak = all(choice.get("requestRandomTiebreak") for choice in choices)
    if both_tiebreak:
        exclude_ids = {p["catalogChallengeId"] for p in proposals.values() if p.get("catalogChallengeId")}
        chosen = pick_random_catalog_challenge(couple_pk, exclude_ids=exclude_ids)
        if not chosen:
            return month_record
        selected_challenge = {"source": "catalog", "catalogChallengeId": chosen["id"], "title": chosen["title"],
                               "description": chosen["description"], "category": chosen["category"]}
        return _assign_selected(couple_pk, month_record, selected_challenge, "agreement_random")

    targets = {choice.get("chosenProposedBy") for choice in choices if not choice.get("requestRandomTiebreak")}
    if len(targets) == 1 and None not in targets:
        agreed_proposal_owner = next(iter(targets))
        proposal = proposals.get(agreed_proposal_owner)
        if not proposal:
            return month_record
        selected_challenge = {"source": proposal["source"], "catalogChallengeId": proposal.get("catalogChallengeId"),
                               "title": proposal["title"], "description": proposal["description"],
                               "category": proposal["category"]}
        return _assign_selected(couple_pk, month_record, selected_challenge, "agreement")

    return month_record  # desacuerdo: permanece PENDING


def _assign_selected(couple_pk, month_record, selected_challenge, selected_source):
    now = now_iso()
    version = int(month_record.get("version", 0))
    try:
        result = table.update_item(
            Key={"PK": couple_pk, "SK": _month_sk(month_record["monthKey"])},
            UpdateExpression=(
                "SET #status=:assigned, #selectedChallenge=:challenge, #selectedSource=:source, "
                "#assignedAt=:now, #assignedCycle=:cycle, #version=:nextVersion, #updatedAt=:now"
            ),
            ExpressionAttributeNames={"#status": "status", "#selectedChallenge": "selectedChallenge",
                                       "#selectedSource": "selectedSource", "#assignedAt": "assignedAt",
                                       "#assignedCycle": "assignedCycle", "#version": "version",
                                       "#updatedAt": "updatedAt"},
            ExpressionAttributeValues={":assigned": "ASSIGNED", ":challenge": selected_challenge,
                                        ":source": selected_source, ":now": now, ":cycle": month_record["cycle"],
                                        ":pending": "PENDING", ":expectedVersion": version,
                                        ":nextVersion": version + 1},
            ConditionExpression="#status = :pending AND #version = :expectedVersion",
            ReturnValues="ALL_NEW",
        )
        log_event(logger, "🟢", "Reto mensual asignado", couple_ref=couple_pk[-8:], source=selected_source)
        return result["Attributes"]
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return table.get_item(Key={"PK": couple_pk, "SK": _month_sk(month_record["monthKey"])},
                                   ConsistentRead=True)["Item"]
        raise


# ─── Reto activo: editar / cancelar / reemplazar ─────────────────────────────


def _record_history(couple_pk, month_record, user, action, reason=None):
    entry_id = str(uuid.uuid4())
    table.put_item(Item={
        "PK": couple_pk, "SK": f"MESHISTORIAL#{month_record['monthKey']}#{month_record['cycle']}#{entry_id}",
        "monthKey": month_record["monthKey"], "cycle": month_record["cycle"], "action": action,
        "previousChallenge": month_record.get("selectedChallenge"), "by": user["sub"], "byEmail": user["email"],
        "reason": reason, "createdAt": now_iso(),
    })


def edit_active_challenge(couple_pk, user, data):
    month_record = _load_current_month(couple_pk)
    if month_record["status"] != "ASSIGNED":
        raise ChallengeError("No hay un reto activo para editar", 409)
    if not isinstance(data, dict) or not data:
        raise ChallengeError("Debe proporcionar al menos un campo para actualizar")
    allowed = {"title", "description", "category"}
    unknown = set(data) - allowed
    if unknown:
        raise ChallengeError(f"Campos no editables: {', '.join(sorted(unknown))}")

    current = month_record["selectedChallenge"]
    title = data.get("title", current["title"])
    description = data.get("description", current["description"])
    category = data.get("category", current["category"])
    title, description, category = _validate_challenge_fields(title, description, category)

    _record_history(couple_pk, month_record, user, "edited")
    updated_challenge = {**current, "title": title, "description": description, "category": category}
    now = now_iso()
    version = int(month_record.get("version", 0))
    try:
        result = table.update_item(
            Key={"PK": couple_pk, "SK": _month_sk(month_record["monthKey"])},
            UpdateExpression="SET #selectedChallenge=:challenge, #version=:nextVersion, #updatedAt=:now",
            ExpressionAttributeNames={"#selectedChallenge": "selectedChallenge", "#version": "version",
                                       "#updatedAt": "updatedAt", "#status": "status"},
            ExpressionAttributeValues={":challenge": updated_challenge, ":now": now, ":assigned": "ASSIGNED",
                                        ":expectedVersion": version, ":nextVersion": version + 1},
            ConditionExpression="#status = :assigned AND #version = :expectedVersion",
            ReturnValues="ALL_NEW",
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            raise ChallengeError("El reto activo cambió por una solicitud concurrente; vuelve a intentarlo", 409) from exc
        raise
    log_event(logger, "🟢", "Reto activo editado", couple_ref=couple_pk[-8:])
    return build_month_state(couple_pk, result["Attributes"], user), 200


def cancel_active_challenge(couple_pk, user, reason=None):
    month_record = _load_current_month(couple_pk)
    if month_record["status"] != "ASSIGNED":
        raise ChallengeError("No hay un reto activo para cancelar", 409)
    _record_history(couple_pk, month_record, user, "cancelled", reason)

    now = now_iso()
    now_dt = datetime.now(timezone.utc)
    deadline_dt = datetime.fromisoformat(month_record["deadlineAt"])
    new_deadline = month_record["deadlineAt"] if now_dt < deadline_dt else (
        now_dt + timedelta(days=REPLACEMENT_GRACE_DAYS)).isoformat()
    new_cycle = int(month_record["cycle"]) + 1
    version = int(month_record.get("version", 0))
    try:
        result = table.update_item(
            Key={"PK": couple_pk, "SK": _month_sk(month_record["monthKey"])},
            UpdateExpression=(
                "SET #status=:pending, #selectedChallenge=:none, #selectedSource=:none, #assignedAt=:none, "
                "#assignedCycle=:none, #cycle=:nextCycle, #deadlineAt=:deadline, #version=:nextVersion, "
                "#updatedAt=:now"
            ),
            ExpressionAttributeNames={"#status": "status", "#selectedChallenge": "selectedChallenge",
                                       "#selectedSource": "selectedSource", "#assignedAt": "assignedAt",
                                       "#assignedCycle": "assignedCycle", "#cycle": "cycle",
                                       "#deadlineAt": "deadlineAt", "#version": "version", "#updatedAt": "updatedAt"},
            ExpressionAttributeValues={":pending": "PENDING", ":none": None, ":nextCycle": new_cycle,
                                        ":deadline": new_deadline, ":now": now, ":assigned": "ASSIGNED",
                                        ":expectedVersion": version, ":nextVersion": version + 1},
            ConditionExpression="#status = :assigned AND #version = :expectedVersion",
            ReturnValues="ALL_NEW",
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            raise ChallengeError("El reto activo cambió por una solicitud concurrente; vuelve a intentarlo", 409) from exc
        raise
    log_event(logger, "🟢", "Reto activo cancelado", couple_ref=couple_pk[-8:], new_cycle=new_cycle)
    return result["Attributes"]


def cancel_active_challenge_endpoint(couple_pk, user, data):
    reason = data.get("reason") if isinstance(data, dict) else None
    month_record = cancel_active_challenge(couple_pk, user, reason)
    return build_month_state(couple_pk, month_record, user), 200


def replace_active_challenge(couple_pk, user, data):
    if not isinstance(data, dict):
        raise ChallengeError("El body debe ser un objeto JSON")
    reason = data.pop("reason", None) if "reason" in data else None
    month_record = cancel_active_challenge(couple_pk, user, reason or "Reemplazo de reto activo")
    # El integrante que reemplaza propone de inmediato el sustituto; la pareja debe acordarlo (misma regla de elección).
    snapshot = _snapshot_from_payload(data)
    now = now_iso()
    sk = _proposal_sk(month_record["monthKey"], month_record["cycle"], user["sub"])
    table.put_item(Item={
        "PK": couple_pk, "SK": sk, "monthKey": month_record["monthKey"], "cycle": month_record["cycle"],
        "proposedBy": user["sub"], "proposedByEmail": user["email"], "proposedByName": user.get("name"),
        **snapshot, "createdAt": now, "updatedAt": now,
    })
    log_event(logger, "🟢", "Reemplazo de reto activo propuesto", couple_ref=couple_pk[-8:])
    return build_month_state(couple_pk, month_record, user), 200


# ─── Completar reto + cupón compartido ────────────────────────────────────────


def complete_active_challenge(couple_pk, user):
    month_record = _load_current_month(couple_pk)
    if month_record["status"] == "COMPLETED":
        coupon = table.get_item(Key={"PK": couple_pk, "SK": f"CUPON#{month_record['couponId']}"}).get("Item")
        return {
            "month": build_month_state(couple_pk, month_record, user),
            "coupon": coupon,
            "couponCreated": False,
            "alreadyCompleted": True,
        }, 200
    if month_record["status"] != "ASSIGNED":
        raise ChallengeError("No hay un reto activo para completar", 409)

    challenge = month_record["selectedChallenge"]
    coupon_id = str(uuid.uuid4())
    now = now_iso()
    version = int(month_record.get("version", 0))
    coupon_item = {
        "PK": couple_pk, "SK": f"CUPON#{coupon_id}", "couponId": coupon_id,
        "title": f"Recompensa: {challenge['title']}",
        "description": challenge.get("description") or "Cupón por completar el reto mensual",
        "sourceMonthKey": month_record["monthKey"], "sourceCycle": month_record["cycle"],
        "sourceChallengeTitle": challenge["title"], "createdBy": user["sub"], "createdByEmail": user["email"],
        "createdByName": user.get("name"), "createdAt": now, "redeemed": False,
    }
    actions = [
        _update_action(
            {"PK": couple_pk, "SK": _month_sk(month_record["monthKey"])},
            "SET #status=:completed, #completedBy=:sub, #completedByEmail=:email, #completedByName=:name, "
            "#completedAt=:now, #couponId=:couponId, #version=:nextVersion, #updatedAt=:now",
            {":completed": "COMPLETED", ":sub": user["sub"], ":email": user["email"], ":name": user.get("name"),
             ":now": now, ":couponId": coupon_id, ":assigned": "ASSIGNED", ":expectedVersion": version,
             ":nextVersion": version + 1},
            {"#status": "status", "#completedBy": "completedBy", "#completedByEmail": "completedByEmail",
             "#completedByName": "completedByName", "#completedAt": "completedAt", "#couponId": "couponId",
             "#version": "version", "#updatedAt": "updatedAt"},
            "#status = :assigned AND #version = :expectedVersion",
        ),
        _put_action(coupon_item),
    ]
    try:
        _execute_transaction(actions)
        log_event(logger, "🟢", "Reto mensual completado y cupón creado", couple_ref=couple_pk[-8:],
                   coupon_id=coupon_id)
        updated_month = table.get_item(Key={"PK": couple_pk, "SK": _month_sk(month_record["monthKey"])},
                                        ConsistentRead=True)["Item"]
        return {
            "month": build_month_state(couple_pk, updated_month, user),
            "coupon": coupon_item,
            "couponCreated": True,
            "alreadyCompleted": False,
        }, 201
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "Unknown")
        if error_code in {"TransactionCanceledException", "ConditionalCheckFailedException"}:
            refreshed = table.get_item(Key={"PK": couple_pk, "SK": _month_sk(month_record["monthKey"])},
                                        ConsistentRead=True)["Item"]
            if refreshed["status"] == "COMPLETED":
                coupon = table.get_item(Key={"PK": couple_pk, "SK": f"CUPON#{refreshed['couponId']}"}).get("Item")
                return {
                    "month": build_month_state(couple_pk, refreshed, user),
                    "coupon": coupon,
                    "couponCreated": False,
                    "alreadyCompleted": True,
                }, 200
            raise ChallengeError("Conflicto al completar el reto; vuelve a intentarlo", 409) from exc
        raise


def get_current_coupon(couple_pk, user):
    month_record = _load_current_month(couple_pk)
    if not month_record.get("couponId"):
        return {"error": "Aún no se ha generado un cupón para el reto de este mes"}, 404
    coupon = table.get_item(Key={"PK": couple_pk, "SK": f"CUPON#{month_record['couponId']}"}).get("Item")
    return coupon, 200


# ─── Favoritos ────────────────────────────────────────────────────────────────


def add_favorite(couple_pk, user, data):
    snapshot = _snapshot_from_payload(data)
    favorite_id = snapshot["catalogChallengeId"] or str(uuid.uuid4())
    now = now_iso()
    item = {"PK": couple_pk, "SK": f"FAVORITO#{favorite_id}", "favoriteId": favorite_id, **snapshot,
            "markedBy": user["sub"], "markedByEmail": user["email"], "createdAt": now}
    try:
        table.put_item(Item=item, ConditionExpression=Attr("PK").not_exists())
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            raise ChallengeError("Ese reto ya está en favoritos", 409) from exc
        raise
    log_event(logger, "🟢", "Favorito agregado", couple_ref=couple_pk[-8:])
    return item, 201


def remove_favorite(couple_pk, favorite_id):
    try:
        table.delete_item(Key={"PK": couple_pk, "SK": f"FAVORITO#{favorite_id}"}, ConditionExpression=Attr("PK").exists())
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            raise ChallengeError("Favorito no encontrado", 404) from exc
        raise
    log_event(logger, "🟢", "Favorito eliminado", couple_ref=couple_pk[-8:])
    return {"message": "Favorito eliminado", "favoriteId": favorite_id}, 200


def list_favorites(couple_pk):
    items = query_all(table, KeyConditionExpression=Key("PK").eq(couple_pk) & Key("SK").begins_with("FAVORITO#"))
    return {"items": items, "count": len(items)}, 200


# ─── Construcción de la respuesta pública del mes ────────────────────────────


def _public_proposal(proposal):
    return {k: proposal.get(k) for k in ("proposedBy", "proposedByEmail", "proposedByName", "source",
                                          "catalogChallengeId", "title", "description", "category", "createdAt",
                                          "updatedAt")}


def _public_choice(choice):
    return {k: choice.get(k) for k in ("chosenBy", "chosenByEmail", "chosenProposedBy", "requestRandomTiebreak",
                                        "createdAt", "updatedAt")}


def build_month_state(couple_pk, month_record, user):
    month_key, cycle = month_record["monthKey"], month_record["cycle"]
    couple_meta = get_couple_meta(couple_pk) or {}
    proposals = _list_proposals(couple_pk, month_key, cycle) if month_record["status"] == "PENDING" else []
    choices = _list_choices(couple_pk, month_key, cycle) if month_record["status"] == "PENDING" else []

    viewer_proposal = next((p for p in proposals if p["proposedBy"] == user["sub"]), None)
    viewer_choice = next((c for c in choices if c["chosenBy"] == user["sub"]), None)
    partner_choice = next((c for c in choices if c["chosenBy"] != user["sub"]), None)

    disagreement = False
    if month_record["status"] == "PENDING" and viewer_choice and partner_choice:
        both_tiebreak = viewer_choice.get("requestRandomTiebreak") and partner_choice.get("requestRandomTiebreak")
        if not both_tiebreak and viewer_choice.get("chosenProposedBy") != partner_choice.get("chosenProposedBy"):
            disagreement = True

    available_actions = []
    if month_record["status"] == "PENDING":
        available_actions.append("withdrawProposal" if viewer_proposal else "propose")
        if viewer_proposal:
            available_actions.append("updateProposal")
        if proposals:
            available_actions.append("chooseProposal")
        if disagreement:
            available_actions.append("requestRandomTiebreak")
    elif month_record["status"] == "ASSIGNED":
        available_actions += ["completeChallenge", "editChallenge", "cancelChallenge", "replaceChallenge"]
    elif month_record["status"] == "COMPLETED":
        available_actions.append("viewCoupon")

    return {
        "monthKey": month_key,
        "cycle": cycle,
        "status": month_record["status"],
        "deadlineAt": month_record["deadlineAt"],
        "selectedChallenge": month_record.get("selectedChallenge"),
        "selectedSource": month_record.get("selectedSource"),
        "assignedAt": month_record.get("assignedAt"),
        "completedBy": month_record.get("completedBy"),
        "completedByEmail": month_record.get("completedByEmail"),
        "completedAt": month_record.get("completedAt"),
        "couponId": month_record.get("couponId"),
        "proposals": [_public_proposal(p) for p in proposals],
        "choices": [_public_choice(c) for c in choices],
        "disagreement": disagreement,
        "availableActions": available_actions,
        "viewer": {"sub": user["sub"], "email": user["email"], "name": user.get("name")},
        "partner": {"email": _partner_email(couple_meta, user["email"]), "name": _partner_name(couple_meta, user["email"])},
        "generatedAt": now_iso(),
    }


def get_month_status(couple_pk, user, month_key):
    is_current = month_key == current_month_key()
    if is_current:
        month_record = _load_current_month(couple_pk, month_key)
        return build_month_state(couple_pk, month_record, user), 200
    existing = table.get_item(Key={"PK": couple_pk, "SK": _month_sk(month_key)}, ConsistentRead=True).get("Item")
    if not existing:
        return {"error": f"El mes '{month_key}' no tiene actividad registrada"}, 404
    return build_month_state(couple_pk, existing, user), 200


# ─── Lambda handler / enrutamiento ────────────────────────────────────────────


def lambda_handler(event, context):
    method = event.get("requestContext", {}).get("http", {}).get("method", "")
    path = event.get("rawPath", "")
    logger.info(json.dumps({"level": "⚪️", "message": "Solicitud MonthlyChallengesCRUD", "method": method,
                             "path": path, "function": getattr(context, "function_name", "unknown")},
                            ensure_ascii=False))

    if method == "OPTIONS":
        return build_response(200, {})

    try:
        body = parse_body(event)
        query_params = event.get("queryStringParameters") or {}

        if method == "POST" and path == "/monthly-challenges/init":
            user = _get_request_user(event)
            required = {"user1Email", "user2Email", "user1Name", "user2Name"}
            if not isinstance(body, dict) or not all(k in body for k in required):
                return build_response(400, {"error": "Faltan campos requeridos"})
            result, status = init_couple(user, body["user1Name"], body["user2Name"], body["user1Email"],
                                          body["user2Email"])
            return build_response(status, result)

        # ──────── Catálogo (no depende de la pareja, salvo /random) ──────────
        if path == "/monthly-challenges/catalog" and method == "POST":
            user = _get_request_user(event)
            result, status = create_catalog_challenge(cast(dict, body))
            return build_response(status, result)
        if path == "/monthly-challenges/catalog" and method == "GET":
            result, status = list_catalog_challenges(query_params)
            return build_response(status, result)
        if path == "/monthly-challenges/catalog/random" and method == "GET":
            couple_pk, user = _resolve_couple_context(event)
            result, status = suggest_random_challenge(couple_pk, query_params)
            return build_response(status, result)
        challenge_id = get_path_param(event, "challengeId")
        if challenge_id and "/catalog/" in path and method == "GET":
            result, status = get_catalog_challenge(challenge_id)
            return build_response(status, result)
        if challenge_id and "/catalog/" in path and method == "PUT":
            result, status = update_catalog_challenge(challenge_id, cast(dict, body))
            return build_response(status, result)
        if challenge_id and "/catalog/" in path and method == "DELETE":
            result, status = deactivate_catalog_challenge(challenge_id)
            return build_response(status, result)

        # ──────── Rutas de pareja (requieren membresía) ──────────────────────
        couple_pk, user = _resolve_couple_context(event)

        if path == "/monthly-challenges/current" and method == "GET":
            month_record = _load_current_month(couple_pk)
            return build_response(200, build_month_state(couple_pk, month_record, user))

        month_key = get_path_param(event, "monthKey")
        if month_key and path == f"/monthly-challenges/{month_key}" and method == "GET":
            result, status = get_month_status(couple_pk, user, month_key)
            return build_response(status, result)

        if path == "/monthly-challenges/current/proposals" and method == "POST":
            result, status = create_or_update_proposal(couple_pk, user, cast(dict, body))
            return build_response(status, result)
        if path == "/monthly-challenges/current/proposals" and method == "DELETE":
            result, status = withdraw_proposal(couple_pk, user)
            return build_response(status, result)
        if path == "/monthly-challenges/current/choice" and method == "POST":
            result, status = register_choice(couple_pk, user, cast(dict, body))
            return build_response(status, result)
        if path == "/monthly-challenges/current/active" and method == "PUT":
            result, status = edit_active_challenge(couple_pk, user, cast(dict, body))
            return build_response(status, result)
        if path == "/monthly-challenges/current/cancel" and method == "POST":
            result, status = cancel_active_challenge_endpoint(couple_pk, user, cast(dict, body))
            return build_response(status, result)
        if path == "/monthly-challenges/current/replace" and method == "POST":
            result, status = replace_active_challenge(couple_pk, user, cast(dict, body))
            return build_response(status, result)
        if path == "/monthly-challenges/current/complete" and method == "POST":
            result, status = complete_active_challenge(couple_pk, user)
            return build_response(status, result)
        if path == "/monthly-challenges/current/coupon" and method == "GET":
            result, status = get_current_coupon(couple_pk, user)
            return build_response(status, result)

        if path == "/monthly-challenges/favorites" and method == "POST":
            result, status = add_favorite(couple_pk, user, cast(dict, body))
            return build_response(status, result)
        if path == "/monthly-challenges/favorites" and method == "GET":
            result, status = list_favorites(couple_pk)
            return build_response(status, result)
        favorite_id = get_path_param(event, "favoriteId")
        if favorite_id and method == "DELETE":
            result, status = remove_favorite(couple_pk, favorite_id)
            return build_response(status, result)

        return build_response(404, {"error": "Ruta no encontrada"})

    except UnauthenticatedError as exc:
        return build_response(401, {"error": str(exc)})
    except ForbiddenError as exc:
        return build_response(403, {"error": str(exc)})
    except ChallengeError as exc:
        return build_response(exc.status_code, {"error": str(exc)})
    except ValueError as exc:
        return build_response(400, {"error": str(exc)})
    except ClientError as exc:
        error_body, _ = _database_error("Error DynamoDB en MonthlyChallengesCRUD", exc)
        return build_response(502, error_body)
    except Exception:
        logger.exception("🔴 Error inesperado en MonthlyChallengesCRUD")
        return build_response(500, {"error": "Error interno del servidor"})
