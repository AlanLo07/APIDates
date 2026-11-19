"""Shared weekly questions for the two registered Cognito users."""
import hashlib
import logging
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import TypeSerializer
from botocore.exceptions import ClientError

from common.utils import build_response, log_event, parse_body, query_all

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_NAME = os.environ.get("WEEKLY_QUESTIONS_TABLE_NAME", "WeeklyQuestionsTable")
USER_POOL_ID = os.environ.get("COGNITO_USER_POOL_ID", "")
TIME_ZONE = "America/Mexico_City"
LOCAL_ZONE = ZoneInfo(TIME_ZONE)
BASE_PATH = "/weekly-questions"
MAX_ATTEMPTS = 4
MAX_TEXT = 1000
MAX_ANSWER = 5000

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)
client = boto3.client("dynamodb")
cognito = boto3.client("cognito-idp")
serializer = TypeSerializer()


class QuestionError(ValueError):
    def __init__(self, message, status_code=400):
        super().__init__(message)
        self.status_code = status_code


def _now():
    return datetime.now(timezone.utc)


def _week(now):
    local = now.astimezone(LOCAL_ZONE)
    start = (local - timedelta(days=local.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0,
    )
    return {
        "weekKey": start.date().isoformat(),
        "weekStart": start.isoformat(),
        "weekEnd": (start + timedelta(days=7)).isoformat(),
        "timeZone": TIME_ZONE,
    }


def _context(event):
    claims = event.get("requestContext", {}).get("authorizer", {}).get("jwt", {}).get("claims", {})
    sub = str(claims.get("sub") or "").strip()
    if not sub:
        raise QuestionError("Usuario no autenticado", 401)
    if not USER_POOL_ID:
        raise QuestionError("Falta configurar COGNITO_USER_POOL_ID", 503)
    users = []
    params = {"UserPoolId": USER_POOL_ID, "Limit": 3}
    while True:
        response = cognito.list_users(**params)
        for profile in response.get("Users", []):
            attrs = {entry["Name"]: entry["Value"] for entry in profile.get("Attributes", [])}
            users.append({"sub": str(attrs.get("sub") or "").strip(),
                          "name": str(attrs.get("name") or "").strip()})
        if len(users) > 2:
            raise QuestionError("Se requieren exactamente dos usuarios en Cognito", 503)
        token = response.get("PaginationToken")
        if not token:
            break
        params["PaginationToken"] = token
    subs = sorted(user["sub"] for user in users)
    if len(subs) != 2 or not all(subs) or subs[0] == subs[1]:
        raise QuestionError("Se requieren dos usuarios con identificadores diferentes", 503)
    if sub not in subs:
        raise QuestionError("El usuario no pertenece a la pareja", 403)
    digest = hashlib.sha256("|".join(subs).encode("utf-8")).hexdigest()[:24]
    return {"pk": f"COUPLE#{digest}", "sub": sub, "users": users}


def _text(value, field, maximum):
    if not isinstance(value, str) or not value.strip() or len(value.strip()) > maximum:
        raise QuestionError(f"{field} debe tener entre 1 y {maximum} caracteres")
    return value.strip()


def _question_id(value):
    if not isinstance(value, str):
        raise QuestionError("Identificador de pregunta invalido")
    try:
        normalized = str(uuid.UUID(value))
    except ValueError as exc:
        raise QuestionError("Identificador de pregunta invalido") from exc
    if value != normalized:
        raise QuestionError("Identificador de pregunta invalido")
    return normalized


def _body(event, allowed):
    try:
        data = parse_body(event)
    except ValueError as exc:
        raise QuestionError(str(exc)) from exc
    if not isinstance(data, dict):
        raise QuestionError("El body debe ser un objeto JSON")
    if set(data) - allowed:
        raise QuestionError("El body contiene campos no permitidos")
    return data


def _get(pk, sk):
    return table.get_item(Key={"PK": pk, "SK": sk}, ConsistentRead=True).get("Item")


def _catalog():
    return query_all(
        table, KeyConditionExpression=Key("PK").eq("CATALOG") & Key("SK").begins_with("QUESTION#"),
        ConsistentRead=True,
    )


def _histories(ctx):
    items = query_all(
        table, KeyConditionExpression=Key("PK").eq(ctx["pk"]) & Key("SK").begins_with("HISTORY#"),
        ConsistentRead=True,
    )
    return {item["id"]: item for item in items}


def _catalog_item(question_id):
    item = _get("CATALOG", f"QUESTION#{question_id}")
    if item is None:
        raise QuestionError("Pregunta no encontrada", 404)
    return item


def _public_catalog(item):
    return {key: value for key, value in item.items() if key not in {"PK", "SK"}}


def _conditional_conflict(exc):
    code = exc.response.get("Error", {}).get("Code")
    if code == "ConditionalCheckFailedException":
        return True
    reasons = exc.response.get("CancellationReasons", [])
    return (
        code == "TransactionCanceledException"
        and any(reason.get("Code") == "ConditionalCheckFailed" for reason in reasons)
        and all(reason.get("Code") in {None, "None", "ConditionalCheckFailed"} for reason in reasons)
    )


def _transaction(actions):
    serialized = []
    for action in actions:
        operation, detail = next(iter(action.items()))
        encoded = dict(detail, TableName=TABLE_NAME)
        for field in ("Item", "Key", "ExpressionAttributeValues"):
            if field in encoded:
                encoded[field] = {key: serializer.serialize(value) for key, value in encoded[field].items()}
        serialized.append({operation: encoded})
    client.transact_write_items(TransactItems=serialized)


def _history_put(item, previous):
    action = {"Item": item, "ConditionExpression": "attribute_not_exists(PK)"}
    if previous is not None:
        action["ConditionExpression"] = "#version = :version"
        action["ExpressionAttributeNames"] = {"#version": "version"}
        action["ExpressionAttributeValues"] = {":version": previous["version"]}
    return {"Put": action}


def _current_state(ctx, week):
    return _get(ctx["pk"], f"WEEK#{week['weekKey']}")


def _question_view(catalog_item, history, ctx, state):
    source = history["snapshot"] if history else catalog_item
    answers = history.get("answers", {}) if history else {}
    is_current = state is not None and state["questionId"] == source["id"]
    own_answer = ctx["sub"] in answers
    count = len(answers)
    status = "unseen" if history is None else ("completed" if count == 2 else "partial" if count else "pending")
    return {
        "id": source["id"], "text": source["text"], "category": source["category"],
        "catalogVersion": source["version"],
        "presented": history is not None, "locked": history is None,
        "isCurrent": is_current, "status": status,
        "canAnswer": is_current and not own_answer,
        "canEdit": own_answer,
        "answers": answers,
        "firstPresentedAt": history["firstPresentedAt"] if history else None,
        "lastPresentedWeek": history["lastPresentedWeek"] if history else None,
    }


def _current_response(week, state, history, ctx):
    return build_response(200, {
        **week, "users": ctx["users"],
        "question": _question_view(history["snapshot"], history, ctx, state) if history else None,
        "message": None if history else "No hay preguntas disponibles",
    })


def get_current(ctx):
    for _ in range(MAX_ATTEMPTS):
        now = _now()
        week = _week(now)
        state = _current_state(ctx, week)
        if state is not None:
            history = _get(ctx["pk"], f"HISTORY#{state['questionId']}")
            if history is None:
                raise RuntimeError("La seleccion semanal no tiene historial")
            return _current_response(week, state, history, ctx)
        histories = _histories(ctx)
        candidates = [
            item for item in _catalog()
            if len(histories.get(item["id"], {}).get("answers", {})) < 2
        ]
        if not candidates:
            # Do not freeze an empty week: a newly added question can be selected.
            state = _current_state(ctx, week)
            if state is not None:
                history = _get(ctx["pk"], f"HISTORY#{state['questionId']}")
                if history is None:
                    raise RuntimeError("La seleccion semanal no tiene historial")
                return _current_response(week, state, history, ctx)
            return _current_response(week, None, None, ctx)
        selected = random.choice(candidates)
        previous = histories.get(selected["id"])
        if previous is None:
            history = {
                "PK": ctx["pk"], "SK": f"HISTORY#{selected['id']}", "id": selected["id"],
                "snapshot": {key: selected[key] for key in ("id", "text", "category", "version")},
                "answers": {}, "firstPresentedAt": now.isoformat(), "version": 1,
            }
        else:
            history = {**previous, "version": previous["version"] + 1}
        history["lastPresentedWeek"] = week["weekKey"]
        state = {
            "PK": ctx["pk"], "SK": f"WEEK#{week['weekKey']}",
            "questionId": selected["id"], **week, "selectedAt": now.isoformat(),
        }
        if _week(_now())["weekKey"] != week["weekKey"]:
            continue
        try:
            _transaction([
                {"Put": {"Item": state, "ConditionExpression": "attribute_not_exists(PK)"}},
                _history_put(history, previous),
            ])
        except ClientError as exc:
            if _conditional_conflict(exc):
                continue
            raise
        log_event(logger, "🟢", "Pregunta semanal seleccionada", week=week["weekKey"])
        return _current_response(week, state, history, ctx)
    raise QuestionError("La seleccion cambio simultaneamente; vuelve a consultar", 409)


def save_catalog(ctx, data, question_id=None):
    text = _text(data.get("text"), "text", MAX_TEXT)
    category = _text(data.get("category"), "category", 60)
    now = _now().isoformat()
    if question_id is None:
        question_id = str(uuid.uuid4())
        item = {
            "PK": "CATALOG", "SK": f"QUESTION#{question_id}", "id": question_id,
            "text": text, "category": category, "version": 1,
            "createdAt": now, "updatedAt": now,
            "createdBy": ctx["sub"], "updatedBy": ctx["sub"],
        }
        condition = {"ConditionExpression": "attribute_not_exists(PK)"}
    else:
        version = data.get("version")
        if type(version) is not int or version < 1:
            raise QuestionError("version debe ser un entero positivo")
        previous = _catalog_item(question_id)
        if previous["version"] != version:
            raise QuestionError("El catalogo cambio; consulta su version actual", 409)
        item = {**previous, "text": text, "category": category, "version": version + 1,
                "updatedAt": now, "updatedBy": ctx["sub"]}
        condition = {
            "ConditionExpression": "#version = :version",
            "ExpressionAttributeNames": {"#version": "version"},
            "ExpressionAttributeValues": {":version": version},
        }
    try:
        table.put_item(Item=item, **condition)
    except ClientError as exc:
        if _conditional_conflict(exc):
            raise QuestionError("El catalogo cambio; consulta su version actual", 409) from exc
        raise
    log_event(logger, "🟢", "Pregunta de catalogo guardada")
    return build_response(201 if item["version"] == 1 else 200, _public_catalog(item))


def save_answer(ctx, question_id, data):
    text = _text(data.get("text"), "text", MAX_ANSWER)
    catalog_item = _catalog_item(question_id)
    for _ in range(MAX_ATTEMPTS):
        history = _get(ctx["pk"], f"HISTORY#{question_id}")
        if history is None:
            raise QuestionError("La pregunta todavia no se ha presentado", 403)
        now = _now()
        week = _week(now)
        state = _current_state(ctx, week)
        previous_answer = history["answers"].get(ctx["sub"])
        if previous_answer is None and (state is None or state["questionId"] != question_id):
            raise QuestionError("Solo puedes crear una respuesta para la pregunta de la semana", 403)
        answer = {
            "text": text, "createdAt": previous_answer["createdAt"] if previous_answer else now.isoformat(),
            "updatedAt": now.isoformat(),
        }
        updated = {
            **history, "version": history["version"] + 1,
            "answers": {**history["answers"], ctx["sub"]: answer},
        }
        actions = [_history_put(updated, history)]
        if previous_answer is None:
            actions.append({"ConditionCheck": {
                "Key": {"PK": ctx["pk"], "SK": f"WEEK#{week['weekKey']}"},
                "ConditionExpression": "questionId = :question",
                "ExpressionAttributeValues": {":question": question_id},
            }})
            if _week(_now())["weekKey"] != week["weekKey"]:
                continue
        try:
            _transaction(actions)
        except ClientError as exc:
            if _conditional_conflict(exc):
                continue
            raise
        log_event(logger, "🟢", "Respuesta semanal guardada", editing=previous_answer is not None)
        return build_response(
            201 if previous_answer is None else 200,
            _question_view(catalog_item, updated, ctx, state),
        )
    raise QuestionError("Las respuestas cambiaron simultaneamente; vuelve a intentar", 409)


def _paginate(items, params):
    raw_limit = params.get("limit", "20")
    if (not isinstance(raw_limit, str) or len(raw_limit) > 3
            or not raw_limit.isascii() or not raw_limit.isdigit()):
        raise QuestionError("limit debe ser un entero entre 1 y 100")
    limit = int(raw_limit)
    if not 1 <= limit <= 100:
        raise QuestionError("limit debe ser un entero entre 1 y 100")
    after = _question_id(params["nextToken"]) if "nextToken" in params else None
    ordered = sorted((item for item in items if after is None or item["id"] > after),
                     key=lambda item: item["id"])
    page = ordered[:limit]
    return build_response(200, {
        "items": page, "nextToken": page[-1]["id"] if len(ordered) > limit else None,
    })


def list_questions(ctx, params, catalog_only=False):
    category = _text(params["category"], "category", 60) if "category" in params else None
    items = _catalog()
    if catalog_only:
        return _paginate(
            [_public_catalog(item) for item in items if category is None or item["category"] == category],
            params,
        )
    status = params.get("status")
    if status is not None and status not in {"unseen", "pending", "partial", "completed"}:
        raise QuestionError("status debe ser unseen, pending, partial o completed")
    pending_for = params.get("pendingFor")
    if pending_for is not None and pending_for not in {"me", "partner"}:
        raise QuestionError("pendingFor debe ser me o partner")
    histories = _histories(ctx)
    state = _current_state(ctx, _week(_now()))
    target = ctx["sub"] if pending_for == "me" else next(
        user["sub"] for user in ctx["users"] if user["sub"] != ctx["sub"]
    )
    views = []
    for item in items:
        view = _question_view(item, histories.get(item["id"]), ctx, state)
        if category is not None and view["category"] != category:
            continue
        if status is not None and view["status"] != status:
            continue
        if pending_for is not None and (not view["presented"] or target in view["answers"]):
            continue
        views.append(view)
    return _paginate(views, params)


def _route(event, ctx, method, path):
    params = event.get("queryStringParameters") or {}
    if path == f"{BASE_PATH}/catalog":
        if method == "GET":
            return list_questions(ctx, params, catalog_only=True)
        if method == "POST":
            return save_catalog(ctx, _body(event, {"text", "category"}))
    elif path == f"{BASE_PATH}/current":
        if method == "GET":
            return get_current(ctx)
    elif path == BASE_PATH:
        if method == "GET":
            return list_questions(ctx, params)
    elif path.startswith(f"{BASE_PATH}/catalog/"):
        suffix = path[len(f"{BASE_PATH}/catalog/"):]
        if "/" in suffix:
            raise QuestionError("Ruta no encontrada", 404)
        question_id = _question_id(suffix)
        if method == "GET":
            return build_response(200, _public_catalog(_catalog_item(question_id)))
        if method == "PUT":
            return save_catalog(ctx, _body(event, {"text", "category", "version"}), question_id)
    elif path.startswith(f"{BASE_PATH}/"):
        parts = path[len(BASE_PATH) + 1:].split("/")
        if len(parts) == 1:
            question_id = _question_id(parts[0])
            if method == "GET":
                item = _catalog_item(question_id)
                history = _get(ctx["pk"], f"HISTORY#{question_id}")
                state = _current_state(ctx, _week(_now()))
                return build_response(200, _question_view(item, history, ctx, state))
        elif len(parts) == 2 and parts[1] == "answer":
            question_id = _question_id(parts[0])
            if method == "PUT":
                return save_answer(ctx, question_id, _body(event, {"text"}))
        else:
            raise QuestionError("Ruta no encontrada", 404)
    else:
        raise QuestionError("Ruta no encontrada", 404)
    raise QuestionError("Metodo no permitido", 405)


def lambda_handler(event, context):
    method = event.get("requestContext", {}).get("http", {}).get("method", "").upper()
    path = event.get("rawPath", "")
    log_event(logger, "⚪️", "Solicitud WeeklyQuestionsCRUD", method=method)
    try:
        ctx = _context(event)
        response = _route(event, ctx, method, path)
        response["headers"]["Cache-Control"] = "no-store"
        return response
    except QuestionError as exc:
        log_event(logger, "🟡", "Solicitud de preguntas rechazada", status=exc.status_code, reason=str(exc))
        return build_response(exc.status_code, {"error": str(exc)})
    except ClientError as exc:
        log_event(logger, "🔴", "Error AWS en preguntas semanales",
                  error_code=exc.response.get("Error", {}).get("Code", "Unknown"))
        return build_response(503, {"error": "No fue posible acceder al servicio; vuelve a intentar"})
    except Exception:
        logger.exception("Error inesperado en WeeklyQuestionsCRUD")
        return build_response(500, {"error": "No fue posible procesar la solicitud"})
