"""
finances_handler.py — CRUD para administración de finanzas de una pareja.

Modelo de finanzas compartidas:
- Una tabla DynamoDB con estructura PK/SK compuesta.
- PK por pareja: PAREJA#{coupleId}, resuelta desde los claims de Cognito.
- Compatibilidad controlada con la partición histórica PAREJA#DEFAULT.
- Colecciones:
  - SK=META: Datos de la pareja y configuración
  - SK=GASTO#{gastoId}: Transacciones individuales
  - SK=PRESUPUESTO#{monthYear}: Presupuestos mensuales (YYYY-MM)
  - SK=HISTORICO#{monthYear}: Datos históricos calculados

Rutas principales (sin parejaId):
- GET /finances - Obtener datos de pareja + resumen
- PUT /finances - Actualizar datos de pareja
- GET/POST /finances/gastos
- GET/PUT/DELETE /finances/gastos/{gastoId}
- POST/GET /finances/presupuesto/{monthYear}
- GET /finances/historico
- GET /finances/historico/{monthYear}

Características:
✅ Aislamiento por pareja y membresía autenticada
✅ Validación de estructura de datos
✅ Cálculo automático de presupuestos y históricos
✅ Filtrado por mes y categoría
✅ Auditoría de cambios (createdAt, updatedAt)
"""
import json
import hashlib
import logging
import os
import re
import uuid
import base64
import binascii
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, cast

import boto3
from boto3.dynamodb.conditions import Attr, Key
from boto3.dynamodb.types import TypeSerializer
from botocore.config import Config
from botocore.exceptions import ClientError

from common.utils import build_response, get_path_param, log_event, parse_body, query_all  # type: ignore

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
dynamodb_client = cast(Any, dynamodb.meta).client
cognito = boto3.client("cognito-idp")
TABLE_NAME = os.environ.get("FINANCES_TABLE_NAME", "FinancesTable")
table = dynamodb.Table(TABLE_NAME)  # type: ignore
serializer = TypeSerializer()

# ─── Constantes Globales ──────────────────────────────────────────────────────

LEGACY_COUPLE_ID = "DEFAULT"
LEGACY_FINANCES_EMAILS = frozenset(
    email.strip().casefold()
    for email in os.environ.get("FINANCES_LEGACY_COUPLE_EMAILS", "").split(",")
    if email.strip()
)
DEFAULT_CURRENCY = "COP"

EXPENSE_CATEGORIES = {
    "subscriptions": {
        "label": "Suscripciones",
        "icon": "subscriptions_rounded",
        "color": "#6A88D6",
        "suggestions": ["Netflix", "Spotify Duo", "Google One", "Canva Pro"],
    },
    "groceries": {
        "label": "Supermercado",
        "icon": "shopping_basket_rounded",
        "color": "#4CAF50",
        "suggestions": ["Supermercado semanal", "Mercado de frutas", "Productos de limpieza"],
    },
    "transport": {
        "label": "Transporte",
        "icon": "directions_car_rounded",
        "color": "#42A5F5",
        "suggestions": ["Gasolina", "Uber", "Parqueadero", "Peajes"],
    },
    "dateNights": {
        "label": "Citas y salidas",
        "icon": "wine_bar_rounded",
        "color": "#E57373",
        "suggestions": ["Cena aniversario", "Cine", "Cafe y postres", "Salida de fin de semana"],
    },
    "home": {
        "label": "Casa",
        "icon": "home_rounded",
        "color": "#8D6E63",
        "suggestions": ["Arriendo", "Servicios", "Internet hogar", "Mantenimiento"],
    },
    "health": {
        "label": "Salud y bienestar",
        "icon": "spa_rounded",
        "color": "#26A69A",
        "suggestions": ["Farmacia", "Consulta medica", "Gimnasio", "Vitaminas"],
    },
    "vacations": {
        "label": "Vacaciones",
        "icon": "flight_takeoff_rounded",
        "color": "#FF8A65",
        "suggestions": ["Reserva hotel", "Tiquetes", "Tour", "Fondo viaje"],
    },
    "gifts": {
        "label": "Regalos",
        "icon": "card_giftcard_rounded",
        "color": "#AB47BC",
        "suggestions": ["Cumpleanos", "Aniversario", "Detalle sorpresa", "Flores"],
    },
    "pets": {
        "label": "Mascotas",
        "icon": "pets_rounded",
        "color": "#8D6E63",
        "suggestions": ["Concentrado", "Veterinario", "Bano y peluqueria", "Juguetes"],
    },
    "hobbies": {
        "label": "Gustos personales",
        "icon": "favorite_rounded",
        "color": "#E91E63",
        "suggestions": ["Videojuego", "Libro", "Ropa", "Curso online"],
    },
    "savings": {
        "label": "Ahorro",
        "icon": "savings_rounded",
        "color": "#5C6BC0",
        "suggestions": ["Ahorro emergencia", "Meta carro", "Meta apartamento", "Fondo boda"],
    },
    "others": {
        "label": "Otros",
        "icon": "receipt_long_rounded",
        "color": "#8D6E63",
        "suggestions": ["Imprevisto", "Comision bancaria", "Pago pendiente", "Otro gasto"],
    },
}

REQUIRED_EXPENSE_FIELDS = {"title", "amount", "date", "category"}
REQUIRED_PARTNER_FIELDS = {"email", "name"}
EDITABLE_EXPENSE_FIELDS = {"title", "amount", "date", "category", "note"}
MONTH_YEAR_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
SAVINGS_CONFIG_SK = "AHORROS#CONFIG"
SAVINGS_CUSHION_ID = "COLCHON"
SAVINGS_CUSHION_SK = f"META_AHORRO#{SAVINGS_CUSHION_ID}"
SAVINGS_GOAL_PREFIX = "META_AHORRO#"
SAVINGS_MOVEMENT_PREFIX = "AHORRO_MOVIMIENTO#"
SAVINGS_OPERATION_PREFIX = "AHORRO_OPERACION#"
SAVINGS_IDEMPOTENCY_PREFIX = "AHORRO_IDEMPOTENCIA#"
MAX_SAVINGS_RETRIES = 5
MAX_HISTORY_PAGE_SIZE = 100


class UnauthenticatedError(PermissionError):
    pass


class ForbiddenError(PermissionError):
    pass


class SavingsError(ValueError):
    def __init__(self, message, status_code=400):
        super().__init__(message)
        self.status_code = status_code

# ─── Utilidades ───────────────────────────────────────────────────────────────


def get_iso_timestamp():
    """🔵 Devuelve timestamp ISO 8601 UTC."""
    return datetime.now(timezone.utc).isoformat()


def to_decimal(value):
    """Convierte montos a Decimal sin perder precisión monetaria."""
    return Decimal(str(value))


def _normalize_email(value):
    return str(value or "").strip().casefold()


def _get_request_user(event):
    claims = (
        event.get("requestContext", {})
        .get("authorizer", {})
        .get("jwt", {})
        .get("claims", {})
    )
    user_id = str(claims.get("sub") or "").strip()
    email = _normalize_email(claims.get("email"))
    if not user_id:
        raise UnauthenticatedError("Usuario no autenticado")
    return {
        "sub": user_id,
        "email": email,
        "name": str(claims.get("name") or "").strip(),
    }


def _membership_key(prefix, identifier):
    return {"PK": f"{prefix}#{identifier}", "SK": "PAREJA"}


def _email_from_access_token(event):
    headers = event.get("headers", {}) or {}
    authorization = next(
        (value for key, value in headers.items() if key.lower() == "authorization"),
        "",
    )
    scheme, separator, access_token = str(authorization).partition(" ")
    if not separator or scheme.casefold() != "bearer" or not access_token.strip():
        return ""
    try:
        profile = cognito.get_user(AccessToken=access_token.strip())
    except ClientError as exc:
        log_event(
            logger,
            "🟡",
            "No se pudieron recuperar atributos de Cognito",
            error_code=exc.response.get("Error", {}).get("Code", "Unknown"),
        )
        return ""
    attributes = {
        attribute["Name"]: attribute["Value"]
        for attribute in profile.get("UserAttributes", [])
    }
    return _normalize_email(attributes.get("email"))


def _save_user_membership(user, couple_id):
    now = get_iso_timestamp()
    item = {
        **_membership_key("USUARIO", user["sub"]),
        "coupleId": couple_id,
        "createdAt": now,
        "updatedAt": now,
    }
    if user.get("email"):
        item["email"] = user["email"]
    try:
        table.put_item(
            Item=item,
            ConditionExpression=Attr("PK").not_exists() | Attr("coupleId").eq(couple_id),
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            raise ForbiddenError("El usuario ya pertenece a otra pareja") from exc
        raise


def _resolve_couple_context(event):
    user = _get_request_user(event)
    user_membership = table.get_item(
        Key=_membership_key("USUARIO", user["sub"]),
        ConsistentRead=True,
    ).get("Item")
    if user_membership and not user["email"]:
        user["email"] = user["email"] or _normalize_email(user_membership.get("email"))
    if not user["email"]:
        user["email"] = _email_from_access_token(event)

    if user["email"] in LEGACY_FINANCES_EMAILS:
        legacy_meta = table.get_item(
            Key={"PK": f"PAREJA#{LEGACY_COUPLE_ID}", "SK": "META"},
            ConsistentRead=True,
        ).get("Item")
        if legacy_meta:
            return f"PAREJA#{LEGACY_COUPLE_ID}", user

    if user_membership:
        return f"PAREJA#{user_membership['coupleId']}", user

    if user["email"]:
        email_membership = table.get_item(
            Key=_membership_key("EMAIL", user["email"]),
            ConsistentRead=True,
        ).get("Item")
        if email_membership:
            couple_id = email_membership["coupleId"]
            _save_user_membership(user, couple_id)
            return f"PAREJA#{couple_id}", user

        if user["email"] in _legacy_default_emails():
            _save_user_membership(user, LEGACY_COUPLE_ID)
            return f"PAREJA#{LEGACY_COUPLE_ID}", user

    raise ForbiddenError("El usuario no pertenece a una pareja de finanzas")


def _couple_id_for_emails(user1_email, user2_email):
    normalized = sorted((_normalize_email(user1_email), _normalize_email(user2_email)))
    digest = hashlib.sha256("|".join(normalized).encode("utf-8")).hexdigest()
    return digest[:24]


def _legacy_default_items():
    return query_all(
        table,
        KeyConditionExpression=Key("PK").eq(f"PAREJA#{LEGACY_COUPLE_ID}"),
    )


def _legacy_default_emails():
    legacy_items = _legacy_default_items()
    emails = set()
    for item in legacy_items:
        for partner in ("user1", "user2"):
            emails.add(_normalize_email(item.get(partner, {}).get("email")))
        for field in ("createdBy", "createdByEmail"):
            emails.add(_normalize_email(item.get(field)))
    emails.discard("")
    if legacy_items:
        emails.update(LEGACY_FINANCES_EMAILS)
    return emails


def _database_error(message, exc):
    log_event(
        logger,
        "🔴",
        message,
        error_code=exc.response.get("Error", {}).get("Code", "Unknown"),
    )
    return {"error": "Error de base de datos"}, 500


def validate_expense(expense_data):
    """🔵 Valida estructura de gasto."""
    errors = []
    for field in REQUIRED_EXPENSE_FIELDS:
        if field not in expense_data or expense_data[field] is None:
            errors.append(f"Campo requerido faltante: {field}")

    # Validar monto
    try:
        amount = to_decimal(expense_data.get("amount", 0))
        if amount <= 0:
            errors.append("El monto debe ser mayor a 0")
    except (InvalidOperation, ValueError, TypeError):
        errors.append("Monto inválido")

    # Validar categoría
    category = expense_data.get("category", "")
    if category not in EXPENSE_CATEGORIES:
        errors.append(f"Categoría inválida: {category}")

    # Validar fecha
    try:
        datetime.fromisoformat(expense_data.get("date", "").replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        errors.append("Fecha debe estar en formato ISO 8601")

    return errors


def validate_budget(budget_data):
    """🔵 Valida estructura de presupuesto."""
    errors = []
    if "monthYear" not in budget_data:
        errors.append("monthYear es requerido (YYYY-MM)")
    elif not MONTH_YEAR_PATTERN.fullmatch(str(budget_data["monthYear"])):
        errors.append("monthYear debe tener formato YYYY-MM")
    if "amount" not in budget_data or budget_data["amount"] is None:
        errors.append("amount es requerido")

    try:
        amount = to_decimal(budget_data.get("amount", 0))
        if amount < 0:
            errors.append("El presupuesto no puede ser negativo")
    except (InvalidOperation, ValueError, TypeError):
        errors.append("Presupuesto debe ser un número válido")

    return errors


def get_month_year_from_date(date_str):
    """🔵 Extrae YYYY-MM de fecha ISO."""
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m")
    except (AttributeError, TypeError, ValueError):
        return None


def calculate_monthly_stats(couple_pk, month_year):
    """🔵 Calcula estadísticas mensuales de la pareja."""
    try:
        items = query_all(table,
            KeyConditionExpression=Key("PK").eq(couple_pk)
            & Key("SK").begins_with("GASTO#"),
            ProjectionExpression="SK,amount,category,#date",
            ExpressionAttributeNames={"#date": "date"},
        )
        month_expenses = [
            item
            for item in items
            if get_month_year_from_date(item.get("date", "")) == month_year
        ]

        total_spent = sum(
            (to_decimal(item["amount"]) for item in month_expenses),
            Decimal("0"),
        )

        # Agrupar por categoría
        by_category = {}
        for item in month_expenses:
            cat = item.get("category", "others")
            by_category[cat] = by_category.get(cat, Decimal("0")) + to_decimal(item["amount"])

        # Obtener presupuesto
        budget_response = table.get_item(
            Key={"PK": couple_pk, "SK": f"PRESUPUESTO#{month_year}"}
        )
        budget_amount = to_decimal(
            budget_response.get("Item", {}).get("amount", 0)
        )

        over_budget = total_spent > budget_amount if budget_amount > 0 else False

        return {
            "monthYear": month_year,
            "totalSpent": total_spent,
            "budgetAmount": budget_amount,
            "byCategory": by_category,
            "expenseCount": len(month_expenses),
            "overBudget": over_budget,
            "difference": budget_amount - total_spent if budget_amount > 0 else Decimal("0"),
            "calculatedAt": get_iso_timestamp(),
        }
    except ClientError as e:
        log_event(
            logger,
            "🔴",
            "Error calculando estadísticas",
            error_code=e.response.get("Error", {}).get("Code", "Unknown"),
        )
        return None


def persist_monthly_stats(couple_pk, month_year):
    """Persiste la vista mensual derivada sin invalidar la operación principal."""
    if not month_year:
        return
    stats = calculate_monthly_stats(couple_pk, month_year)
    if not stats:
        return
    try:
        table.put_item(
            Item={
                "PK": couple_pk,
                "SK": f"HISTORICO#{month_year}",
                **stats,
            }
        )
    except ClientError as exc:
        log_event(
            logger,
            "🟡",
            "No se pudo actualizar histórico mensual",
            month_year=month_year,
            error_code=exc.response.get("Error", {}).get("Code", "Unknown"),
        )


# ─── Operaciones de Parejas ───────────────────────────────────────────────────

def init_couple(user, user1_name, user2_name, user1_email, user2_email):
    """🟢 Inicializa una pareja y sus membresías."""
    now = get_iso_timestamp()
    normalized_emails = {_normalize_email(user1_email), _normalize_email(user2_email)}
    if "" in normalized_emails or len(normalized_emails) != 2:
        return {"error": "Los correos de los integrantes deben ser válidos y diferentes"}, 400
    if not user["email"] or user["email"] not in normalized_emails:
        return {"error": "El usuario autenticado debe ser uno de los integrantes"}, 403

    legacy_items = _legacy_default_items()
    legacy_owner_emails = _legacy_default_emails() - LEGACY_FINANCES_EMAILS
    configured_pair = len(LEGACY_FINANCES_EMAILS) == 2 and normalized_emails == LEGACY_FINANCES_EMAILS
    adopts_legacy_partition = bool(legacy_items) and (
        configured_pair or bool(normalized_emails & legacy_owner_emails)
    )
    couple_id = (
        LEGACY_COUPLE_ID
        if adopts_legacy_partition
        else _couple_id_for_emails(user1_email, user2_email)
    )
    couple_pk = f"PAREJA#{couple_id}"

    try:
        couple_item = {
                "PK": couple_pk,
                "SK": "META",
                "coupleId": couple_id,
                "user1": {
                    "name": user1_name,
                    "email": _normalize_email(user1_email),
                },
                "user2": {
                    "name": user2_name,
                    "email": _normalize_email(user2_email),
                },
                "monthlyBudget": Decimal("0"),
                "currency": DEFAULT_CURRENCY,
                "locale": "es_ES",
                "createdAt": now,
                "updatedAt": now,
        }
        membership_items = [
            {
                    **_membership_key("EMAIL", email),
                    "coupleId": couple_id,
                    "createdAt": now,
                    "updatedAt": now,
            }
            for email in normalized_emails
        ]
        user_membership = {
            **_membership_key("USUARIO", user["sub"]),
            "coupleId": couple_id,
            "createdAt": now,
            "updatedAt": now,
        }
        if user.get("email"):
            user_membership["email"] = user["email"]
        membership_items.append(user_membership)
        transaction_items = []
        if not table.get_item(
            Key={"PK": couple_pk, "SK": "META"}, ConsistentRead=True
        ).get("Item"):
            transaction_items.append(couple_item)
        for item in membership_items:
            existing_membership = table.get_item(
                Key={"PK": item["PK"], "SK": item["SK"]}, ConsistentRead=True
            ).get("Item")
            if not existing_membership:
                transaction_items.append(item)
            elif existing_membership.get("coupleId") != couple_id and not adopts_legacy_partition:
                return {"error": "El usuario ya pertenece a otra pareja"}, 409
        if transaction_items:
            dynamodb_client.transact_write_items(
                TransactItems=[
                {
                    "Put": {
                        "TableName": TABLE_NAME,
                        "Item": {key: serializer.serialize(value) for key, value in item.items()},
                        "ConditionExpression": "attribute_not_exists(PK)",
                    }
                }
                for item in transaction_items
                ]
            )

        log_event(logger, "🟢", "Pareja inicializada", couple_ref=couple_id[-8:])
        return {
            "coupleId": couple_id,
            "user1": {"name": user1_name, "email": user1_email},
            "user2": {"name": user2_name, "email": user2_email},
            "currency": DEFAULT_CURRENCY,
            "createdAt": now,
        }, 201
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in {
            "ConditionalCheckFailedException",
            "TransactionCanceledException",
        }:
            return {"error": "La pareja o uno de sus integrantes ya está registrado"}, 409
        log_event(
            logger,
            "🔴",
            "Error inicializando pareja",
            error_code=e.response.get("Error", {}).get("Code", "Unknown"),
        )
        raise


def get_couple(couple_pk):
    """🟢 Obtiene datos de la pareja autorizada."""
    try:
        response = table.get_item(Key={"PK": couple_pk, "SK": "META"})
        if "Item" not in response:
            return None
        return response["Item"]
    except ClientError as e:
        log_event(
            logger,
            "🔴",
            "Error obteniendo pareja",
            error_code=e.response.get("Error", {}).get("Code", "Unknown"),
        )
        raise


# ─── Operaciones de Gastos ────────────────────────────────────────────────────


def create_expense(couple_pk, title, amount, date, category, note=None, created_by=None, created_by_email=None):
    """🟢 Crea un nuevo gasto."""
    errors = validate_expense(
        {"title": title, "amount": amount, "date": date, "category": category}
    )
    if errors:
        return {"error": errors}, 400

    gasto_id = str(uuid.uuid4())
    now = get_iso_timestamp()
    month_year = get_month_year_from_date(date)

    try:
        expense_item = {
            "PK": couple_pk,
            "SK": f"GASTO#{gasto_id}",
            "gastoId": gasto_id,
            "title": title,
            "amount": Decimal(str(amount)),
            "date": date,
            "category": category,
            "monthYear": month_year,
            "note": note,
            "createdBy": created_by or "unknown",
            "createdByEmail": created_by_email,
            "createdAt": now,
            "updatedAt": now,
        }

        table.put_item(Item=expense_item)

        persist_monthly_stats(couple_pk, month_year)

        log_event(logger, "🟢", "Gasto creado", gasto_id=gasto_id, month_year=month_year)
        return expense_item, 201
    except ClientError as e:
        return _database_error("Error creando gasto", e)


def list_expenses(couple_pk, month_year=None, category=None):
    """🟢 Lista gastos con filtros opcionales."""
    try:
        expenses = query_all(table,
            KeyConditionExpression=Key("PK").eq(couple_pk)
            & Key("SK").begins_with("GASTO#"),
            ScanIndexForward=False,  # Más recientes primero
        )

        # Filtrar por mes si se especifica
        if month_year:
            expenses = [e for e in expenses if e.get("monthYear") == month_year]

        # Filtrar por categoría si se especifica
        if category:
            expenses = [e for e in expenses if e.get("category") == category]

        expenses.sort(key=lambda expense: expense.get("date", ""), reverse=True)

        return expenses, 200
    except ClientError as e:
        return _database_error("Error listando gastos", e)


def get_expense(couple_pk, gasto_id):
    """🟢 Obtiene un gasto específico."""
    try:
        response = table.get_item(
            Key={"PK": couple_pk, "SK": f"GASTO#{gasto_id}"}
        )
        if "Item" not in response:
            return None, 404
        return response["Item"], 200
    except ClientError as e:
        return _database_error("Error obteniendo gasto", e)


def update_expense(couple_pk, gasto_id, update_data):
    """🟢 Actualiza un gasto existente."""
    try:
        unknown_fields = set(update_data) - EDITABLE_EXPENSE_FIELDS
        if unknown_fields:
            return {
                "error": f"Campos no editables: {', '.join(sorted(unknown_fields))}"
            }, 400

        # Obtener el gasto actual
        current, status = get_expense(couple_pk, gasto_id)
        if status != 200:
            return current, status

        # Validar datos si se incluyen campos críticos
        if any(k in update_data for k in ["title", "amount", "date", "category"]):
            validation_data = {
                "title": update_data.get("title", current["title"]), # type: ignore
                "amount": update_data.get("amount", current["amount"]), # type: ignore
                "date": update_data.get("date", current["date"]), # type: ignore
                "category": update_data.get("category", current["category"]), # type: ignore
            }
            errors = validate_expense(validation_data)
            if errors:
                return {"error": errors}, 400

        now = get_iso_timestamp()
        requested_updates = dict(update_data)
        requested_updates["updatedAt"] = now

        # Construir expresión de actualización
        update_expr_parts = []
        expr_values = {}
        expr_names = {}

        for key, value in requested_updates.items():
            if key == "date":
                new_month = get_month_year_from_date(value)
                expr_names["#monthYear"] = "monthYear"
                expr_values[":monthYear"] = new_month
                update_expr_parts.append("#monthYear = :monthYear")

            expr_names[f"#{key}"] = key
            expr_values[f":{key}"] = (
                Decimal(str(value))
                if key == "amount"
                else value
            )
            update_expr_parts.append(f"#{key} = :{key}")

        if not update_expr_parts:
            return current, 200

        response = table.update_item(
            Key={"PK": couple_pk, "SK": f"GASTO#{gasto_id}"},
            UpdateExpression="SET " + ", ".join(update_expr_parts),
            ExpressionAttributeNames=expr_names,
            ExpressionAttributeValues=expr_values,
            ConditionExpression=Attr("PK").exists() & Attr("SK").exists(),
            ReturnValues="ALL_NEW",
        )

        if "date" in update_data or "amount" in update_data or "category" in update_data:
            affected_months = {
                current.get("monthYear"), # type: ignore
                response["Attributes"].get("monthYear"),
            }
            for affected_month in affected_months:
                persist_monthly_stats(couple_pk, affected_month)

        log_event(logger, "🟢", "Gasto actualizado", gasto_id=gasto_id)
        return response["Attributes"], 200
    except ClientError as e:
        return _database_error("Error actualizando gasto", e)


def delete_expense(couple_pk, gasto_id):
    """🟢 Elimina un gasto."""
    try:
        # Obtener el gasto para conocer su mes
        expense, status = get_expense(couple_pk, gasto_id)
        if status != 200:
            return expense, status

        month_year = expense.get("monthYear") # type: ignore

        table.delete_item(
            Key={"PK": couple_pk, "SK": f"GASTO#{gasto_id}"},
            ConditionExpression=Attr("PK").exists() & Attr("SK").exists(),
        )

        # Recalcular estadísticas mensuales
        if month_year:
            persist_monthly_stats(couple_pk, month_year)

        log_event(logger, "🟢", "Gasto eliminado", gasto_id=gasto_id)
        return {"message": "Gasto eliminado exitosamente"}, 200
    except ClientError as e:
        return _database_error("Error eliminando gasto", e)


# ─── Operaciones de Presupuestos ──────────────────────────────────────────────


def set_budget(couple_pk, month_year, amount, notes=None):
    """🟢 Establece o actualiza presupuesto mensual."""
    errors = validate_budget({"monthYear": month_year, "amount": amount})
    if errors:
        return {"error": errors}, 400

    now = get_iso_timestamp()

    try:
        table.update_item(
            Key={"PK": couple_pk, "SK": f"PRESUPUESTO#{month_year}"},
            UpdateExpression=(
                "SET #monthYear = :monthYear, #amount = :amount, #notes = :notes, "
                "#createdAt = if_not_exists(#createdAt, :now), #updatedAt = :now"
            ),
            ExpressionAttributeNames={
                "#monthYear": "monthYear",
                "#amount": "amount",
                "#notes": "notes",
                "#createdAt": "createdAt",
                "#updatedAt": "updatedAt",
            },
            ExpressionAttributeValues={
                ":monthYear": month_year,
                ":amount": Decimal(str(amount)),
                ":notes": notes,
                ":now": now,
            },
        )

        persist_monthly_stats(couple_pk, month_year)

        log_event(logger, "🟢", "Presupuesto establecido", month_year=month_year)
        return {
            "monthYear": month_year,
            "amount": Decimal(str(amount)),
            "updatedAt": now,
        }, 200
    except ClientError as e:
        return _database_error("Error estableciendo presupuesto", e)


def get_budget(couple_pk, month_year):
    """🟢 Obtiene presupuesto de un mes."""
    try:
        response = table.get_item(
            Key={"PK": couple_pk, "SK": f"PRESUPUESTO#{month_year}"}
        )
        if "Item" not in response:
            return None, 404
        return response["Item"], 200
    except ClientError as e:
        return _database_error("Error obteniendo presupuesto", e)


# ─── Operaciones de Históricos ────────────────────────────────────────────────


def get_monthly_history(couple_pk, month_year):
    """🟢 Obtiene estadísticas de un mes específico."""
    try:
        response = table.get_item(
            Key={"PK": couple_pk, "SK": f"HISTORICO#{month_year}"}
        )
        if "Item" not in response:
            # Si no existe, calcularlo
            stats = calculate_monthly_stats(couple_pk, month_year)
            if not stats:
                return {"error": "No se pudo calcular el histórico mensual"}, 500
            return stats, 200
        return response["Item"], 200
    except ClientError as e:
        return _database_error("Error obteniendo histórico", e)


def get_all_history(couple_pk, limit=12):
    """🟢 Obtiene histórico de últimos N meses."""
    try:
        items = query_all(table,
            KeyConditionExpression=Key("PK").eq(couple_pk)
            & Key("SK").begins_with("HISTORICO#"),
            ScanIndexForward=False,  # Más recientes primero
            Limit=limit,
        )
        return items, 200
    except ClientError as e:
        return _database_error("Error obteniendo histórico", e)


# ─── Operaciones de Resumen ───────────────────────────────────────────────────


def get_summary(couple_pk):
    """🔵 Obtiene resumen completo de finanzas (muy relevante)."""
    try:
        couple = get_couple(couple_pk)
        if not couple:
            return {"error": "Pareja no inicializada"}, 404

        # Obtener todos los gastos
        expenses, _ = list_expenses(couple_pk)
        expenses = cast(list[dict], expenses)

        # Gasto total
        total_spent = sum(
            (to_decimal(expense["amount"]) for expense in expenses),
            Decimal("0"),
        )

        # Gasto de esta semana (últimos 7 días)
        from datetime import timedelta

        now = datetime.now(timezone.utc)
        week_ago = (now - timedelta(days=7)).isoformat()
        weekly_spent = sum(
            (to_decimal(expense["amount"]) for expense in expenses
             if expense.get("date", "") >= week_ago),
            Decimal("0"),
        )

        # Meses disponibles
        months = sorted(set(e.get("monthYear") for e in expenses if e.get("monthYear"))) # type: ignore

        # Estadísticas por categoría
        by_category = {}
        for expense in expenses:
            cat = expense.get("category", "others")
            by_category[cat] = by_category.get(cat, Decimal("0")) + to_decimal(expense["amount"])

        return {
            "couple": couple,
            "totalSpent": total_spent,
            "weeklySpent": weekly_spent,
            "expenseCount": len(expenses),
            "availableMonths": months,
            "byCategory": by_category,
            "expenseCategories": EXPENSE_CATEGORIES,
        }, 200
    except ClientError as e:
        return _database_error("Error obteniendo resumen", e)


# ─── Metas de ahorro compartidas ─────────────────────────────────────────────


def _money(value, field_name="amount"):
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise SavingsError(f"{field_name} debe ser un monto decimal válido") from None
    if not amount.is_finite():
        raise SavingsError(f"{field_name} debe ser finito")
    return amount


def _required_positive_amount(value, field_name="amount"):
    amount = _money(value, field_name)
    if amount <= 0:
        raise SavingsError(f"{field_name} debe ser mayor que cero")
    return amount


def _normalize_savings_date(value, field_name="date", allow_date_only=False):
    if not isinstance(value, str) or not value.strip():
        raise SavingsError(f"{field_name} es requerido en formato ISO 8601")
    raw = value.strip()
    try:
        if allow_date_only and re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
            parsed = datetime.fromisoformat(raw).replace(tzinfo=timezone.utc)
        else:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        raise SavingsError(f"{field_name} debe estar en formato ISO 8601") from None
    if parsed.tzinfo is None:
        raise SavingsError(f"{field_name} debe incluir zona horaria")
    return parsed.astimezone(timezone.utc).isoformat()


def _dynamo_value_map(values):
    return {key: serializer.serialize(value) for key, value in values.items()}


def _execute_savings_transaction(actions):
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
    return {
        "Put": {
            "TableName": TABLE_NAME,
            "Item": item,
            "ConditionExpression": condition,
        }
    }


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


def _delete_action(key, condition, values=None, names=None):
    action = {
        "TableName": TABLE_NAME,
        "Key": key,
        "ConditionExpression": condition,
    }
    if values:
        action["ExpressionAttributeValues"] = values
    if names:
        action["ExpressionAttributeNames"] = names
    return {"Delete": action}


def _ensure_savings_initialized(couple_pk):
    now = get_iso_timestamp()
    initial_items = (
        {
            "PK": couple_pk,
            "SK": SAVINGS_CONFIG_SK,
            "goalOrder": [],
            "version": 0,
            "currency": DEFAULT_CURRENCY,
            "createdAt": now,
            "updatedAt": now,
        },
        {
            "PK": couple_pk,
            "SK": SAVINGS_CUSHION_SK,
            "goalId": SAVINGS_CUSHION_ID,
            "name": "Colchón",
            "entityType": "savings_cushion",
            "accumulatedAmount": Decimal("0"),
            "status": "cushion",
            "createdAt": now,
            "updatedAt": now,
        },
    )
    for item in initial_items:
        existing = table.get_item(
            Key={"PK": couple_pk, "SK": item["SK"]},
            ConsistentRead=True,
        ).get("Item")
        if existing:
            continue
        try:
            table.put_item(
                Item=item,
                ConditionExpression=Attr("PK").not_exists(),
            )
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
                raise


def _read_savings_state(couple_pk):
    _ensure_savings_initialized(couple_pk)
    config_response = table.get_item(
        Key={"PK": couple_pk, "SK": SAVINGS_CONFIG_SK},
        ConsistentRead=True,
    )
    cushion_response = table.get_item(
        Key={"PK": couple_pk, "SK": SAVINGS_CUSHION_SK},
        ConsistentRead=True,
    )
    if "Item" not in config_response or "Item" not in cushion_response:
        raise SavingsError("No se pudo inicializar la cascada de ahorros", 500)

    goals = query_all(
        table,
        KeyConditionExpression=Key("PK").eq(couple_pk)
        & Key("SK").begins_with(SAVINGS_GOAL_PREFIX),
        ConsistentRead=True,
    )
    goal_map = {
        goal["goalId"]: goal
        for goal in goals
        if goal.get("goalId") != SAVINGS_CUSHION_ID
    }
    config = config_response["Item"]
    order = config.get("goalOrder", [])
    if not isinstance(order, list) or len(order) != len(set(order)):
        raise SavingsError("La prioridad de metas está inconsistente", 500)
    for goal_id in order:
        if goal_id not in goal_map or goal_map[goal_id].get("status") != "pending":
            raise SavingsError("La prioridad de metas está inconsistente", 500)
    pending_ids = {
        goal_id for goal_id, goal in goal_map.items()
        if goal.get("status") == "pending"
    }
    if pending_ids != set(order):
        raise SavingsError("La prioridad de metas está inconsistente", 500)
    return config, goal_map, cushion_response["Item"]


def _percent(part, whole):
    if whole <= 0:
        return Decimal("0")
    return (part * Decimal("100") / whole).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _goal_response(goal, priority=None, derived_status=None):
    target = _money(goal.get("targetAmount", 0), "targetAmount")
    accumulated = _money(goal.get("accumulatedAmount", 0), "accumulatedAmount")
    result = {
        "goalId": goal["goalId"],
        "name": goal["name"],
        "targetAmount": target,
        "accumulatedAmount": accumulated,
        "remainingAmount": max(Decimal("0"), target - accumulated),
        "progressPercent": _percent(accumulated, target),
        "status": derived_status or goal.get("status"),
        "createdBy": goal.get("createdBy"),
        "createdByEmail": goal.get("createdByEmail"),
        "createdByName": goal.get("createdByName"),
        "createdAt": goal.get("createdAt"),
        "updatedAt": goal.get("updatedAt"),
    }
    if priority is not None:
        result["priority"] = priority
    if goal.get("linkedBodaId"):
        result["linkedBodaId"] = goal["linkedBodaId"]
        result["isLinkedToWeddingBudget"] = True
    else:
        result["isLinkedToWeddingBudget"] = False
    if goal.get("completedAt"):
        result["completedAt"] = goal["completedAt"]
    if goal.get("archivedAt"):
        result["archivedAt"] = goal["archivedAt"]
    return result


def _latest_savings_movements(couple_pk, limit=10):
    response = table.query(
        KeyConditionExpression=Key("PK").eq(couple_pk)
        & Key("SK").begins_with(SAVINGS_MOVEMENT_PREFIX),
        ScanIndexForward=False,
        Limit=limit,
    )
    return response.get("Items", [])


def get_savings_summary(couple_pk):
    couple = get_couple(couple_pk)
    if not couple:
        raise SavingsError("Pareja no inicializada", 404)
    config, goals_by_id, cushion = _read_savings_state(couple_pk)
    order = config["goalOrder"]
    pending = [
        _goal_response(goals_by_id[goal_id], index, "active" if index == 1 else "waiting")
        for index, goal_id in enumerate(order, start=1)
    ]
    active_goal = pending[0] if pending else None
    waiting_goals = pending[1:] if len(pending) > 1 else []
    completed_items = sorted(
        (goal for goal in goals_by_id.values() if goal.get("status") == "completed"),
        key=lambda goal: goal.get("completedAt", ""),
    )
    archived_items = sorted(
        (goal for goal in goals_by_id.values() if goal.get("status") == "archived"),
        key=lambda goal: goal.get("archivedAt", ""),
        reverse=True,
    )
    completed = [_goal_response(goal) for goal in completed_items]
    archived = [_goal_response(goal) for goal in archived_items]
    cushion_balance = _money(cushion.get("accumulatedAmount", 0), "cushionBalance")
    all_active_goals = [
        goal for goal in goals_by_id.values() if goal.get("status") != "archived"
    ]
    total_target = sum(
        (_money(goal["targetAmount"], "targetAmount") for goal in all_active_goals),
        Decimal("0"),
    )
    cascade_accumulated = sum(
        (_money(goal.get("accumulatedAmount", 0)) for goal in all_active_goals),
        Decimal("0"),
    )
    segments = [
        {
            "goalId": goal["goalId"],
            "name": goal["name"],
            "targetAmount": _money(goal["targetAmount"]),
            "accumulatedAmount": _money(goal.get("accumulatedAmount", 0)),
            "status": goal.get("status"),
            "progressPercent": _percent(
                _money(goal.get("accumulatedAmount", 0)),
                _money(goal["targetAmount"]),
            ),
        }
        for goal in completed_items
    ]
    segments.extend(
        {
            "goalId": goals_by_id[goal_id]["goalId"],
            "name": goals_by_id[goal_id]["name"],
            "targetAmount": _money(goals_by_id[goal_id]["targetAmount"]),
            "accumulatedAmount": _money(goals_by_id[goal_id].get("accumulatedAmount", 0)),
            "status": "active" if index == 1 else "waiting",
            "progressPercent": _percent(
                _money(goals_by_id[goal_id].get("accumulatedAmount", 0)),
                _money(goals_by_id[goal_id]["targetAmount"]),
            ),
        }
        for index, goal_id in enumerate(order, start=1)
    )
    segments.append({
        "goalId": SAVINGS_CUSHION_ID,
        "name": cushion.get("name", "Colchon"),
        "targetAmount": None,
        "accumulatedAmount": cushion_balance,
        "status": "cushion",
        "progressPercent": None,
    })
    total_saved = sum(
        (_money(goal.get("accumulatedAmount", 0)) for goal in goals_by_id.values()),
        cushion_balance,
    )
    remaining = active_goal["remainingAmount"] if active_goal else Decimal("0")
    active_target = active_goal["targetAmount"] if active_goal else Decimal("0")
    return {
        "currency": config.get("currency", DEFAULT_CURRENCY),
        "totalSaved": total_saved,
        "activeGoal": active_goal,
        "activeGoalRemainingAmount": remaining,
        "activeGoalMissingPercent": _percent(remaining, active_target),
        "waitingGoals": waiting_goals,
        "completedGoals": completed,
        "archivedGoals": archived,
        "cushion": {
            "goalId": SAVINGS_CUSHION_ID,
            "name": cushion.get("name", "Colchon"),
            "balance": cushion_balance,
            "status": "cushion",
            "isArchivable": False,
            "isReorderable": False,
        },
        "cascade": {
            "targetAmount": total_target,
            "accumulatedAmount": cascade_accumulated,
            "progressPercent": _percent(cascade_accumulated, total_target),
            "segments": segments,
        },
        "latestMovements": _latest_savings_movements(couple_pk),
        "configVersion": int(config.get("version", 0)),
    }


def _config_update_action(couple_pk, config, goal_order, now):
    version = int(config.get("version", 0))
    return _update_action(
        {"PK": couple_pk, "SK": SAVINGS_CONFIG_SK},
        "SET #goalOrder = :goalOrder, #version = :nextVersion, #updatedAt = :updatedAt",
        {
            ":goalOrder": goal_order,
            ":expectedVersion": version,
            ":nextVersion": version + 1,
            ":updatedAt": now,
        },
        {"#goalOrder": "goalOrder", "#version": "version", "#updatedAt": "updatedAt"},
        "#version = :expectedVersion",
    )


def _request_idempotency_key(event):
    headers = event.get("headers") or {}
    key = next(
        (value for name, value in headers.items() if name.casefold() == "idempotency-key"),
        None,
    )
    if not isinstance(key, str) or not key.strip() or len(key.strip()) > 128:
        raise SavingsError("El header Idempotency-Key es requerido (1-128 caracteres)")
    return hashlib.sha256(key.strip().encode("utf-8")).hexdigest()


def _request_hash(payload):
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _idempotency_key(couple_pk, key_hash):
    return {"PK": couple_pk, "SK": f"{SAVINGS_IDEMPOTENCY_PREFIX}{key_hash}"}


def _get_idempotency_record(couple_pk, key_hash):
    return table.get_item(
        Key=_idempotency_key(couple_pk, key_hash),
        ConsistentRead=True,
    ).get("Item")


def _idempotency_put_action(couple_pk, key_hash, request_hash, status_code, result, now):
    return _put_action({
        **_idempotency_key(couple_pk, key_hash),
        "requestHash": request_hash,
        "statusCode": status_code,
        "result": result,
        "createdAt": now,
    })


def _idempotent_replay(record, request_hash):
    if record.get("requestHash") != request_hash:
        raise SavingsError("Idempotency-Key ya fue usado con otra solicitud", 409)
    return record.get("result", {}), 200, True


def _run_idempotent_transaction(couple_pk, key_hash, request_hash, build_transaction):
    existing = _get_idempotency_record(couple_pk, key_hash)
    if existing:
        return _idempotent_replay(existing, request_hash)
    for _ in range(MAX_SAVINGS_RETRIES):
        actions, result, status_code = build_transaction()
        now = get_iso_timestamp()
        actions.append(
            _idempotency_put_action(
                couple_pk, key_hash, request_hash, status_code, result, now
            )
        )
        try:
            _execute_savings_transaction(actions)
            return result, status_code, False
        except ClientError as exc:
            existing = _get_idempotency_record(couple_pk, key_hash)
            if existing:
                return _idempotent_replay(existing, request_hash)
            error_code = exc.response.get("Error", {}).get("Code", "Unknown")
            if error_code in {"TransactionCanceledException", "ConditionalCheckFailedException"}:
                continue
            raise
    raise SavingsError("Conflicto concurrente; vuelve a intentar la operación", 409)


def _validate_note(note, required=False):
    if note is None:
        note = ""
    if not isinstance(note, str):
        raise SavingsError("note debe ser texto")
    note = note.strip()
    if required and not note:
        raise SavingsError("note es obligatorio")
    if len(note) > 1000:
        raise SavingsError("note no puede superar 1000 caracteres")
    return note


def _release_wedding_link_action(couple_pk, goal):
    boda_id = goal.get("linkedBodaId")
    if not boda_id:
        return None
    return _delete_action(
        {"PK": couple_pk, "SK": f"VINCULO_BODA#{boda_id}"},
        "#goalId = :goalId",
        {":goalId": goal["goalId"]},
        {"#goalId": "goalId"},
    )


def _goal_from_state(goal_map, goal_id):
    goal = goal_map.get(goal_id)
    if not goal:
        raise SavingsError("Meta no encontrada", 404)
    return goal


def create_savings_goal(couple_pk, user, data, idempotency_hash):
    unknown = set(data) - {"name", "targetAmount", "bodaId", "priority"}
    if unknown:
        raise SavingsError(f"Campos no admitidos: {', '.join(sorted(unknown))}")
    name = data.get("name")
    if not isinstance(name, str) or not name.strip() or len(name.strip()) > 100:
        raise SavingsError("name es obligatorio y no puede superar 100 caracteres")
    target = _required_positive_amount(data.get("targetAmount"), "targetAmount")
    boda_id = data.get("bodaId")
    if boda_id is not None:
        if not isinstance(boda_id, str) or not boda_id.strip() or len(boda_id.strip()) > 128:
            raise SavingsError("bodaId debe ser un identificador válido")
        boda_id = boda_id.strip()
    priority = data.get("priority")
    if priority is not None and (isinstance(priority, bool) or not isinstance(priority, int)):
        raise SavingsError("priority debe ser un entero")

    normalized = {
        "name": name.strip(),
        "targetAmount": str(target),
        "bodaId": boda_id,
        "priority": priority,
        "userId": user["sub"],
    }
    request_hash = _request_hash(normalized)
    goal_id = str(uuid.uuid4())
    created_at = get_iso_timestamp()

    def build_transaction():
        config, goal_map, _ = _read_savings_state(couple_pk)
        order = list(config["goalOrder"])
        insertion_priority = priority if priority is not None else len(order) + 1
        if not 1 <= insertion_priority <= len(order) + 1:
            raise SavingsError("priority debe estar entre 1 y la última posición disponible")
        if boda_id:
            linked = table.get_item(
                Key={"PK": couple_pk, "SK": f"VINCULO_BODA#{boda_id}"},
                ConsistentRead=True,
            ).get("Item")
            if linked:
                raise SavingsError("El presupuesto de boda ya está vinculado a otra meta pendiente", 409)
        ordered_goal_ids = order[:]
        ordered_goal_ids.insert(insertion_priority - 1, goal_id)
        goal = {
            "PK": couple_pk,
            "SK": f"{SAVINGS_GOAL_PREFIX}{goal_id}",
            "goalId": goal_id,
            "name": name.strip(),
            "targetAmount": target,
            "accumulatedAmount": Decimal("0"),
            "status": "pending",
            "createdBy": user["sub"],
            "createdByEmail": user["email"],
            "createdByName": user.get("name") or None,
            "createdAt": created_at,
            "updatedAt": created_at,
        }
        if boda_id:
            goal["linkedBodaId"] = boda_id
        result = {
            "goal": _goal_response(goal, insertion_priority, "active" if insertion_priority == 1 else "waiting"),
            "goalId": goal_id,
        }
        actions = [
            _config_update_action(couple_pk, config, ordered_goal_ids, created_at),
            _put_action(goal),
        ]
        if boda_id:
            actions.append(_put_action({
                "PK": couple_pk,
                "SK": f"VINCULO_BODA#{boda_id}",
                "goalId": goal_id,
                "createdAt": created_at,
            }))
        return actions, result, 201

    result, status, replay = _run_idempotent_transaction(
        couple_pk, idempotency_hash, request_hash, build_transaction
    )
    response = {**result, "summary": get_savings_summary(couple_pk)}
    if replay:
        response["idempotentReplay"] = True
    return response, status


def update_savings_goal(couple_pk, goal_id, data):
    allowed = {"name", "targetAmount", "bodaId"}
    unknown = set(data) - allowed
    if unknown:
        raise SavingsError(f"Campos no editables: {', '.join(sorted(unknown))}")
    if not data:
        raise SavingsError("Debe proporcionar al menos un campo para actualizar")
    if goal_id == SAVINGS_CUSHION_ID:
        raise SavingsError("La meta Colchón no se puede editar", 409)
    config, goal_map, _ = _read_savings_state(couple_pk)
    goal = _goal_from_state(goal_map, goal_id)
    if goal_id == SAVINGS_CUSHION_ID or goal.get("status") == "cushion":
        raise SavingsError("La meta Colchon no se puede editar", 409)
    if goal.get("status") != "pending":
        raise SavingsError("Solo se pueden editar metas pendientes", 409)

    name = goal["name"]
    if "name" in data:
        if not isinstance(data["name"], str) or not data["name"].strip() or len(data["name"].strip()) > 100:
            raise SavingsError("name es obligatorio y no puede superar 100 caracteres")
        name = data["name"].strip()
    target = _money(goal["targetAmount"], "targetAmount")
    if "targetAmount" in data:
        target = _required_positive_amount(data["targetAmount"], "targetAmount")
    accumulated = _money(goal.get("accumulatedAmount", 0))
    if target < accumulated:
        raise SavingsError("targetAmount no puede ser menor que el monto acumulado", 409)
    completes_goal = target == accumulated

    old_boda_id = goal.get("linkedBodaId")
    boda_id = old_boda_id
    if "bodaId" in data:
        boda_id = data["bodaId"]
        if boda_id is not None:
            if not isinstance(boda_id, str) or not boda_id.strip() or len(boda_id.strip()) > 128:
                raise SavingsError("bodaId debe ser un identificador válido o null")
            boda_id = boda_id.strip()
    if boda_id and boda_id != old_boda_id:
        if completes_goal:
            raise SavingsError("No se puede vincular una meta que quedará completada", 409)
        linked = table.get_item(
            Key={"PK": couple_pk, "SK": f"VINCULO_BODA#{boda_id}"},
            ConsistentRead=True,
        ).get("Item")
        if linked:
            raise SavingsError("El presupuesto de boda ya está vinculado a otra meta pendiente", 409)

    now = get_iso_timestamp()
    names = {"#name": "name", "#target": "targetAmount", "#updatedAt": "updatedAt"}
    values = {
        ":name": name,
        ":target": target,
        ":updatedAt": now,
        ":expectedUpdatedAt": goal["updatedAt"],
        ":expectedBalance": accumulated,
        ":pending": "pending",
    }
    expression = "SET #name = :name, #target = :target, #updatedAt = :updatedAt"
    if completes_goal:
        expression += ", #status = :completed, #completedAt = :completedAt"
        names["#status"] = "status"
        names["#completedAt"] = "completedAt"
        values[":completed"] = "completed"
        values[":completedAt"] = now
    if boda_id != old_boda_id:
        names["#linkedBodaId"] = "linkedBodaId"
        if boda_id:
            values[":bodaId"] = boda_id
            expression += ", #linkedBodaId = :bodaId"
        else:
            expression += " REMOVE #linkedBodaId"
    goal_action = _update_action(
        {"PK": couple_pk, "SK": f"{SAVINGS_GOAL_PREFIX}{goal_id}"},
        expression,
        values,
        names,
        "#status = :pending AND #updatedAt = :expectedUpdatedAt AND accumulatedAmount = :expectedBalance",
    )
    goal_action["Update"]["ExpressionAttributeNames"]["#status"] = "status"
    new_order = [pending_id for pending_id in config["goalOrder"] if pending_id != goal_id] if completes_goal else config["goalOrder"]
    actions = [_config_update_action(couple_pk, config, new_order, now), goal_action]
    if old_boda_id and (old_boda_id != boda_id or completes_goal):
        actions.append(_delete_action(
            {"PK": couple_pk, "SK": f"VINCULO_BODA#{old_boda_id}"},
            "#goalId = :goalId",
            {":goalId": goal_id},
            {"#goalId": "goalId"},
        ))
    if boda_id and boda_id != old_boda_id and not completes_goal:
        actions.append(_put_action({
            "PK": couple_pk,
            "SK": f"VINCULO_BODA#{boda_id}",
            "goalId": goal_id,
            "createdAt": now,
        }))
    try:
        _execute_savings_transaction(actions)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "Unknown")
        if code in {"TransactionCanceledException", "ConditionalCheckFailedException"}:
            raise SavingsError("La meta cambió simultáneamente; vuelve a consultar e intenta otra vez", 409) from exc
        raise
    updated_goal = {
        **goal,
        "name": name,
        "targetAmount": target,
        "linkedBodaId": boda_id,
        "updatedAt": now,
    }
    if completes_goal:
        updated_goal.update({"status": "completed", "completedAt": now})
        updated_priority = None
    else:
        updated_priority = config["goalOrder"].index(goal_id) + 1
    return {
        "goal": _goal_response(updated_goal, updated_priority),
        "completedGoal": updated_goal if completes_goal else None,
        "summary": get_savings_summary(couple_pk),
    }, 200


def archive_savings_goal(couple_pk, goal_id):
    if goal_id == SAVINGS_CUSHION_ID:
        raise SavingsError("La meta Colchon no se puede archivar", 409)
    config, goal_map, _ = _read_savings_state(couple_pk)
    goal = _goal_from_state(goal_map, goal_id)
    if goal.get("status") == "archived":
        return {"goalId": goal_id, "status": "archived", "summary": get_savings_summary(couple_pk)}, 200
    if goal.get("status") not in {"pending", "completed"}:
        raise SavingsError("Solo se pueden archivar metas normales", 409)
    now = get_iso_timestamp()
    order = [pending_id for pending_id in config["goalOrder"] if pending_id != goal_id]
    values = {
        ":archived": "archived",
        ":now": now,
        ":expectedStatus": goal["status"],
        ":expectedUpdatedAt": goal["updatedAt"],
    }
    names = {"#status": "status", "#updatedAt": "updatedAt", "#archivedAt": "archivedAt"}
    goal_action = _update_action(
        {"PK": couple_pk, "SK": f"{SAVINGS_GOAL_PREFIX}{goal_id}"},
        "SET #status = :archived, #updatedAt = :now, #archivedAt = :now",
        values,
        names,
        "#status = :expectedStatus AND #updatedAt = :expectedUpdatedAt",
    )
    actions = [_config_update_action(couple_pk, config, order, now), goal_action]
    release_link = _release_wedding_link_action(couple_pk, goal) if goal.get("status") == "pending" else None
    if release_link:
        actions.append(release_link)
    try:
        _execute_savings_transaction(actions)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"TransactionCanceledException", "ConditionalCheckFailedException"}:
            raise SavingsError("La meta cambió simultáneamente; vuelve a consultar e intenta otra vez", 409) from exc
        raise
    return {"goalId": goal_id, "status": "archived", "summary": get_savings_summary(couple_pk)}, 200


def reorder_savings_goals(couple_pk, goal_ids):
    if not isinstance(goal_ids, list) or any(not isinstance(goal_id, str) for goal_id in goal_ids):
        raise SavingsError("goalIds debe ser una lista de identificadores")
    config, _, _ = _read_savings_state(couple_pk)
    current_order = config["goalOrder"]
    if len(goal_ids) != len(set(goal_ids)):
        raise SavingsError("goalIds contiene identificadores duplicados")
    if set(goal_ids) != set(current_order):
        raise SavingsError("goalIds debe incluir exactamente todas las metas pendientes")
    if goal_ids != current_order:
        now = get_iso_timestamp()
        try:
            _execute_savings_transaction([
                _config_update_action(couple_pk, config, goal_ids, now)
            ])
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in {"TransactionCanceledException", "ConditionalCheckFailedException"}:
                raise SavingsError("Las metas cambiaron simultáneamente; vuelve a consultar e intenta otra vez", 409) from exc
            raise
    return get_savings_summary(couple_pk), 200


def _movement_item(couple_pk, operation_id, timestamp, sequence, movement_type, amount, balance_after, user, note, goal_id=None, goal_name=None, automatic_reference=None, source_goal_id=None):
    item = {
        "PK": couple_pk,
        "SK": f"{SAVINGS_MOVEMENT_PREFIX}{timestamp}#{operation_id}#{sequence:02d}",
        "movementId": f"{operation_id}-{sequence:02d}",
        "operationId": operation_id,
        "movementType": movement_type,
        "amount": amount,
        "balanceAfter": balance_after,
        "note": note or None,
        "userId": user["sub"],
        "userEmail": user["email"],
        "date": timestamp,
        "isCushion": movement_type.startswith("cushion_"),
        "createdAt": get_iso_timestamp(),
    }
    if goal_id:
        item["goalId"] = goal_id
    if goal_name:
        item["goalName"] = goal_name
    if automatic_reference:
        item["automaticReference"] = automatic_reference
    if source_goal_id:
        item["sourceGoalId"] = source_goal_id
    return item


def register_savings_contribution(couple_pk, user, data, idempotency_hash):
    unknown = set(data) - {"amount", "note", "date"}
    if unknown:
        raise SavingsError(f"Campos no admitidos: {', '.join(sorted(unknown))}")
    amount = _required_positive_amount(data.get("amount"))
    note = _validate_note(data.get("note"))
    timestamp = _normalize_savings_date(data.get("date"))
    request_hash = _request_hash({
        "amount": str(amount), "note": note, "date": timestamp, "userId": user["sub"]
    })
    operation_id = str(uuid.uuid4())

    def build_transaction():
        config, goal_map, cushion = _read_savings_state(couple_pk)
        order = list(config["goalOrder"])
        cushion_balance = _money(cushion.get("accumulatedAmount", 0))
        actions = []
        movements = []
        completed_goal = None
        assigned_to_goal = Decimal("0")
        sent_to_cushion = Decimal("0")
        goal_balance = None
        goal_name = None
        new_order = order
        if order:
            goal_id = order[0]
            goal = _goal_from_state(goal_map, goal_id)
            target = _money(goal["targetAmount"], "targetAmount")
            old_goal_balance = _money(goal.get("accumulatedAmount", 0))
            remaining = target - old_goal_balance
            if remaining <= 0:
                raise SavingsError("La meta activa tiene un saldo inconsistente", 500)
            assigned_to_goal = min(amount, remaining)
            goal_balance = old_goal_balance + assigned_to_goal
            completed = goal_balance == target
            if completed:
                completed_goal = {
                    "goalId": goal_id,
                    "name": goal["name"],
                    "targetAmount": target,
                    "completedAt": timestamp,
                }
                new_order = order[1:]
            sent_to_cushion = amount - assigned_to_goal
            goal_name = goal["name"]
            goal_values = {
                ":newBalance": goal_balance,
                ":expectedBalance": old_goal_balance,
                ":targetAmount": target,
                ":pending": "pending",
                ":now": get_iso_timestamp(),
            }
            goal_names = {"#status": "status", "#updatedAt": "updatedAt", "#accumulated": "accumulatedAmount"}
            goal_expression = "SET #accumulated = :newBalance, #updatedAt = :now"
            if completed:
                goal_names["#completedAt"] = "completedAt"
                goal_expression += ", #status = :completed, #completedAt = :completedAt"
                goal_values[":completed"] = "completed"
                goal_values[":completedAt"] = timestamp
            actions.append(_update_action(
                {"PK": couple_pk, "SK": f"{SAVINGS_GOAL_PREFIX}{goal_id}"},
                goal_expression,
                goal_values,
                goal_names,
                "#status = :pending AND #accumulated = :expectedBalance AND targetAmount = :targetAmount",
            ))
            movements.append(_movement_item(
                couple_pk, operation_id, timestamp, 1, "goal_contribution",
                assigned_to_goal, goal_balance, user, note, goal_id, goal_name,
            ))
            if sent_to_cushion > 0:
                cushion_balance += sent_to_cushion
                movements.append(_movement_item(
                    couple_pk, operation_id, timestamp, 2, "cushion_surplus",
                    sent_to_cushion, cushion_balance, user, note,
                    SAVINGS_CUSHION_ID, "Colchón", f"Sobrante de: {goal_name}", goal_id,
                ))
        else:
            sent_to_cushion = amount
            cushion_balance += amount
            movements.append(_movement_item(
                couple_pk, operation_id, timestamp, 1, "cushion_contribution",
                amount, cushion_balance, user, note,
                SAVINGS_CUSHION_ID, "Colchón",
            ))

        if sent_to_cushion > 0:
            actions.append(_update_action(
                {"PK": couple_pk, "SK": SAVINGS_CUSHION_SK},
                "SET #balance = :newBalance, #updatedAt = :now",
                {
                    ":newBalance": cushion_balance,
                    ":expectedBalance": _money(cushion.get("accumulatedAmount", 0)),
                    ":now": get_iso_timestamp(),
                },
                {"#balance": "accumulatedAmount", "#updatedAt": "updatedAt"},
                "#balance = :expectedBalance",
            ))

        now = get_iso_timestamp()
        actions.insert(0, _config_update_action(couple_pk, config, new_order, now))
        operation = {
            "PK": couple_pk,
            "SK": f"{SAVINGS_OPERATION_PREFIX}{timestamp}#{operation_id}",
            "operationId": operation_id,
            "operationType": "contribution",
            "amount": amount,
            "allocatedToGoal": assigned_to_goal,
            "sentToCushion": sent_to_cushion,
            "goalId": order[0] if order else SAVINGS_CUSHION_ID,
            "goalName": goal_name or "Colchon",
            "note": note or None,
            "userId": user["sub"],
            "userEmail": user["email"],
            "date": timestamp,
            "createdAt": now,
        }
        actions.append(_put_action(operation))
        actions.extend(_put_action(movement) for movement in movements)
        release_link = _release_wedding_link_action(couple_pk, goal_map[order[0]]) if completed_goal and goal_map[order[0]].get("linkedBodaId") else None
        if release_link:
            actions.append(release_link)
        result = {
            "operationId": operation_id,
            "operationType": "contribution",
            "amount": amount,
            "allocatedToGoal": assigned_to_goal,
            "sentToCushion": sent_to_cushion,
            "completedGoal": completed_goal,
            "activeGoalId": new_order[0] if new_order else None,
            "goalBalance": goal_balance,
            "cushionBalance": cushion_balance,
            "currency": DEFAULT_CURRENCY,
        }
        return actions, result, 201

    result, status, replay = _run_idempotent_transaction(
        couple_pk, idempotency_hash, request_hash, build_transaction
    )
    response = {**result, "summary": get_savings_summary(couple_pk)}
    if replay:
        response["idempotentReplay"] = True
    if result.get("completedGoal"):
        response["celebration"] = {
            "type": "goal_completed",
            "goal": result["completedGoal"],
        }
    return response, status


def withdraw_from_cushion(couple_pk, user, data, idempotency_hash):
    unknown = set(data) - {"amount", "note", "date"}
    if unknown:
        raise SavingsError(f"Campos no admitidos para retiro del Colchón: {', '.join(sorted(unknown))}")
    amount = _required_positive_amount(data.get("amount"))
    note = _validate_note(data.get("note"), required=True)
    timestamp = _normalize_savings_date(data.get("date"))
    request_hash = _request_hash({
        "amount": str(amount), "note": note, "date": timestamp, "userId": user["sub"]
    })
    operation_id = str(uuid.uuid4())

    def build_transaction():
        config, _, cushion = _read_savings_state(couple_pk)
        old_balance = _money(cushion.get("accumulatedAmount", 0))
        if amount > old_balance:
            raise SavingsError("El retiro supera el saldo disponible del Colchon", 422)
        new_balance = old_balance - amount
        now = get_iso_timestamp()
        movement = _movement_item(
            couple_pk, operation_id, timestamp, 1, "cushion_withdrawal",
            amount, new_balance, user, note, SAVINGS_CUSHION_ID, "Colchón",
        )
        operation = {
            "PK": couple_pk,
            "SK": f"{SAVINGS_OPERATION_PREFIX}{timestamp}#{operation_id}",
            "operationId": operation_id,
            "operationType": "withdrawal",
            "amount": amount,
            "note": note,
            "userId": user["sub"],
            "userEmail": user["email"],
            "date": timestamp,
            "balanceAfter": new_balance,
            "createdAt": now,
        }
        result = {
            "operationId": operation_id,
            "operationType": "withdrawal",
            "amount": amount,
            "note": note,
            "balanceBefore": old_balance,
            "balanceAfter": new_balance,
            "cushionBalance": new_balance,
            "currency": DEFAULT_CURRENCY,
        }
        actions = [
            _config_update_action(couple_pk, config, config["goalOrder"], now),
            _update_action(
                {"PK": couple_pk, "SK": SAVINGS_CUSHION_SK},
                "SET #balance = :newBalance, #updatedAt = :now",
                {
                    ":newBalance": new_balance,
                    ":expectedBalance": old_balance,
                    ":now": now,
                },
                {"#balance": "accumulatedAmount", "#updatedAt": "updatedAt"},
                "#balance = :expectedBalance AND #balance >= :newBalance",
            ),
            _put_action(operation),
            _put_action(movement),
        ]
        return actions, result, 201

    result, status, replay = _run_idempotent_transaction(
        couple_pk, idempotency_hash, request_hash, build_transaction
    )
    response = {**result, "summary": get_savings_summary(couple_pk)}
    if replay:
        response["idempotentReplay"] = True
    return response, status


def list_savings_history(couple_pk, query_params):
    params = query_params or {}
    goal_id = params.get("goalId")
    cushion_only = str(params.get("cushion", "false")).casefold() in {"true", "1", "yes"}
    if goal_id and cushion_only:
        raise SavingsError("Usa goalId o cushion=true, no ambos")
    if goal_id == SAVINGS_CUSHION_ID:
        cushion_only = True
        goal_id = None
    start = params.get("from")
    end = params.get("to")
    start_iso = _normalize_savings_date(start, "from", allow_date_only=True) if start else None
    end_iso = _normalize_savings_date(end, "to", allow_date_only=True) if end else None
    if end_iso and end and re.fullmatch(r"\d{4}-\d{2}-\d{2}", end.strip()):
        end_iso = end_iso.replace("T00:00:00+00:00", "T23:59:59.999999+00:00")
    if start_iso and end_iso and start_iso > end_iso:
        raise SavingsError("from debe ser anterior o igual a to")
    try:
        limit = int(params.get("limit", 50))
    except (ValueError, TypeError):
        raise SavingsError("limit debe ser un entero") from None
    if not 1 <= limit <= MAX_HISTORY_PAGE_SIZE:
        raise SavingsError(f"limit debe estar entre 1 y {MAX_HISTORY_PAGE_SIZE}")
    cursor = params.get("cursor")
    exclusive_start_key = None
    if cursor:
        try:
            decoded = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))
            exclusive_start_key = json.loads(decoded)
            if set(exclusive_start_key) != {"PK", "SK"} or exclusive_start_key["PK"] != couple_pk:
                raise ValueError
        except (ValueError, TypeError, json.JSONDecodeError, binascii.Error):
            raise SavingsError("cursor inválido") from None

    if start_iso or end_iso:
        lower = f"{SAVINGS_MOVEMENT_PREFIX}{start_iso or '0000-01-01T00:00:00+00:00'}"
        upper = f"{SAVINGS_MOVEMENT_PREFIX}{end_iso or '9999-12-31T23:59:59.999999+00:00'}~"
        key_condition = Key("PK").eq(couple_pk) & Key("SK").between(lower, upper)
    else:
        key_condition = Key("PK").eq(couple_pk) & Key("SK").begins_with(SAVINGS_MOVEMENT_PREFIX)

    movements = []
    last_evaluated_key = None
    request_start_key = exclusive_start_key
    while len(movements) < limit:
        request = {
            "KeyConditionExpression": key_condition,
            "ScanIndexForward": False,
            "Limit": max(1, limit - len(movements)),
        }
        if request_start_key:
            request["ExclusiveStartKey"] = request_start_key
        response = table.query(**request)
        for movement in response.get("Items", []):
            if goal_id and movement.get("goalId") != goal_id:
                continue
            if cushion_only and not movement.get("isCushion"):
                continue
            movements.append(movement)
            if len(movements) == limit:
                break
        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key or len(movements) >= limit:
            break
        request_start_key = last_evaluated_key
    next_cursor = None
    if last_evaluated_key:
        next_cursor = base64.urlsafe_b64encode(
            json.dumps(last_evaluated_key, separators=(",", ":")).encode("utf-8")
        ).decode("ascii").rstrip("=")
    return {
        "items": movements,
        "count": len(movements),
        "limit": limit,
        "nextCursor": next_cursor,
        "sort": "date_desc",
    }, 200


# ─── Handler principal ────────────────────────────────────────────────────────


def _handle_savings_routes(method, path, event, body, couple_pk, user):
    if path == "/finances/metas" and method == "GET":
        return build_response(200, get_savings_summary(couple_pk))
    if path == "/finances/metas" and method == "POST":
        key_hash = _request_idempotency_key(event)
        result, status = create_savings_goal(couple_pk, user, body, key_hash)
        return build_response(status, result)
    if path == "/finances/metas/reordenar" and method == "POST":
        if "goalIds" not in body:
            return build_response(400, {"error": "goalIds es requerido"})
        result, status = reorder_savings_goals(couple_pk, body["goalIds"])
        return build_response(status, result)

    goal_id = get_path_param(event, "goalId")
    if goal_id and "/finances/metas/" in path and method == "PATCH":
        result, status = update_savings_goal(couple_pk, goal_id, body)
        return build_response(status, result)
    if goal_id and "/finances/metas/" in path and method == "DELETE":
        result, status = archive_savings_goal(couple_pk, goal_id)
        return build_response(status, result)
    if path == "/finances/ahorros/aportes" and method == "POST":
        key_hash = _request_idempotency_key(event)
        result, status = register_savings_contribution(couple_pk, user, body, key_hash)
        return build_response(status, result)
    if path == "/finances/ahorros/retiros" and method == "POST":
        key_hash = _request_idempotency_key(event)
        result, status = withdraw_from_cushion(couple_pk, user, body, key_hash)
        return build_response(status, result)
    if path == "/finances/ahorros/historial" and method == "GET":
        result, status = list_savings_history(couple_pk, event.get("queryStringParameters") or {})
        return build_response(status, result)
    return None


def lambda_handler(event, context):
    """🔵 Maneja todas las rutas de la API de finanzas."""
    method = event.get("requestContext", {}).get("http", {}).get("method", "")
    path = event.get("rawPath", "")

    logger.info(json.dumps({
        "level": "⚪️",
        "message": "Solicitud FinancesCRUD",
        "method": method,
        "path": path,
        "function": getattr(context, "function_name", "unknown"),
    }, ensure_ascii=False))

    try:
        body = parse_body(event)
        if not isinstance(body, dict):
            return build_response(400, {"error": "El body debe ser un objeto JSON"})

        # ──────── POST /finances/init ─────────────────────────────────────────
        if method == "POST" and path == "/finances/init":
            user = _get_request_user(event)
            if not user["email"]:
                user["email"] = _email_from_access_token(event)
            required = {"user1Email", "user2Email", "user1Name", "user2Name"}
            if not all(k in body for k in required):
                return build_response(400, {"error": "Faltan campos requeridos"})

            result, status = init_couple(
                user,
                body["user1Name"],
                body["user2Name"],
                body["user1Email"],
                body["user2Email"],
            )
            return build_response(status, result)

        couple_pk, user = _resolve_couple_context(event)

        # ──────── GET /finances ──────────────────────────────────────────────
        if method == "GET" and path == "/finances":
            couple = get_couple(couple_pk)
            if not couple:
                return build_response(404, {"error": "Pareja no inicializada. Use POST /finances/init"})
            return build_response(200, couple)

        # ──────── GET /finances/resumen ──────────────────────────────────────
        if method == "GET" and path == "/finances/resumen":
            result, status = get_summary(couple_pk)
            return build_response(status, result)

        savings_response = _handle_savings_routes(method, path, event, body, couple_pk, user)
        if savings_response is not None:
            return savings_response

        # ──────── POST /finances/gastos ──────────────────────────────────────
        if method == "POST" and path == "/finances/gastos":
            required = {"title", "amount", "date", "category"}
            if not all(k in body for k in required):
                return build_response(400, {"error": "Faltan campos requeridos"})

            result, status = create_expense(
                couple_pk,
                body["title"],
                body["amount"],
                body["date"],
                body["category"],
                body.get("note"),
                user["sub"],
                user["email"],
            )
            return build_response(status, result)

        # ──────── GET /finances/gastos ───────────────────────────────────────
        if method == "GET" and path == "/finances/gastos":
            month = event.get("queryStringParameters", {}).get("month") if event.get("queryStringParameters") else None
            category = event.get("queryStringParameters", {}).get("category") if event.get("queryStringParameters") else None
            expenses, status = list_expenses(couple_pk, month, category)
            return build_response(status, {"expenses": expenses})

        # ──────── GET /finances/gastos/{gastoId} ─────────────────────────────
        gasto_id = get_path_param(event, "gastoId")
        if method == "GET" and gasto_id and "/gastos/" in path:
            expense, status = get_expense(couple_pk, gasto_id)
            return build_response(status, cast(dict, expense))

        # ──────── PUT /finances/gastos/{gastoId} ──────────────────────────────
        if method == "PUT" and gasto_id and "/gastos/" in path:
            result, status = update_expense(couple_pk, gasto_id, body)
            return build_response(status, cast(dict, result))

        # ──────── DELETE /finances/gastos/{gastoId} ──────────────────────────
        if method == "DELETE" and gasto_id and "/gastos/" in path:
            result, status = delete_expense(couple_pk, gasto_id)
            return build_response(status, cast(dict, result))

        # ──────── POST /finances/presupuesto/{monthYear} ──────────────────────
        month_year = get_path_param(event, "monthYear")
        if method == "POST" and "/presupuesto/" in path and month_year:
            if "amount" not in body:
                return build_response(400, {"error": "amount requerido"})
            result, status = set_budget(couple_pk, month_year, body["amount"], body.get("notes"))
            return build_response(status, cast(dict, result))

        # ──────── GET /finances/presupuesto/{monthYear} ──────────────────────
        if method == "GET" and "/presupuesto/" in path and month_year:
            result, status = get_budget(couple_pk, month_year)
            return build_response(status, cast(dict, result))

        # ──────── GET /finances/historico/{monthYear} ────────────────────────
        if method == "GET" and "/historico/" in path and month_year:
            result, status = get_monthly_history(couple_pk, month_year)
            return build_response(status, result)

        # ──────── GET /finances/historico ────────────────────────────────────
        if method == "GET" and path == "/finances/historico":
            limit = int(event.get("queryStringParameters", {}).get("limit", 12) if event.get("queryStringParameters") else 12)
            if not 1 <= limit <= 120:
                return build_response(400, {"error": "limit debe estar entre 1 y 120"})
            result, status = get_all_history(couple_pk, limit)
            return build_response(status, {"history": result})

        return build_response(404, {"error": "Ruta no encontrada"})

    except UnauthenticatedError as e:
        return build_response(401, {"error": str(e)})
    except ForbiddenError as e:
        return build_response(403, {"error": str(e)})
    except SavingsError as e:
        return build_response(e.status_code, {"error": str(e)})
    except ValueError as e:
        return build_response(400, {"error": str(e)})
    except Exception:
        logger.exception("🔴 Error inesperado en FinancesCRUD")
        return build_response(500, {"error": "Error interno del servidor"})
