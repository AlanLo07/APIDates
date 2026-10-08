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
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import cast

import boto3
from boto3.dynamodb.conditions import Attr, Key
from boto3.dynamodb.types import TypeSerializer
from botocore.config import Config
from botocore.exceptions import ClientError

from common.utils import build_response, get_path_param, log_event, parse_body, query_all  # type: ignore

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ.get("FINANCES_TABLE_NAME", "FinancesTable")
table = dynamodb.Table(TABLE_NAME)  # type: ignore
serializer = TypeSerializer()

# ─── Constantes Globales ──────────────────────────────────────────────────────

LEGACY_COUPLE_ID = "DEFAULT"
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


class UnauthenticatedError(PermissionError):
    pass


class ForbiddenError(PermissionError):
    pass

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
    if not user_id or not email:
        raise UnauthenticatedError("Usuario no autenticado")
    return {
        "sub": user_id,
        "email": email,
        "name": str(claims.get("name") or "").strip(),
    }


def _membership_key(prefix, identifier):
    return {"PK": f"{prefix}#{identifier}", "SK": "PAREJA"}


def _save_user_membership(user, couple_id):
    now = get_iso_timestamp()
    try:
        table.put_item(
            Item={
                **_membership_key("USUARIO", user["sub"]),
                "coupleId": couple_id,
                "email": user["email"],
                "createdAt": now,
                "updatedAt": now,
            },
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
    if user_membership:
        return f"PAREJA#{user_membership['coupleId']}", user

    email_membership = table.get_item(
            Key=_membership_key("EMAIL", user["email"]),
            ConsistentRead=True,
        ).get("Item")
    if email_membership:
        couple_id = email_membership["coupleId"]
        _save_user_membership(user, couple_id)
        return f"PAREJA#{couple_id}", user

    # Compatibilidad: adopta la partición histórica solo si el correo pertenece
    # a uno de sus integrantes.
    legacy = table.get_item(
        Key={"PK": f"PAREJA#{LEGACY_COUPLE_ID}", "SK": "META"},
        ConsistentRead=True,
    ).get("Item")
    legacy_emails = {
        _normalize_email(legacy.get(partner, {}).get("email"))
        for partner in ("user1", "user2")
    } if legacy else set()
    if user["email"] in legacy_emails:
        _save_user_membership(user, LEGACY_COUPLE_ID)
        return f"PAREJA#{LEGACY_COUPLE_ID}", user

    raise ForbiddenError("El usuario no pertenece a una pareja de finanzas")


def _couple_id_for_emails(user1_email, user2_email):
    normalized = sorted((_normalize_email(user1_email), _normalize_email(user2_email)))
    digest = hashlib.sha256("|".join(normalized).encode("utf-8")).hexdigest()
    return digest[:24]


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
    if user["email"] not in normalized_emails:
        return {"error": "El usuario autenticado debe ser uno de los integrantes"}, 403

    couple_id = _couple_id_for_emails(user1_email, user2_email)
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
        membership_items.append({
            **_membership_key("USUARIO", user["sub"]),
            "coupleId": couple_id,
            "email": user["email"],
            "createdAt": now,
            "updatedAt": now,
        })
        dynamodb.meta.client.transact_write_items(
            TransactItems=[
                {
                    "Put": {
                        "TableName": TABLE_NAME,
                        "Item": {key: serializer.serialize(value) for key, value in item.items()},
                        "ConditionExpression": "attribute_not_exists(PK)",
                    }
                }
                for item in [couple_item, *membership_items]
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


# ─── Handler principal ────────────────────────────────────────────────────────


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
            return build_response(status, expense)

        # ──────── PUT /finances/gastos/{gastoId} ──────────────────────────────
        if method == "PUT" and gasto_id and "/gastos/" in path:
            result, status = update_expense(couple_pk, gasto_id, body)
            return build_response(status, result)

        # ──────── DELETE /finances/gastos/{gastoId} ──────────────────────────
        if method == "DELETE" and gasto_id and "/gastos/" in path:
            result, status = delete_expense(couple_pk, gasto_id)
            return build_response(status, result)

        # ──────── POST /finances/presupuesto/{monthYear} ──────────────────────
        month_year = get_path_param(event, "monthYear")
        if method == "POST" and "/presupuesto/" in path and month_year:
            if "amount" not in body:
                return build_response(400, {"error": "amount requerido"})
            result, status = set_budget(couple_pk, month_year, body["amount"], body.get("notes"))
            return build_response(status, result)

        # ──────── GET /finances/presupuesto/{monthYear} ──────────────────────
        if method == "GET" and "/presupuesto/" in path and month_year:
            result, status = get_budget(couple_pk, month_year)
            return build_response(status, result)

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
    except ValueError as e:
        return build_response(400, {"error": str(e)})
    except Exception:
        logger.exception("🔴 Error inesperado en FinancesCRUD")
        return build_response(500, {"error": "Error interno del servidor"})
