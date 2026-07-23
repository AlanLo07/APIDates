"""
finances_handler.py — CRUD para administración de finanzas de una pareja.

Modelo Simplificado (Una pareja única):
- Una tabla DynamoDB con estructura PK/SK compuesta.
- PK fija: PAREJA#DEFAULT (siempre la misma pareja)
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
✅ Modelo simplificado para una sola pareja
✅ Validación de estructura de datos
✅ Cálculo automático de presupuestos y históricos
✅ Filtrado por mes y categoría
✅ Auditoría de cambios (createdAt, updatedAt)
"""
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.config import Config
from botocore.exceptions import ClientError

from common.utils import build_response, get_path_param, parse_body  # type: ignore

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ.get("FINANCES_TABLE_NAME", "FinancesTable")
table = dynamodb.Table(TABLE_NAME)  # type: ignore

# ─── Constantes Globales ──────────────────────────────────────────────────────

PAREJA_ID = "DEFAULT"  # 🔵 ID fijo para la única pareja
PAREJA_PK = f"PAREJA#{PAREJA_ID}"

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

# ─── Utilidades ───────────────────────────────────────────────────────────────


def get_iso_timestamp():
    """🔵 Devuelve timestamp ISO 8601 UTC."""
    return datetime.now(timezone.utc).isoformat()


def validate_expense(expense_data):
    """🔵 Valida estructura de gasto."""
    errors = []
    for field in REQUIRED_EXPENSE_FIELDS:
        if field not in expense_data or expense_data[field] is None:
            errors.append(f"Campo requerido faltante: {field}")

    # Validar monto
    try:
        amount = float(expense_data.get("amount", 0))
        if amount <= 0:
            errors.append("El monto debe ser mayor a 0")
    except (ValueError, TypeError):
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
    if "amount" not in budget_data or budget_data["amount"] is None:
        errors.append("amount es requerido")

    try:
        amount = float(budget_data.get("amount", 0))
        if amount < 0:
            errors.append("El presupuesto no puede ser negativo")
    except (ValueError, TypeError):
        errors.append("Presupuesto debe ser un número válido")

    return errors


def get_month_year_from_date(date_str):
    """🔵 Extrae YYYY-MM de fecha ISO."""
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m")
    except:
        return None


def calculate_monthly_stats(month_year):
    """🔵 Calcula estadísticas mensuales automáticamente (pareja única)."""
    try:
        response = table.query(
            KeyConditionExpression=Key("PK").eq(PAREJA_PK)
            & Key("SK").begins_with("GASTO#"),
            ProjectionExpression="SK,amount,category,#date",
            ExpressionAttributeNames={"#date": "date"},
        )

        items = response.get("Items", [])
        month_expenses = [
            item
            for item in items
            if get_month_year_from_date(item.get("date", "")) == month_year
        ]

        total_spent = sum(float(item["amount"]) for item in month_expenses)

        # Agrupar por categoría
        by_category = {}
        for item in month_expenses:
            cat = item.get("category", "others")
            by_category[cat] = by_category.get(cat, 0) + float(item["amount"])

        # Obtener presupuesto
        budget_response = table.get_item(
            Key={"PK": PAREJA_PK, "SK": f"PRESUPUESTO#{month_year}"}
        )
        budget_amount = float(
            budget_response.get("Item", {}).get("amount", 0)
        )

        over_budget = total_spent > budget_amount if budget_amount > 0 else False

        return {
            "monthYear": month_year,
            "totalSpent": Decimal(str(total_spent)),
            "budgetAmount": Decimal(str(budget_amount)),
            "byCategory": {k: Decimal(str(v)) for k, v in by_category.items()},
            "expenseCount": len(month_expenses),
            "overBudget": over_budget,
            "difference": Decimal(str(budget_amount - total_spent)) if budget_amount > 0 else Decimal("0"),
            "calculatedAt": get_iso_timestamp(),
        }
    except ClientError as e:
        logger.error(f"🔴 Error calculando estadísticas: {e}")
        return None


# ─── Operaciones de Parejas ───────────────────────────────────────────────────

def init_couple(user1_name, user2_name, user1_email, user2_email):
    """🟢 Inicializa datos de la pareja (una sola vez)."""
    now = get_iso_timestamp()

    try:
        table.put_item(
            Item={
                "PK": PAREJA_PK,
                "SK": "META",
                "user1": {
                    "name": user1_name,
                    "email": user1_email,
                },
                "user2": {
                    "name": user2_name,
                    "email": user2_email,
                },
                "monthlyBudget": Decimal("0"),
                "currency": "$",
                "locale": "es_ES",
                "createdAt": now,
                "updatedAt": now,
            }
        )

        logger.info(f"🟢 Pareja inicializada")
        return {
            "user1": {"name": user1_name, "email": user1_email},
            "user2": {"name": user2_name, "email": user2_email},
            "createdAt": now,
        }
    except ClientError as e:
        logger.error(f"🔴 Error inicializando pareja: {e}")
        raise


def get_couple():
    """🟢 Obtiene datos de la pareja única."""
    try:
        response = table.get_item(Key={"PK": PAREJA_PK, "SK": "META"})
        if "Item" not in response:
            return None
        return response["Item"]
    except ClientError as e:
        logger.error(f"🔴 Error obteniendo pareja: {e}")
        return None


# ─── Operaciones de Gastos ────────────────────────────────────────────────────


def create_expense(title, amount, date, category, note=None, created_by=None):
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
            "PK": PAREJA_PK,
            "SK": f"GASTO#{gasto_id}",
            "gastoId": gasto_id,
            "title": title,
            "amount": Decimal(str(amount)),
            "date": date,
            "category": category,
            "monthYear": month_year,
            "note": note,
            "createdBy": created_by or "unknown",
            "createdAt": now,
            "updatedAt": now,
        }

        table.put_item(Item=expense_item)

        # Calcular y actualizar estadísticas mensuales
        stats = calculate_monthly_stats(month_year)
        if stats:
            table.put_item(
                Item={
                    "PK": PAREJA_PK,
                    "SK": f"HISTORICO#{month_year}",
                    **stats,
                }
            )

        logger.info(f"🟢 Gasto creado: {gasto_id} en {month_year}")
        return expense_item, 201
    except ClientError as e:
        logger.error(f"🔴 Error creando gasto: {e}")
        return {"error": str(e)}, 500


def list_expenses(month_year=None, category=None):
    """🟢 Lista gastos con filtros opcionales."""
    try:
        response = table.query(
            KeyConditionExpression=Key("PK").eq(PAREJA_PK)
            & Key("SK").begins_with("GASTO#"),
            ScanIndexForward=False,  # Más recientes primero
        )

        expenses = response.get("Items", [])

        # Filtrar por mes si se especifica
        if month_year:
            expenses = [e for e in expenses if e.get("monthYear") == month_year]

        # Filtrar por categoría si se especifica
        if category:
            expenses = [e for e in expenses if e.get("category") == category]

        return expenses, 200
    except ClientError as e:
        logger.error(f"🔴 Error listando gastos: {e}")
        return {"error": str(e)}, 500


def get_expense(gasto_id):
    """🟢 Obtiene un gasto específico."""
    try:
        response = table.get_item(
            Key={"PK": PAREJA_PK, "SK": f"GASTO#{gasto_id}"}
        )
        if "Item" not in response:
            return None, 404
        return response["Item"], 200
    except ClientError as e:
        logger.error(f"🔴 Error obteniendo gasto: {e}")
        return {"error": str(e)}, 500


def update_expense(gasto_id, update_data):
    """🟢 Actualiza un gasto existente."""
    try:
        # Obtener el gasto actual
        current, status = get_expense(gasto_id)
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
        update_data["updatedAt"] = now

        # Construir expresión de actualización
        update_expr_parts = []
        expr_values = {}
        expr_names = {}

        for key, value in update_data.items():
            if key not in ["PK", "SK", "gastoId", "createdAt"]:
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
            Key={"PK": PAREJA_PK, "SK": f"GASTO#{gasto_id}"},
            UpdateExpression="SET " + ", ".join(update_expr_parts),
            ExpressionAttributeNames=expr_names,
            ExpressionAttributeValues=expr_values,
            ReturnValues="ALL_NEW",
        )

        # Recalcular estadísticas si cambió el mes o el monto
        if "date" in update_data or "amount" in update_data:
            month_year = current.get("monthYear") # type: ignore
            if month_year:
                calculate_monthly_stats(month_year)

        logger.info(f"🟢 Gasto actualizado: {gasto_id}")
        return response["Attributes"], 200
    except ClientError as e:
        logger.error(f"🔴 Error actualizando gasto: {e}")
        return {"error": str(e)}, 500


def delete_expense(gasto_id):
    """🟢 Elimina un gasto."""
    try:
        # Obtener el gasto para conocer su mes
        expense, status = get_expense(gasto_id)
        if status != 200:
            return expense, status

        month_year = expense.get("monthYear") # type: ignore

        table.delete_item(Key={"PK": PAREJA_PK, "SK": f"GASTO#{gasto_id}"})

        # Recalcular estadísticas mensuales
        if month_year:
            calculate_monthly_stats(month_year)

        logger.info(f"🟢 Gasto eliminado: {gasto_id}")
        return {"message": "Gasto eliminado exitosamente"}, 200
    except ClientError as e:
        logger.error(f"🔴 Error eliminando gasto: {e}")
        return {"error": str(e)}, 500


# ─── Operaciones de Presupuestos ──────────────────────────────────────────────


def set_budget(month_year, amount, notes=None):
    """🟢 Establece o actualiza presupuesto mensual."""
    errors = validate_budget({"monthYear": month_year, "amount": amount})
    if errors:
        return {"error": errors}, 400

    now = get_iso_timestamp()

    try:
        table.put_item(
            Item={
                "PK": PAREJA_PK,
                "SK": f"PRESUPUESTO#{month_year}",
                "monthYear": month_year,
                "amount": Decimal(str(amount)),
                "notes": notes,
                "createdAt": now,
                "updatedAt": now,
            }
        )

        # Recalcular estadísticas con el nuevo presupuesto
        stats = calculate_monthly_stats(month_year)
        if stats:
            table.put_item(
                Item={
                    "PK": PAREJA_PK,
                    "SK": f"HISTORICO#{month_year}",
                    **stats,
                }
            )

        logger.info(f"🟢 Presupuesto establecido: {month_year} = {amount}")
        return {
            "monthYear": month_year,
            "amount": Decimal(str(amount)),
            "updatedAt": now,
        }, 200
    except ClientError as e:
        logger.error(f"🔴 Error estableciendo presupuesto: {e}")
        return {"error": str(e)}, 500


def get_budget(month_year):
    """🟢 Obtiene presupuesto de un mes."""
    try:
        response = table.get_item(
            Key={"PK": PAREJA_PK, "SK": f"PRESUPUESTO#{month_year}"}
        )
        if "Item" not in response:
            return None, 404
        return response["Item"], 200
    except ClientError as e:
        logger.error(f"🔴 Error obteniendo presupuesto: {e}")
        return {"error": str(e)}, 500


# ─── Operaciones de Históricos ────────────────────────────────────────────────


def get_monthly_history(month_year):
    """🟢 Obtiene estadísticas de un mes específico."""
    try:
        response = table.get_item(
            Key={"PK": PAREJA_PK, "SK": f"HISTORICO#{month_year}"}
        )
        if "Item" not in response:
            # Si no existe, calcularlo
            return calculate_monthly_stats(month_year), 200
        return response["Item"], 200
    except ClientError as e:
        logger.error(f"🔴 Error obteniendo histórico: {e}")
        return {"error": str(e)}, 500


def get_all_history(limit=12):
    """🟢 Obtiene histórico de últimos N meses."""
    try:
        response = table.query(
            KeyConditionExpression=Key("PK").eq(PAREJA_PK)
            & Key("SK").begins_with("HISTORICO#"),
            ScanIndexForward=False,  # Más recientes primero
            Limit=limit,
        )
        items = response.get("Items", [])
        return items, 200
    except ClientError as e:
        logger.error(f"🔴 Error obteniendo histórico: {e}")
        return {"error": str(e)}, 500


# ─── Operaciones de Resumen ───────────────────────────────────────────────────


def get_summary():
    """🔵 Obtiene resumen completo de finanzas (muy relevante)."""
    try:
        couple = get_couple()
        if not couple:
            return {"error": "Pareja no inicializada"}, 404

        # Obtener todos los gastos
        expenses, _ = list_expenses()

        # Gasto total
        total_spent = sum(float(e["amount"]) for e in expenses) # type: ignore

        # Gasto de esta semana (últimos 7 días)
        from datetime import timedelta

        now = datetime.now(timezone.utc)
        week_ago = (now - timedelta(days=7)).isoformat()
        weekly_spent = sum(
            float(e["amount"]) # type: ignore
            for e in expenses
            if e.get("date", "") >= week_ago # type: ignore
        )

        # Meses disponibles
        months = sorted(set(e.get("monthYear") for e in expenses if e.get("monthYear"))) # type: ignore

        # Estadísticas por categoría
        by_category = {}
        for expense in expenses:
            cat = expense.get("category", "others") # type: ignore
            by_category[cat] = by_category.get(cat, 0) + float(expense["amount"]) # type: ignore

        return {
            "couple": couple,
            "totalSpent": Decimal(str(total_spent)),
            "weeklySpent": Decimal(str(weekly_spent)),
            "expenseCount": len(expenses),
            "availableMonths": months,
            "byCategory": {k: Decimal(str(v)) for k, v in by_category.items()},
            "expenseCategories": EXPENSE_CATEGORIES,
        }, 200
    except ClientError as e:
        logger.error(f"🔴 Error obteniendo resumen: {e}")
        return {"error": str(e)}, 500


# ─── Handler principal ────────────────────────────────────────────────────────


def lambda_handler(event, context):
    """🔵 Maneja todas las rutas de la API de finanzas."""
    logger.info(json.dumps({"event_keys": list(event.keys())}))

    method = event.get("requestContext", {}).get("http", {}).get("method", "")
    path = event.get("rawPath", "")
    body = parse_body(event)

    logger.info(f"⚪️  {method} {path}")

    try:
        # ──────── POST /finances/init ─────────────────────────────────────────
        if method == "POST" and path == "/finances/init":
            required = {"user1Email", "user2Email", "user1Name", "user2Name"}
            if not all(k in body for k in required):
                return build_response(400, {"error": "Faltan campos requeridos"})

            result = init_couple(
                body["user1Name"],
                body["user2Name"],
                body["user1Email"],
                body["user2Email"],
            )
            return build_response(201, result)

        # ──────── GET /finances ──────────────────────────────────────────────
        if method == "GET" and path == "/finances":
            couple = get_couple()
            if not couple:
                return build_response(404, {"error": "Pareja no inicializada. Use POST /finances/init"})
            return build_response(200, couple)

        # ──────── GET /finances/resumen ──────────────────────────────────────
        if method == "GET" and path == "/finances/resumen":
            result, status = get_summary()
            return build_response(status, result)

        # ──────── POST /finances/gastos ──────────────────────────────────────
        if method == "POST" and path == "/finances/gastos":
            required = {"title", "amount", "date", "category"}
            if not all(k in body for k in required):
                return build_response(400, {"error": "Faltan campos requeridos"})

            result, status = create_expense(
                body["title"],
                body["amount"],
                body["date"],
                body["category"],
                body.get("note"),
                body.get("createdBy"),
            )
            return build_response(status, result)

        # ──────── GET /finances/gastos ───────────────────────────────────────
        if method == "GET" and path == "/finances/gastos":
            month = event.get("queryStringParameters", {}).get("month") if event.get("queryStringParameters") else None
            category = event.get("queryStringParameters", {}).get("category") if event.get("queryStringParameters") else None
            expenses, status = list_expenses(month, category)
            return build_response(status, {"expenses": expenses})

        # ──────── GET /finances/gastos/{gastoId} ─────────────────────────────
        gasto_id = get_path_param(event, "gastoId")
        if method == "GET" and gasto_id and "/gastos/" in path:
            expense, status = get_expense(gasto_id)
            return build_response(status, expense)

        # ──────── PUT /finances/gastos/{gastoId} ──────────────────────────────
        if method == "PUT" and gasto_id and "/gastos/" in path:
            result, status = update_expense(gasto_id, body)
            return build_response(status, result)

        # ──────── DELETE /finances/gastos/{gastoId} ──────────────────────────
        if method == "DELETE" and gasto_id and "/gastos/" in path:
            result, status = delete_expense(gasto_id)
            return build_response(status, result)

        # ──────── POST /finances/presupuesto/{monthYear} ──────────────────────
        month_year = get_path_param(event, "monthYear")
        if method == "POST" and "/presupuesto/" in path and month_year:
            if "amount" not in body:
                return build_response(400, {"error": "amount requerido"})
            result, status = set_budget(month_year, body["amount"], body.get("notes"))
            return build_response(status, result)

        # ──────── GET /finances/presupuesto/{monthYear} ──────────────────────
        if method == "GET" and "/presupuesto/" in path and month_year:
            result, status = get_budget(month_year)
            return build_response(status, result)

        # ──────── GET /finances/historico/{monthYear} ────────────────────────
        if method == "GET" and "/historico/" in path and month_year:
            result, status = get_monthly_history(month_year)
            return build_response(status, result)

        # ──────── GET /finances/historico ────────────────────────────────────
        if method == "GET" and path == "/finances/historico":
            limit = int(event.get("queryStringParameters", {}).get("limit", 12) if event.get("queryStringParameters") else 12)
            result, status = get_all_history(limit)
            return build_response(status, {"history": result})

        return build_response(404, {"error": "Ruta no encontrada"})

    except Exception as e:
        logger.error(f"🔴 Error inesperado: {str(e)}", exc_info=True)
        return build_response(500, {"error": "Error interno del servidor"})
