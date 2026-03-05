"""
utils.py — Utilidades compartidas entre Lambdas
Mejoras aplicadas:
- Serialización de Decimal para JSON
- build_response centralizado con JSON body serializado
- Constantes y helpers reutilizables
"""
import json
from decimal import Decimal


class DecimalEncoder(json.JSONEncoder):
    """Serializa Decimal de DynamoDB a float/int para JSON."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            # Conserva entero si no tiene decimales
            return int(obj) if obj % 1 == 0 else float(obj)
        return super().default(obj)


def build_response(status_code: int, body: dict | list) -> dict:
    """
    Construye una respuesta HTTP estándar con CORS.
    El body se serializa a JSON string (requerido por Lambda Function URL y API Gateway).
    """
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,PATCH,OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,X-Api-Key",
        },
        "body": json.dumps(body, cls=DecimalEncoder),
    }


def parse_body(event: dict) -> dict | list:
    """Parsea el body del evento de forma segura."""
    raw = event.get("body") or "{}"
    if isinstance(raw, (dict, list)):
        return raw
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"Body inválido: {e}") from e


def get_path_param(event: dict, key: str) -> str | None:
    """Obtiene un path parameter de forma segura."""
    return (event.get("pathParameters") or {}).get(key)