"""Construccion compartida de checklists para escrituras atomicas."""
import uuid
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from boto3.dynamodb.types import TypeSerializer


VALID_KINDS = {"supermercado", "viaje", "deseos", "personalizado"}
VALID_PRIORITIES = {"alta", "media", "baja"}
KIND_DEFAULTS = {
    "supermercado": {"emoji": "\U0001f6d2", "colorValue": 0xFF4CAF50, "usaGrupos": True},
    "viaje": {"emoji": "\U0001f9f3", "colorValue": 0xFFFF8A65, "usaGrupos": True},
    "deseos": {"emoji": "\u2b50", "colorValue": 0xFFAB47BC, "usaGrupos": False},
    "personalizado": {"emoji": "\U0001f4cb", "colorValue": 0xFF6A88D6, "usaGrupos": False},
}
DEFAULT_GROUPS_BY_KIND = {
    "supermercado": [
        {"nombre": "Frutas y verduras", "emoji": "\U0001f966"},
        {"nombre": "L\u00e1cteos", "emoji": "\U0001f95b"},
        {"nombre": "Carnes y pescados", "emoji": "\U0001f357"},
        {"nombre": "Panader\u00eda", "emoji": "\U0001f35e"},
        {"nombre": "Despensa", "emoji": "\U0001f96b"},
        {"nombre": "Bebidas", "emoji": "\U0001f964"},
        {"nombre": "Congelados", "emoji": "\U0001f9ca"},
        {"nombre": "Limpieza", "emoji": "\U0001f9f4"},
        {"nombre": "Cuidado personal", "emoji": "\U0001f9fc"},
        {"nombre": "Otros", "emoji": "\U0001f4e6"},
    ],
    "viaje": [
        {"nombre": "Ropa", "emoji": "\U0001f455"},
        {"nombre": "Documentos", "emoji": "\U0001f6c2"},
        {"nombre": "Dinero y tarjetas", "emoji": "\U0001f4b3"},
        {"nombre": "Cosm\u00e9ticos y aseo", "emoji": "\U0001f9f4"},
        {"nombre": "Electr\u00f3nica", "emoji": "\U0001f50c"},
        {"nombre": "Salud", "emoji": "\U0001f48a"},
        {"nombre": "Otros", "emoji": "\U0001f392"},
    ],
}


def _text(value) -> str:
    return value.strip() if isinstance(value, str) else ""


def _identifier(value) -> str:
    if value is None:
        return str(uuid.uuid4())
    if not isinstance(value, str) or not value.strip():
        raise ValueError("El identificador debe ser texto no vacio")
    return value.strip()


def _number(value, field: str, integer: bool = False) -> Decimal:
    try:
        number = Decimal(str(value))
        if not number.is_finite():
            raise ValueError()
        return number.to_integral_value() if integer else number
    except (TypeError, ValueError, InvalidOperation) as exc:
        raise ValueError(f"El campo '{field}' debe ser numerico finito") from exc


def build_checklist_records(data: dict, is_default: bool = False) -> list[dict]:
    if not isinstance(data, dict):
        raise ValueError("El checklist debe ser un objeto JSON")
    titulo = _text(data.get("titulo"))
    if not titulo:
        raise ValueError("El campo 'titulo' es requerido para el checklist")
    kind = data.get("kind") or "personalizado"
    if not isinstance(kind, str) or kind not in VALID_KINDS:
        raise ValueError(f"'kind' invalido. Opciones: {', '.join(sorted(VALID_KINDS))}")
    defaults = KIND_DEFAULTS[kind]
    checklist_id = _identifier(data.get("id") or data.get("checklistId"))
    usa_grupos = data.get("usaGrupos", defaults["usaGrupos"])
    if not isinstance(usa_grupos, bool):
        raise ValueError("El campo 'usaGrupos' debe ser booleano")
    timestamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    common = {"pk": f"CHECKLIST#{checklist_id}", "checklistId": checklist_id,
              "createdAt": timestamp, "updatedAt": timestamp}
    records = [{
        **common, "sk": "META", "id": checklist_id, "entityType": "checklist_board",
        "titulo": titulo, "kind": kind,
        "emoji": _text(data.get("emoji")) or defaults["emoji"],
        "colorValue": _number(data.get("colorValue", defaults["colorValue"]), "colorValue", True),
        "usaGrupos": usa_grupos, "isDefault": bool(is_default),
    }]
    grupos = data.get("grupos", [])
    items = data.get("items", [])
    if not isinstance(grupos, list) or not isinstance(items, list):
        raise ValueError("Los campos 'grupos' e 'items' deben ser listas")
    if not grupos and usa_grupos:
        grupos = DEFAULT_GROUPS_BY_KIND.get(kind, [])
    if len(grupos) + len(items) > 98:
        raise ValueError("Se permiten hasta 98 grupos e items combinados al crear un checklist")
    group_ids = set()
    for orden, grupo in enumerate(grupos):
        if not isinstance(grupo, dict) or not _text(grupo.get("nombre")):
            raise ValueError("Cada grupo debe incluir 'nombre'")
        group_id = _identifier(grupo.get("id"))
        group_ids.add(group_id)
        records.append({
            **common, "sk": f"GRUPO#{group_id}", "id": group_id,
            "entityType": "checklist_grupo", "type": "checklist_grupo",
            "nombre": _text(grupo.get("nombre")), "emoji": _text(grupo.get("emoji")) or None,
            "orden": _number(grupo.get("orden", orden), "orden", True),
        })
    for item in items:
        if not isinstance(item, dict) or not _text(item.get("nombre")):
            raise ValueError("Cada item debe incluir 'nombre'")
        if "incluido" in item:
            raise ValueError("El campo 'incluido' ya no se admite; usa 'comprado'")
        prioridad = item.get("prioridad") or "media"
        if not isinstance(prioridad, str) or prioridad not in VALID_PRIORITIES:
            raise ValueError("'prioridad' debe ser alta, media o baja")
        priority_order = _number(item.get("prioridadOrden", 999), "prioridadOrden")
        if priority_order < 1 or priority_order != priority_order.to_integral_value():
            raise ValueError("El campo 'prioridadOrden' debe ser un entero mayor o igual a 1")
        comprado = item.get("comprado", False)
        if not isinstance(comprado, bool):
            raise ValueError("El campo 'comprado' debe ser booleano")
        group_id = _text(item.get("groupId")) or None
        if group_id and group_id not in group_ids:
            raise ValueError("El groupId del item debe pertenecer a los grupos enviados")
        item_id = _identifier(item.get("id"))
        records.append({
            **common, "sk": f"ITEM#{item_id}", "id": item_id,
            "entityType": "checklist_item", "type": "checklist_item",
            "nombre": _text(item.get("nombre")), "groupId": group_id,
            "prioridad": prioridad, "prioridadOrden": priority_order,
            "precio": _number(item["precio"], "precio") if item.get("precio") is not None else None,
            "emoji": _text(item.get("emoji")) or None, "comprado": comprado,
            "nota": _text(item.get("nota")) or None,
        })
    if len({record["sk"] for record in records}) != len(records):
        raise ValueError("Los identificadores de grupos o items no pueden repetirse")
    return records


def checklist_puts(table_name: str, records: list[dict]) -> list[dict]:
    return [{"Put": {
        "TableName": table_name, "Item": record,
        "ConditionExpression": "attribute_not_exists(pk) AND attribute_not_exists(sk)",
    }} for record in records]


def write_transaction(client, actions: list[dict]):
    serializer = TypeSerializer()
    serialized = []
    for action in actions:
        operation, detail = next(iter(action.items()))
        payload = dict(detail)
        for field in ("Item", "Key", "ExpressionAttributeValues"):
            if field in payload:
                payload[field] = {key: serializer.serialize(value) for key, value in payload[field].items()}
        serialized.append({operation: payload})
    return client.transact_write_items(TransactItems=serialized)