"""Pruebas funcionales de MonthlyChallengesCRUD.

Usa una tabla DynamoDB "falsa" en memoria (sin red ni Docker) que evalúa
ConditionExpression/UpdateExpression reales para validar la lógica de
concurrencia, acuerdo/desacuerdo y creación de cupón exactamente como se
comportaría DynamoDB.
"""
import copy
import importlib.util
import json
import os
import re
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
COMMON_LAYER = ROOT / "lambdas" / "layers" / "baselayer" / "python"
HANDLER_PATH = ROOT / "lambdas" / "functions" / "MonthlyChallengesCRUD" / "handler.py"
sys.path.insert(0, str(COMMON_LAYER))
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")

deserializer = TypeDeserializer()


def _client_error(code, message="mock"):
    return ClientError({"Error": {"Code": code, "Message": message}}, "MockOperation")


def _eval_condition_object(expr, item):
    """Evalúa objetos boto3.dynamodb.conditions (Attr/Key) contra un item existente."""
    if expr is None:
        return True
    op = type(expr).__name__
    if op == "And":
        left, right = expr.values
        return _eval_condition_object(left, item) and _eval_condition_object(right, item)
    if op == "Or":
        left, right = expr.values
        return _eval_condition_object(left, item) or _eval_condition_object(right, item)
    if op == "AttributeExists":
        (attr,) = expr.values
        return attr.name in item
    if op == "AttributeNotExists":
        (attr,) = expr.values
        return attr.name not in item
    if op == "Equals":
        attr, value = expr.values
        return item.get(attr.name) == value
    raise NotImplementedError(f"Condición no soportada en pruebas: {op}")


def _extract_key_condition(expr):
    """Extrae {'PK': ('eq', value), 'SK': ('begins_with'|'eq', value)} de un KeyConditionExpression."""
    result = {}
    op = type(expr).__name__
    if op == "And":
        left, right = expr.values
        result.update(_extract_key_condition(left))
        result.update(_extract_key_condition(right))
    elif op == "Equals":
        attr, value = expr.values
        result[attr.name] = ("eq", value)
    elif op == "BeginsWith":
        attr, value = expr.values
        result[attr.name] = ("begins_with", value)
    return result


def _eval_condition_string(expr, item, names=None, values=None):
    """Evalúa ConditionExpression en formato string (usadas en update_item y transacciones)."""
    if not expr:
        return True
    names = names or {}
    values = values or {}
    expr = expr.strip()
    m = re.fullmatch(r"attribute_not_exists\(([#\w]+)\)", expr)
    if m:
        field = names.get(m.group(1), m.group(1))
        return field not in item
    m = re.fullmatch(r"attribute_exists\(([#\w]+)\)", expr)
    if m:
        field = names.get(m.group(1), m.group(1))
        return field in item
    for part in expr.split(" AND "):
        left, right = [p.strip() for p in part.split("=")]
        field = names.get(left, left)
        val = values.get(right, right)
        if item.get(field) != val:
            return False
    return True


def _apply_update_expression(expr, item, names=None, values=None):
    names = names or {}
    values = values or {}
    assert expr.strip().startswith("SET "), f"Solo se soporta SET en pruebas: {expr}"
    body = expr.strip()[4:]
    for assignment in body.split(","):
        field_token, value_token = [p.strip() for p in assignment.split("=")]
        field = names.get(field_token, field_token)
        val_placeholder = value_token.strip()
        item[field] = values.get(val_placeholder, val_placeholder)


class FakeTable:
    """Emula lo mínimo de una tabla DynamoDB necesario para MonthlyChallengesCRUD."""

    def __init__(self, name):
        self.name = name
        self.items = {}

    @staticmethod
    def _key(key):
        return (key["PK"], key["SK"])

    def get_item(self, Key, **kwargs):
        item = self.items.get(self._key(Key))
        return {"Item": copy.deepcopy(item)} if item is not None else {}

    def put_item(self, Item, ConditionExpression=None, **kwargs):
        key = (Item["PK"], Item["SK"])
        existing = self.items.get(key, {})
        if not _eval_condition_object(ConditionExpression, existing):
            raise _client_error("ConditionalCheckFailedException")
        self.items[key] = copy.deepcopy(Item)
        return {}

    def delete_item(self, Key, ConditionExpression=None, **kwargs):
        key = self._key(Key)
        existing = self.items.get(key, {})
        if not _eval_condition_object(ConditionExpression, existing):
            raise _client_error("ConditionalCheckFailedException")
        self.items.pop(key, None)
        return {}

    def update_item(self, Key, UpdateExpression, ExpressionAttributeValues=None,
                     ExpressionAttributeNames=None, ConditionExpression=None, ReturnValues=None, **kwargs):
        key = self._key(Key)
        existing = self.items.get(key)
        if existing is None:
            raise _client_error("ConditionalCheckFailedException")
        if not _eval_condition_string(ConditionExpression, existing, ExpressionAttributeNames, ExpressionAttributeValues):
            raise _client_error("ConditionalCheckFailedException")
        new_item = copy.deepcopy(existing)
        _apply_update_expression(UpdateExpression, new_item, ExpressionAttributeNames, ExpressionAttributeValues)
        self.items[key] = new_item
        if ReturnValues == "ALL_NEW":
            return {"Attributes": copy.deepcopy(new_item)}
        return {}

    def query(self, KeyConditionExpression, ScanIndexForward=True, Limit=None, **kwargs):
        cond = _extract_key_condition(KeyConditionExpression)
        pk = cond.get("PK", (None, None))[1]
        sk_cond = cond.get("SK")
        results = []
        for (ipk, isk), item in self.items.items():
            if ipk != pk:
                continue
            if sk_cond:
                op, val = sk_cond
                if op == "begins_with" and not isk.startswith(val):
                    continue
                if op == "eq" and isk != val:
                    continue
            results.append(copy.deepcopy(item))
        results.sort(key=lambda entry: entry["SK"], reverse=not ScanIndexForward)
        if Limit is not None:
            results = results[:Limit]
        return {"Items": results}

    def scan(self, FilterExpression=None, **kwargs):
        results = [copy.deepcopy(i) for i in self.items.values() if _eval_condition_object(FilterExpression, i)]
        return {"Items": results}


class FakeDynamoClient:
    """Emula transact_write_items ejecutando sobre las FakeTable reales del test."""

    def __init__(self, tables_by_name):
        self.tables_by_name = tables_by_name

    def transact_write_items(self, TransactItems):
        # Primera pasada: validar condiciones sin aplicar nada (atomicidad)
        planned = []
        for action in TransactItems:
            op, detail = next(iter(action.items()))
            fake_table = self.tables_by_name[detail["TableName"]]
            if op == "Put":
                item = {k: deserializer.deserialize(v) for k, v in detail["Item"].items()}
                key = (item["PK"], item["SK"])
                existing = fake_table.items.get(key, {})
                if not _eval_condition_string(detail.get("ConditionExpression"), existing):
                    raise _client_error("TransactionCanceledException")
                planned.append((fake_table, "put", key, item))
            elif op == "Update":
                key_map = {k: deserializer.deserialize(v) for k, v in detail["Key"].items()}
                key = (key_map["PK"], key_map["SK"])
                existing = fake_table.items.get(key)
                if existing is None:
                    raise _client_error("TransactionCanceledException")
                values = {k: deserializer.deserialize(v) for k, v in detail.get("ExpressionAttributeValues", {}).items()}
                names = detail.get("ExpressionAttributeNames", {})
                if not _eval_condition_string(detail.get("ConditionExpression"), existing, names, values):
                    raise _client_error("TransactionCanceledException")
                new_item = copy.deepcopy(existing)
                _apply_update_expression(detail["UpdateExpression"], new_item, names, values)
                planned.append((fake_table, "update", key, new_item))
            else:
                raise NotImplementedError(op)
        # Segunda pasada: aplicar
        for fake_table, _kind, key, item in planned:
            fake_table.items[key] = item
        return {}


def load_handler():
    monthly_table = FakeTable("MonthlyChallengesTable")
    catalog_table = FakeTable("MonthlyChallengesCatalogTable")
    tables_by_name = {"MonthlyChallengesTable": monthly_table, "MonthlyChallengesCatalogTable": catalog_table}

    resource = MagicMock()
    resource.Table.side_effect = lambda name: tables_by_name[name]
    resource.meta.client = FakeDynamoClient(tables_by_name)

    with patch("boto3.resource", return_value=resource):
        spec = importlib.util.spec_from_file_location("monthly_challenges_handler_test", HANDLER_PATH)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

    import common.utils as common_utils
    common_utils._SCAN_CACHE.clear()
    return module, monthly_table, catalog_table


def user_claims(sub, email, name="Nombre"):
    return {"sub": sub, "email": email, "name": name}


def make_event(path, method="GET", sub="user-1", email="uno@example.com", body=None, path_params=None):
    return {
        "rawPath": path,
        "body": json.dumps(body) if body is not None else "{}",
        "requestContext": {"http": {"method": method}, "authorizer": {"jwt": {"claims": user_claims(sub, email)}}},
        "pathParameters": path_params or {},
    }


def lambda_context():
    return SimpleNamespace(function_name="monthly-challenges-crud-test")


class MonthlyChallengesHandlerTests(unittest.TestCase):
    def setUp(self):
        self.handler, self.table, self.catalog_table = load_handler()
        self.user1 = {"sub": "sub-1", "email": "uno@example.com", "name": "Uno"}
        self.user2 = {"sub": "sub-2", "email": "dos@example.com", "name": "Dos"}
        self.couple_pk = self._init_couple()

    def _init_couple(self):
        result, status = self.handler.init_couple(
            self.user1, "Uno", "Dos", self.user1["email"], self.user2["email"]
        )
        self.assertEqual(status, 201)
        return f"PAREJA#{result['coupleId']}"

    def _add_catalog_item(self, title="Cena sorpresa", description="Preparen una cena", category="actividades_juntos"):
        item, status = self.handler.create_catalog_challenge(
            {"title": title, "description": description, "category": category}
        )
        self.assertEqual(status, 201)
        return item

    # ── Aislamiento entre parejas ─────────────────────────────────────────

    def test_couple_isolation(self):
        stranger = {"sub": "sub-99", "email": "ajeno@example.com", "name": "Ajeno"}
        with self.assertRaises(self.handler.ForbiddenError):
            self.handler._resolve_couple_context(make_event("/monthly-challenges/current", sub=stranger["sub"],
                                                              email=stranger["email"]))

        # Propuesta de la pareja real no debe ser visible bajo otra partición
        self.handler.create_or_update_proposal(self.couple_pk, self.user1,
                                                {"source": "custom", "title": "Picnic", "description": "Al parque",
                                                 "category": "diversion"})
        other_couple_pk = "PAREJA#otraPareja12345678901234"
        state = self.handler.build_month_state(
            other_couple_pk, self.handler._load_current_month(other_couple_pk), self.user1
        )
        self.assertEqual(state["proposals"], [])

    def test_access_token_without_email_uses_user_membership(self):
        membership = self.table.items[(f"USUARIO#{self.user1['sub']}", "PAREJA")]
        membership.pop("email", None)
        request = make_event("/monthly-challenges/current", sub=self.user1["sub"], email=self.user1["email"])
        del request["requestContext"]["authorizer"]["jwt"]["claims"]["email"]

        couple_pk, user = self.handler._resolve_couple_context(request)

        self.assertEqual(couple_pk, self.couple_pk)
        self.assertEqual(user["sub"], self.user1["sub"])
        self.assertEqual(user["email"], "")

    def test_legacy_email_membership_is_found_without_email_claim(self):
        legacy_email = "legacy@example.com"
        self.table.items[(f"EMAIL#{legacy_email}", "PAREJA")] = {
            "PK": f"EMAIL#{legacy_email}", "SK": "PAREJA",
            "coupleId": self.couple_pk.removeprefix("PAREJA#"),
        }
        request = make_event("/monthly-challenges/current", sub="sub-legacy", email="")
        del request["requestContext"]["authorizer"]["jwt"]["claims"]["email"]
        request["headers"] = {"Authorization": "Bearer access-token"}

        with patch.object(self.handler, "_email_from_access_token", return_value=legacy_email):
            couple_pk, user = self.handler._resolve_couple_context(request)

        self.assertEqual(couple_pk, self.couple_pk)
        self.assertEqual(user["sub"], "sub-legacy")
        promoted = self.table.get_item(
            Key=self.handler._membership_key("USUARIO", "sub-legacy")
        )["Item"]
        self.assertEqual(promoted["coupleId"], self.couple_pk.removeprefix("PAREJA#"))

    def test_init_without_token_email_preserves_email_based_couple_id(self):
        user = {"sub": "sub-99", "email": "", "name": "Nueva"}
        result, status = self.handler.init_couple(
            user, "Nueva", "Pareja", "nueva@example.com", "pareja@example.com"
        )

        expected_id = self.handler._couple_id_for_emails(
            "nueva@example.com", "pareja@example.com"
        )
        self.assertEqual(status, 201)
        self.assertEqual(result["coupleId"], expected_id)
        membership = self.table.get_item(
            Key=self.handler._membership_key("USUARIO", user["sub"])
        )["Item"]
        self.assertNotIn("email", membership)

    # ── Propuestas simultáneas y silencio != consentimiento ───────────────

    def test_simultaneous_proposals_are_recorded_independently(self):
        self.handler.create_or_update_proposal(self.couple_pk, self.user1,
                                                {"source": "custom", "title": "Picnic", "description": "Al parque",
                                                 "category": "diversion"})
        self.handler.create_or_update_proposal(self.couple_pk, self.user2,
                                                {"source": "custom", "title": "Cine", "description": "Ver película",
                                                 "category": "diversion"})
        month_record = self.handler._load_current_month(self.couple_pk)
        state = self.handler.build_month_state(self.couple_pk, month_record, self.user1)
        self.assertEqual(state["status"], "PENDING")
        self.assertEqual(len(state["proposals"]), 2)
        proposers = {p["proposedBy"] for p in state["proposals"]}
        self.assertEqual(proposers, {self.user1["sub"], self.user2["sub"]})

    def test_single_proposal_does_not_auto_assign_without_choice(self):
        self.handler.create_or_update_proposal(self.couple_pk, self.user1,
                                                {"source": "custom", "title": "Picnic", "description": "Al parque",
                                                 "category": "diversion"})
        month_record = self.handler._load_current_month(self.couple_pk)
        state = self.handler.build_month_state(self.couple_pk, month_record, self.user2)
        self.assertEqual(state["status"], "PENDING")
        self.assertIn("chooseProposal", state["availableActions"])

    # ── Desacuerdo y acuerdo ───────────────────────────────────────────────

    def test_disagreement_when_choices_differ(self):
        self.handler.create_or_update_proposal(self.couple_pk, self.user1,
                                                {"source": "custom", "title": "Picnic", "description": "Al parque",
                                                 "category": "diversion"})
        self.handler.create_or_update_proposal(self.couple_pk, self.user2,
                                                {"source": "custom", "title": "Cine", "description": "Ver película",
                                                 "category": "diversion"})
        self.handler.register_choice(self.couple_pk, self.user1, {"proposedBy": self.user1["sub"]})
        result, status = self.handler.register_choice(self.couple_pk, self.user2, {"proposedBy": self.user2["sub"]})
        self.assertEqual(status, 200)
        self.assertEqual(result["status"], "PENDING")
        self.assertTrue(result["disagreement"])
        self.assertIn("requestRandomTiebreak", result["availableActions"])

    def test_agreement_assigns_challenge(self):
        self.handler.create_or_update_proposal(self.couple_pk, self.user1,
                                                {"source": "custom", "title": "Picnic", "description": "Al parque",
                                                 "category": "diversion"})
        self.handler.register_choice(self.couple_pk, self.user1, {"proposedBy": self.user1["sub"]})
        result, status = self.handler.register_choice(self.couple_pk, self.user2, {"proposedBy": self.user1["sub"]})
        self.assertEqual(status, 200)
        self.assertEqual(result["status"], "ASSIGNED")
        self.assertEqual(result["selectedSource"], "agreement")
        self.assertEqual(result["selectedChallenge"]["title"], "Picnic")
        self.assertIn("completeChallenge", result["availableActions"])

    def test_double_tiebreak_assigns_random_challenge(self):
        self._add_catalog_item(title="Noche de juegos")
        self.handler.create_or_update_proposal(self.couple_pk, self.user1,
                                                {"source": "custom", "title": "Picnic", "description": "Al parque",
                                                 "category": "diversion"})
        self.handler.create_or_update_proposal(self.couple_pk, self.user2,
                                                {"source": "custom", "title": "Cine", "description": "Ver película",
                                                 "category": "diversion"})
        self.handler.register_choice(self.couple_pk, self.user1, {"requestRandomTiebreak": True})
        result, status = self.handler.register_choice(self.couple_pk, self.user2, {"requestRandomTiebreak": True})
        self.assertEqual(status, 200)
        self.assertEqual(result["status"], "ASSIGNED")
        self.assertEqual(result["selectedSource"], "agreement_random")

    # ── Ausencia de propuestas -> asignación automática por plazo vencido ─

    def test_no_proposals_triggers_auto_random_after_deadline(self):
        self._add_catalog_item(title="Caminata nocturna")
        with patch.object(self.handler, "_deadline_for", return_value="2000-01-01T00:00:00+00:00"):
            self.handler._get_or_create_month_record(self.couple_pk, self.handler.current_month_key())
        month_record = self.handler._load_current_month(self.couple_pk)
        self.assertEqual(month_record["status"], "ASSIGNED")
        self.assertEqual(month_record["selectedSource"], "auto_random")
        self.assertEqual(month_record["selectedChallenge"]["title"], "Caminata nocturna")

    def test_disagreement_resolved_by_deadline_auto_random(self):
        self._add_catalog_item(title="Reto de respaldo")
        with patch.object(self.handler, "_deadline_for", return_value="2000-01-01T00:00:00+00:00"):
            self.handler._get_or_create_month_record(self.couple_pk, self.handler.current_month_key())
        self.handler.create_or_update_proposal(self.couple_pk, self.user1,
                                                {"source": "custom", "title": "Picnic", "description": "Al parque",
                                                 "category": "diversion"})
        self.handler.create_or_update_proposal(self.couple_pk, self.user2,
                                                {"source": "custom", "title": "Cine", "description": "Ver película",
                                                 "category": "diversion"})
        month_record = self.handler._load_current_month(self.couple_pk)
        self.assertEqual(month_record["status"], "ASSIGNED")
        self.assertEqual(month_record["selectedSource"], "auto_random")

    # ── Selección aleatoria sin repetición innecesaria ────────────────────

    def test_random_selection_avoids_recent_repeat_when_alternative_exists(self):
        first = self._add_catalog_item(title="Reto A")
        second = self._add_catalog_item(title="Reto B")
        # Simula que "Reto A" se usó en un mes anterior
        self.table.items[(self.couple_pk, "MESESTADO#2026-01")] = {
            "PK": self.couple_pk, "SK": "MESESTADO#2026-01", "monthKey": "2026-01",
            "selectedChallenge": {"catalogChallengeId": first["id"], "title": first["title"]},
        }
        chosen = self.handler.pick_random_catalog_challenge(self.couple_pk)
        self.assertEqual(chosen["id"], second["id"])

    # ── Cambio de mes ──────────────────────────────────────────────────────

    def test_month_change_creates_independent_state(self):
        with patch.object(self.handler, "current_month_key", return_value="2026-01"):
            record_jan = self.handler._load_current_month(self.couple_pk)
            self.handler.create_or_update_proposal(self.couple_pk, self.user1,
                                                    {"source": "custom", "title": "Enero", "description": "Reto enero",
                                                     "category": "diversion"})
        with patch.object(self.handler, "current_month_key", return_value="2026-02"):
            record_feb = self.handler._load_current_month(self.couple_pk)
            state_feb = self.handler.build_month_state(self.couple_pk, record_feb, self.user1)
        self.assertNotEqual(record_jan["monthKey"], record_feb["monthKey"])
        self.assertEqual(state_feb["proposals"], [])

    # ── Edición y cancelación del reto activo ─────────────────────────────

    def _assign_by_agreement(self):
        self.handler.create_or_update_proposal(self.couple_pk, self.user1,
                                                {"source": "custom", "title": "Picnic", "description": "Al parque",
                                                 "category": "diversion"})
        self.handler.register_choice(self.couple_pk, self.user1, {"proposedBy": self.user1["sub"]})
        self.handler.register_choice(self.couple_pk, self.user2, {"proposedBy": self.user1["sub"]})
        return self.handler._load_current_month(self.couple_pk)

    def test_edit_active_challenge(self):
        self._assign_by_agreement()
        result, status = self.handler.edit_active_challenge(self.couple_pk, self.user2,
                                                              {"title": "Picnic al atardecer"})
        self.assertEqual(status, 200)
        self.assertEqual(result["selectedChallenge"]["title"], "Picnic al atardecer")
        # Verifica que exista al menos una entrada de auditoría de edición
        self.assertTrue(any(k[1].startswith("MESHISTORIAL#") for k in self.table.items if k[0] == self.couple_pk))

    def test_cancel_active_challenge_resets_to_pending_new_cycle(self):
        record = self._assign_by_agreement()
        self.assertEqual(record["cycle"], 1)
        result, status = self.handler.cancel_active_challenge_endpoint(self.couple_pk, self.user1,
                                                                        {"reason": "Ya no es viable"})
        self.assertEqual(status, 200)
        self.assertEqual(result["status"], "PENDING")
        self.assertEqual(result["cycle"], 2)
        self.assertIsNone(result["selectedChallenge"])

    def test_replace_active_challenge_does_not_create_two_active(self):
        self._assign_by_agreement()
        result, status = self.handler.replace_active_challenge(
            self.couple_pk, self.user1,
            {"source": "custom", "title": "Noche de cartas", "description": "Juegos de mesa", "category": "diversion"}
        )
        self.assertEqual(status, 200)
        self.assertEqual(result["status"], "PENDING")  # requiere acuerdo de la pareja, no se asigna solo
        self.assertEqual(len(result["proposals"]), 1)
        self.assertEqual(result["proposals"][0]["title"], "Noche de cartas")

    # ── Finalización concurrente y cupón único ────────────────────────────

    def test_concurrent_completion_creates_single_shared_coupon(self):
        self._assign_by_agreement()
        first_result, first_status = self.handler.complete_active_challenge(self.couple_pk, self.user1)
        self.assertEqual(first_status, 201)
        self.assertTrue(first_result["couponCreated"])

        stale_record = copy.deepcopy(self.table.items[(self.couple_pk, self.handler._month_sk(self.handler.current_month_key()))])
        stale_record["status"] = "ASSIGNED"  # simula lectura obsoleta de una solicitud concurrente
        with patch.object(self.handler, "_load_current_month", return_value=stale_record):
            second_result, second_status = self.handler.complete_active_challenge(self.couple_pk, self.user2)

        self.assertEqual(second_status, 200)
        self.assertFalse(second_result["couponCreated"])
        self.assertTrue(second_result["alreadyCompleted"])
        self.assertEqual(second_result["coupon"]["couponId"], first_result["coupon"]["couponId"])
        coupons = [item for key, item in self.table.items.items() if key[0] == self.couple_pk and key[1].startswith("CUPON#")]
        self.assertEqual(len(coupons), 1)

    def test_complete_requires_active_challenge(self):
        with self.assertRaises(self.handler.ChallengeError):
            self.handler.complete_active_challenge(self.couple_pk, self.user1)

    # ── Favoritos ──────────────────────────────────────────────────────────

    def test_favorite_catalog_challenge_and_list(self):
        item = self._add_catalog_item(title="Spa en casa")
        favorite, status = self.handler.add_favorite(self.couple_pk, self.user1,
                                                       {"source": "catalog", "catalogChallengeId": item["id"]})
        self.assertEqual(status, 201)
        listed, _ = self.handler.list_favorites(self.couple_pk)
        self.assertEqual(listed["count"], 1)
        with self.assertRaises(self.handler.ChallengeError):
            self.handler.add_favorite(self.couple_pk, self.user2,
                                       {"source": "catalog", "catalogChallengeId": item["id"]})
        removed, status = self.handler.remove_favorite(self.couple_pk, favorite["favoriteId"])
        self.assertEqual(status, 200)

    # ── Catálogo: contenido congelado aunque cambie después ───────────────

    def test_proposal_snapshot_survives_catalog_changes(self):
        item = self._add_catalog_item(title="Original", description="Desc original")
        self.handler.create_or_update_proposal(self.couple_pk, self.user1,
                                                {"source": "catalog", "catalogChallengeId": item["id"]})
        self.handler.update_catalog_challenge(item["id"], {"title": "Modificado"})
        month_record = self.handler._load_current_month(self.couple_pk)
        proposals = self.handler._list_proposals(self.couple_pk, month_record["monthKey"], month_record["cycle"])
        self.assertEqual(proposals[0]["title"], "Original")


if __name__ == "__main__":
    unittest.main()
