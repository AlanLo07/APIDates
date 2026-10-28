import copy
import importlib.util
import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lambdas" / "layers" / "baselayer" / "python"))
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")

from common.checklists import build_checklist_records


def load_handler(folder):
    path = ROOT / "lambdas" / "functions" / folder / "handler.py"
    with patch("boto3.resource"), patch("boto3.client"):
        spec = importlib.util.spec_from_file_location(f"{folder}_checklist_test", path)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module


class CitasChecklistTests(unittest.TestCase):
    def setUp(self):
        self.handler = load_handler("CitasCRUD")
        self.cita = {
            "id": "cita-1", "type": "evento", "title": "Viaje",
            "description": "Fin de semana", "date": "31-10-2026",
        }
        self.checklist = {
            "id": "lista-1", "titulo": "Equipaje", "kind": "personalizado",
            "items": [{"id": "item-1", "nombre": "Pasaporte", "comprado": True}],
        }

    def actions(self):
        return self.handler.dynamodb_client.transact_write_items.call_args.kwargs["TransactItems"]

    def decode(self, value):
        deserializer = TypeDeserializer()
        return {key: deserializer.deserialize(item) for key, item in value.items()}

    def test_create_without_checklist(self):
        response = self.handler.create_item(self.cita)
        self.assertEqual(response["statusCode"], 201)
        self.assertIsNone(json.loads(response["body"])["checklistId"])
        item = self.decode(self.actions()[-1]["Put"]["Item"])
        self.assertIsNone(item["checklistId"])
        self.assertNotIn("checklist", item)

    def test_link_checks_existence_in_same_transaction(self):
        response = self.handler.create_item({**self.cita, "checklistId": " lista-1 "})
        self.assertEqual(response["statusCode"], 201)
        actions = self.actions()
        self.assertEqual(len(actions), 2)
        self.assertEqual(self.decode(actions[0]["ConditionCheck"]["Key"]),
                         {"pk": "CHECKLIST#lista-1", "sk": "META"})
        self.assertEqual(self.decode(actions[-1]["Put"]["Item"])["checklistId"], "lista-1")

    def test_new_checklist_and_cita_are_one_transaction(self):
        response = self.handler.create_item({**self.cita, "checklist": self.checklist})
        self.assertEqual(response["statusCode"], 201)
        self.handler.dynamodb_client.transact_write_items.assert_called_once()
        self.handler.table.put_item.assert_not_called()
        records = [self.decode(action["Put"]["Item"]) for action in self.actions()]
        self.assertEqual([record.get("sk") for record in records], ["META", "ITEM#item-1", None])
        self.assertEqual(records[0]["entityType"], "checklist_board")
        self.assertTrue(records[1]["comprado"])
        self.assertEqual(records[-1]["checklistId"], "lista-1")
        self.assertNotIn("checklist", records[-1])

    def test_invalid_requests_do_not_write(self):
        invalid = [
            {"checklistId": "lista-1", "checklist": self.checklist},
            {"checklistId": " "}, {"checklistId": 123},
            {"checklist": None}, {"checklist": {"items": []}},
            {"checklist": {"titulo": "Lista", "items": [{"nombre": "Pasaporte", "incluido": True}]}},
            {"checklist": {"titulo": "Lista", "items": [{"nombre": "Pasaporte", "comprado": "si"}]}},
        ]
        for payload in invalid:
            with self.subTest(payload=payload):
                response = self.handler.create_item({**self.cita, **payload})
                self.assertEqual(response["statusCode"], 400)
        self.handler.dynamodb_client.transact_write_items.assert_not_called()

    def test_other_cita_types_cannot_link_checklist(self):
        response = self.handler.create_item({**self.cita, "type": "carta", "checklistId": "lista-1"})
        self.assertEqual(response["statusCode"], 400)
        self.handler.dynamodb_client.transact_write_items.assert_not_called()

    def test_nonexistent_checklist_returns_404(self):
        self.handler.dynamodb_client.transact_write_items.side_effect = ClientError({
            "Error": {"Code": "TransactionCanceledException"},
            "CancellationReasons": [{"Code": "ConditionalCheckFailed"}, {"Code": "None"}],
        }, "TransactWriteItems")
        response = self.handler.create_item({**self.cita, "checklistId": "missing"})
        self.assertEqual(response["statusCode"], 404)
        self.handler.table.put_item.assert_not_called()

    def test_duplicate_checklist_returns_409(self):
        self.handler.dynamodb_client.transact_write_items.side_effect = ClientError({
            "Error": {"Code": "TransactionCanceledException"},
            "CancellationReasons": [{"Code": "ConditionalCheckFailed"}, {"Code": "None"}, {"Code": "None"}],
        }, "TransactWriteItems")
        response = self.handler.create_item({**self.cita, "checklist": self.checklist})
        self.assertEqual(response["statusCode"], 409)

    def test_update_omitting_reference_preserves_it(self):
        self.handler.table.get_item.return_value = {"Item": {**self.cita, "checklistId": "lista-1"}}
        response = self.handler.update_item("cita-1", {"title": "Otro viaje"})
        self.assertEqual(response["statusCode"], 200)
        update = self.actions()[-1]["Update"]
        values = self.decode(update["ExpressionAttributeValues"])
        self.assertEqual(values[":v_checklistId"], "lista-1")
        self.assertNotIn("REMOVE", update["UpdateExpression"])

    def test_unlink_removes_legacy_without_deleting_shared_board(self):
        self.handler.table.get_item.return_value = {"Item": {
            **self.cita, "checklistId": "lista-1", "checklist": {"items": []},
        }}
        response = self.handler.update_item("cita-1", {"checklistId": None})
        self.assertEqual(response["statusCode"], 200)
        self.assertEqual(len(self.actions()), 1)
        update = self.actions()[0]["Update"]
        self.assertIsNone(self.decode(update["ExpressionAttributeValues"])[":v_checklistId"])
        self.assertIn("REMOVE #legacy_checklist", update["UpdateExpression"])
        self.handler.table.delete_item.assert_not_called()

    def test_update_can_replace_reference_with_new_checklist(self):
        self.handler.table.get_item.return_value = {"Item": {**self.cita, "checklistId": "old"}}
        response = self.handler.update_item("cita-1", {"checklist": self.checklist})
        self.assertEqual(response["statusCode"], 200)
        actions = self.actions()
        self.assertEqual(len(actions), 3)
        self.assertNotIn("ConditionCheck", actions[0])
        self.assertEqual(self.decode(actions[-1]["Update"]["ExpressionAttributeValues"])[":v_checklistId"], "lista-1")

    def test_legacy_is_not_implicitly_converted_or_removed(self):
        self.handler.table.get_item.return_value = {"Item": {
            **self.cita, "checklist": {"items": [{"nombre": "Pasaporte", "incluido": True}]},
        }}
        response = self.handler.update_item("cita-1", {"title": "Viaje editado"})
        self.assertEqual(response["statusCode"], 200)
        self.assertEqual(len(self.actions()), 1)
        self.assertNotIn("REMOVE", self.actions()[0]["Update"]["UpdateExpression"])

    def test_delete_cita_does_not_delete_checklist(self):
        self.handler.table.get_item.return_value = {"Item": {**self.cita, "checklistId": "lista-1"}}
        response = self.handler.delete_item("cita-1")
        self.assertEqual(response["statusCode"], 200)
        self.handler.table.delete_item.assert_called_once_with(Key={"id": "cita-1"})
        self.handler.dynamodb_client.transact_write_items.assert_not_called()

    def test_bulk_create_returns_ids_and_preserves_partial_errors(self):
        event = {
            "requestContext": {"http": {"method": "POST"}}, "rawPath": "/citas",
            "body": json.dumps([self.cita, {**self.cita, "checklistId": ""}]),
        }
        response = self.handler.lambda_handler(event, None)
        self.assertEqual(response["statusCode"], 207)
        body = json.loads(response["body"])
        self.assertEqual(body["created"], ["cita-1"])
        self.assertEqual(body["errors"][0]["index"], 1)


class ChecklistCreationTests(unittest.TestCase):
    def test_post_checklists_creates_board_groups_and_items_atomically(self):
        handler = load_handler("ChecklistsCRUD")
        response = handler.create_checklist({
            "id": "lista-1", "titulo": "Viaje", "usaGrupos": True,
            "grupos": [{"id": "documentos", "nombre": "Documentos"}],
            "items": [{"id": "pasaporte", "nombre": "Pasaporte", "groupId": "documentos"}],
        })
        self.assertEqual(response["statusCode"], 201)
        self.assertEqual(json.loads(response["body"])["items"], ["pasaporte"])
        handler.dynamodb_client.transact_write_items.assert_called_once()
        handler.table.put_item.assert_not_called()

    def test_defaults_are_preserved(self):
        records = build_checklist_records({"titulo": "Viaje", "kind": "viaje"})
        self.assertTrue(records[0]["usaGrupos"])
        self.assertEqual(len(records), 8)

    def test_all_children_are_validated_before_writes(self):
        handler = load_handler("ChecklistsCRUD")
        with self.assertRaises(ValueError):
            handler.create_checklist({"titulo": "Lista", "grupos": [{"nombre": "Valido"}, {}]})
        handler.dynamodb_client.transact_write_items.assert_not_called()
        handler.table.put_item.assert_not_called()

    def test_duplicate_ids_invalid_groups_and_limits(self):
        payloads = [
            {"grupos": [{"id": "same", "nombre": "A"}, {"id": "same", "nombre": "B"}]},
            {"items": [{"nombre": "A", "groupId": "missing"}]},
            {"items": [{"nombre": "A", "prioridadOrden": 1.5}]},
            {"items": [{"nombre": "A", "precio": "NaN"}]},
            {"items": [{"nombre": str(index)} for index in range(99)]},
        ]
        for payload in payloads:
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                build_checklist_records({"titulo": "Lista", **payload})

    def test_input_is_not_mutated(self):
        data = {"titulo": "Lista", "items": [{"nombre": "A"}]}
        original = copy.deepcopy(data)
        build_checklist_records(data)
        self.assertEqual(data, original)

    def test_seed_skips_existing_checklists_after_transaction_change(self):
        handler = load_handler("ChecklistsCRUD")
        handler.dynamodb_client.transact_write_items.side_effect = ClientError({
            "Error": {"Code": "TransactionCanceledException"},
            "CancellationReasons": [{"Code": "ConditionalCheckFailed"}],
        }, "TransactWriteItems")
        response = handler.seed_default_checklists()
        self.assertEqual(response["statusCode"], 200)
        self.assertEqual(len(json.loads(response["body"])["skipped"]), 3)


if __name__ == "__main__":
    unittest.main()