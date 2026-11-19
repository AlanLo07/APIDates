"""Weekly question API tests without AWS access."""
import copy
import importlib.util
import json
import os
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lambdas" / "layers" / "baselayer" / "python"))
HANDLER = ROOT / "lambdas" / "functions" / "WeeklyQuestionsCRUD" / "handler.py"
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")
DESERIALIZER = TypeDeserializer()


def aws_error(code):
    return ClientError({"Error": {"Code": code, "Message": "test"}}, "Test")


class MemoryDynamo:
    def __init__(self):
        self.items = {}
        self.before_transaction = None
        self.query_page_size = 100

    @staticmethod
    def key(item):
        return item["PK"], item["SK"]

    @staticmethod
    def matches(condition, item, names=None, values=None):
        if condition is None:
            return True
        if condition == "attribute_not_exists(PK)":
            return "PK" not in item
        names, values = names or {}, values or {}
        field, value = condition.split(" = ")
        return item.get(names.get(field, field)) == values[value]

    def get_item(self, Key, **kwargs):
        item = self.items.get(self.key(Key))
        return {"Item": copy.deepcopy(item)} if item is not None else {}

    def put_item(self, Item, ConditionExpression=None, ExpressionAttributeNames=None,
                 ExpressionAttributeValues=None):
        old = self.items.get(self.key(Item), {})
        if not self.matches(ConditionExpression, old, ExpressionAttributeNames, ExpressionAttributeValues):
            raise aws_error("ConditionalCheckFailedException")
        self.items[self.key(Item)] = copy.deepcopy(Item)
        return {}

    @staticmethod
    def query_matches(condition, item):
        expression = condition.get_expression()
        values = expression["values"]
        if expression["operator"] == "AND":
            return all(MemoryDynamo.query_matches(value, item) for value in values)
        field, value = values
        if expression["operator"] == "=":
            return item.get(field.name) == value
        if expression["operator"] == "begins_with":
            return item.get(field.name, "").startswith(value)
        raise AssertionError(expression)

    def query(self, KeyConditionExpression, ExclusiveStartKey=None, **kwargs):
        items = sorted(
            (copy.deepcopy(item) for item in self.items.values()
             if self.query_matches(KeyConditionExpression, item)
             and (ExclusiveStartKey is None or self.key(item) > self.key(ExclusiveStartKey))),
            key=self.key,
        )
        page = items[:self.query_page_size]
        result = {"Items": page}
        if len(items) > len(page):
            result["LastEvaluatedKey"] = {key: page[-1][key] for key in ("PK", "SK")}
        return result

    def transact_write_items(self, TransactItems):
        if self.before_transaction is not None:
            hook, self.before_transaction = self.before_transaction, None
            hook()
        writes = []
        reasons = []
        for action in TransactItems:
            operation, detail = next(iter(action.items()))
            decoded = {
                field: {key: DESERIALIZER.deserialize(value) for key, value in detail[field].items()}
                for field in ("Item", "Key", "ExpressionAttributeValues") if field in detail
            }
            key = self.key(decoded.get("Item", decoded.get("Key")))
            old = self.items.get(key, {})
            valid = self.matches(
                detail.get("ConditionExpression"), old,
                detail.get("ExpressionAttributeNames"), decoded.get("ExpressionAttributeValues"),
            )
            reasons.append({"Code": "None" if valid else "ConditionalCheckFailed"})
            if operation == "Put":
                writes.append((key, decoded["Item"]))
            elif operation != "ConditionCheck":
                raise AssertionError(operation)
        if any(reason["Code"] != "None" for reason in reasons):
            raise ClientError(
                {"Error": {"Code": "TransactionCanceledException", "Message": "test"},
                 "CancellationReasons": reasons}, "TransactWriteItems",
            )
        for key, item in writes:
            self.items[key] = copy.deepcopy(item)
        return {}


def load_handler():
    store = MemoryDynamo()
    resource = MagicMock()
    resource.Table.return_value = store
    cognito = MagicMock()
    cognito.list_users.return_value = {"Users": [
        {"Attributes": [{"Name": "sub", "Value": sub}, {"Name": "name", "Value": name}]}
        for sub, name in (("user-1", "Uno"), ("user-2", "Dos"))
    ]}
    with patch("boto3.resource", return_value=resource), patch(
        "boto3.client", side_effect=lambda service: store if service == "dynamodb" else cognito,
    ):
        spec = importlib.util.spec_from_file_location("weekly_questions_test_handler", HANDLER)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    module.USER_POOL_ID = "test-pool"
    return module, store


class WeeklyQuestionsTests(unittest.TestCase):
    def setUp(self):
        self.handler, self.store = load_handler()
        self.now = datetime(2026, 11, 19, 18, tzinfo=timezone.utc)
        clock = patch.object(self.handler, "_now", side_effect=lambda: self.now)
        clock.start()
        self.addCleanup(clock.stop)

    def request(self, method="GET", path="/weekly-questions", body=None, user="user-1",
                params=None, expected=200):
        event = {
            "requestContext": {"http": {"method": method},
                               "authorizer": {"jwt": {"claims": {"sub": user} if user else {}}}},
            "rawPath": path,
            "queryStringParameters": params,
            "body": json.dumps(body) if body is not None else None,
        }
        response = self.handler.lambda_handler(event, None)
        payload = json.loads(response["body"])
        self.assertEqual(response["statusCode"], expected, payload)
        return payload

    def create(self, text="Como nos vemos en cinco anos?", category="profundas", user="user-1"):
        return self.request("POST", "/weekly-questions/catalog",
                            {"text": text, "category": category}, user=user, expected=201)

    def current(self, user="user-1"):
        return self.request(path="/weekly-questions/current", user=user)

    def answer(self, question_id, text="Juntos", user="user-1", expected=201):
        return self.request("PUT", f"/weekly-questions/{question_id}/answer",
                            {"text": text}, user=user, expected=expected)

    def advance_week(self):
        self.now = datetime(2026, 11, 23, 6, tzinfo=timezone.utc)

    def test_calendar_changes_at_monday_midnight_mexico_including_year_boundary(self):
        before = self.handler._week(datetime(2026, 11, 23, 5, 59, 59, tzinfo=timezone.utc))
        after = self.handler._week(datetime(2026, 11, 23, 6, tzinfo=timezone.utc))
        self.assertEqual(before["weekKey"], "2026-11-16")
        self.assertEqual(after["weekKey"], "2026-11-23")
        self.assertEqual(after["weekStart"], "2026-11-23T00:00:00-06:00")
        self.assertEqual(after["weekEnd"], "2026-11-30T00:00:00-06:00")
        self.assertEqual(self.handler._week(datetime(2027, 1, 1, tzinfo=timezone.utc))["weekKey"],
                         "2026-12-28")

    def test_authentication_membership_and_exactly_two_users_are_required(self):
        self.request(user=None, expected=401)
        self.request(user="outsider", expected=403)
        users = self.handler.cognito.list_users.return_value["Users"]
        self.handler.cognito.list_users.return_value = {"Users": users[:1]}
        self.request(expected=503)
        self.handler.cognito.list_users.return_value = {"Users": users + [users[0]]}
        self.request(expected=503)

    def test_cognito_directory_pagination(self):
        users = self.handler.cognito.list_users.return_value["Users"]
        self.handler.cognito.list_users.side_effect = [
            {"Users": users[:1], "PaginationToken": "next"}, {"Users": users[1:]},
        ]
        self.request()
        self.assertEqual(self.handler.cognito.list_users.call_args.kwargs["PaginationToken"], "next")

    def test_both_users_can_create_and_edit_catalog_but_not_delete(self):
        question = self.create(user="user-2")
        changed = self.request("PUT", f"/weekly-questions/catalog/{question['id']}", {
            "text": "Que animal quisieras ser?", "category": "absurdas", "version": 1,
        })
        self.assertEqual(changed["createdBy"], "user-2")
        self.assertEqual(changed["updatedBy"], "user-1")
        self.assertEqual(changed["version"], 2)
        self.request("PUT", f"/weekly-questions/catalog/{question['id']}", {
            "text": "Otra", "category": "otras", "version": 1,
        }, expected=409)
        self.request("DELETE", f"/weekly-questions/catalog/{question['id']}", expected=405)
        self.assertEqual(self.request(path=f"/weekly-questions/catalog/{question['id']}"), changed)

    def test_catalog_validation_rejects_empty_oversized_and_forbidden_fields(self):
        for body in (
            [], {"text": " ", "category": "otras"}, {"text": "a" * 1001, "category": "otras"},
            {"text": "Pregunta", "category": "a" * 61},
            {"text": "Pregunta", "category": "otras", "active": False},
            {"text": "Pregunta", "category": "otras", "userId": "user-2"},
        ):
            with self.subTest(body=body):
                self.request("POST", "/weekly-questions/catalog", body, expected=400)
        question = self.create()
        self.request("PUT", f"/weekly-questions/catalog/{question['id']}",
                     {"text": "Otra", "category": "otras", "version": True}, expected=400)

    def test_empty_catalog_and_exhaustion_allow_new_questions_in_same_week(self):
        self.assertIsNone(self.current()["question"])
        question = self.create()
        self.assertEqual(self.current()["question"]["id"], question["id"])
        self.answer(question["id"])
        self.answer(question["id"], user="user-2")
        self.assertEqual(self.current()["question"]["status"], "completed")
        self.advance_week()
        empty = self.current()
        self.assertIsNone(empty["question"])
        self.assertEqual(empty["message"], "No hay preguntas disponibles")
        fresh = self.create("Cual es tu recuerdo favorito?", "romanticas")
        self.assertEqual(self.current()["question"]["id"], fresh["id"])

    def test_exact_content_limits_are_accepted(self):
        question = self.create("a" * 1000, "b" * 60)
        self.current()
        answer = self.answer(question["id"], "c" * 5000)
        self.assertEqual(len(answer["answers"]["user-1"]["text"]), 5000)

    def test_selection_is_shared_stable_and_inactive_weeks_are_not_backfilled(self):
        question = self.create()
        first = self.current()
        self.create("Que prefieres?", "divertidas")
        self.assertEqual(self.current(user="user-2")["question"]["id"], question["id"])
        self.assertEqual(self.current()["weekKey"], first["weekKey"])
        self.now = datetime(2026, 12, 21, 12, tzinfo=timezone.utc)
        self.current()
        weeks = [item for item in self.store.items.values() if item["SK"].startswith("WEEK#")]
        self.assertEqual(len(weeks), 2)
        self.assertEqual({item["weekKey"] for item in weeks}, {"2026-11-16", "2026-12-21"})

    def test_concurrent_first_reads_return_winning_selection_without_orphan_history(self):
        first, second = self.create(), self.create("Que animal serias?", "absurdas")
        competitor = []

        def race():
            with patch.object(self.handler.random, "choice", return_value=self.handler._catalog_item(second["id"])):
                competitor.append(self.current(user="user-2"))

        self.store.before_transaction = race
        with patch.object(self.handler.random, "choice", return_value=self.handler._catalog_item(first["id"])):
            winner = self.current()
        self.assertEqual(winner["question"]["id"], second["id"])
        self.assertEqual(winner["question"]["id"], competitor[0]["question"]["id"])
        histories = [item for item in self.store.items.values() if item["SK"].startswith("HISTORY#")]
        self.assertEqual([item["id"] for item in histories], [second["id"]])

    def test_answers_are_immediately_visible_and_concurrent_writes_preserve_both(self):
        question = self.create()
        self.current()
        self.store.before_transaction = lambda: self.answer(question["id"], "Contigo", user="user-2")
        result = self.answer(question["id"], "En casa")
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["answers"]["user-1"]["text"], "En casa")
        self.assertEqual(result["answers"]["user-2"]["text"], "Contigo")
        self.assertFalse(result["canAnswer"])
        self.assertTrue(result["canEdit"])

    def test_partner_sees_first_answer_before_answering(self):
        question = self.create()
        self.current()
        self.answer(question["id"], "Una aventura")
        result = self.current(user="user-2")["question"]
        self.assertEqual(result["answers"]["user-1"]["text"], "Una aventura")
        self.assertEqual(result["status"], "partial")
        self.assertTrue(result["canAnswer"])
        self.assertFalse(result["canEdit"])

    def test_closed_week_blocks_new_answers_but_allows_editing_and_later_reselection(self):
        first = self.create()
        self.current()
        original = self.answer(first["id"])
        second = self.create("Tu comida favorita?", "divertidas")
        self.advance_week()
        with patch.object(self.handler.random, "choice", return_value=self.handler._catalog_item(second["id"])):
            self.current()
        self.answer(first["id"], user="user-2", expected=403)
        edited = self.answer(first["id"], "Siempre juntos", expected=200)
        self.assertEqual(edited["answers"]["user-1"]["createdAt"],
                         original["answers"]["user-1"]["createdAt"])
        self.assertFalse(edited["isCurrent"])
        self.assertTrue(edited["canEdit"])
        self.assertFalse(edited["locked"])
        self.now = datetime(2026, 11, 30, 6, tzinfo=timezone.utc)
        with patch.object(self.handler.random, "choice", return_value=self.handler._catalog_item(first["id"])):
            repeated = self.current()["question"]
        self.assertEqual(repeated["answers"]["user-1"]["text"], "Siempre juntos")
        self.assertTrue(self.answer(first["id"], user="user-2")["status"] == "completed")

    def test_unseen_questions_and_impersonation_cannot_be_answered(self):
        question = self.create()
        self.answer(question["id"], expected=403)
        self.current()
        self.request("PUT", f"/weekly-questions/{question['id']}/answer",
                     {"text": "Respuesta", "userId": "user-2"}, expected=400)
        for text in (" ", "a" * 5001, 123):
            self.answer(question["id"], text, expected=400)

    def test_week_boundary_is_rechecked_before_creating_an_answer(self):
        question = self.create()
        self.current()
        before = datetime(2026, 11, 23, 5, 59, 59, tzinfo=timezone.utc)
        after = datetime(2026, 11, 23, 6, tzinfo=timezone.utc)
        with patch.object(self.handler, "_now", side_effect=[before, after, after]):
            self.answer(question["id"], expected=403)
        view = self.request(path=f"/weekly-questions/{question['id']}")
        self.assertEqual(view["answers"], {})

    def test_catalog_edits_do_not_rewrite_presented_text_even_when_reselected(self):
        question = self.create()
        presented = self.current()["question"]
        self.request("PUT", f"/weekly-questions/catalog/{question['id']}", {
            "text": "Nueva pregunta", "category": "otra categoria", "version": 1,
        })
        catalog = self.request(path=f"/weekly-questions/catalog/{question['id']}")
        self.assertEqual(catalog["text"], "Nueva pregunta")
        self.assertEqual(self.current()["question"]["text"], presented["text"])
        self.advance_week()
        repeated = self.current()["question"]
        self.assertEqual(repeated["text"], presented["text"])
        self.assertEqual(repeated["catalogVersion"], 1)

    def test_lists_paginate_all_dynamo_pages_and_filter_progress(self):
        first = self.create()
        self.current()
        self.answer(first["id"])
        second = self.create("Pregunta divertida", "divertidas")
        self.store.query_page_size = 1
        self.assertEqual(len(self.request()["items"]), 2)
        page = self.request(params={"limit": "1"})
        rest = self.request(params={"limit": "1", "nextToken": page["nextToken"]})
        self.assertIsNone(rest["nextToken"])
        self.assertEqual({page["items"][0]["id"], rest["items"][0]["id"]},
                         {first["id"], second["id"]})
        pending = self.request(params={"pendingFor": "me"}, user="user-2")
        self.assertEqual([item["id"] for item in pending["items"]], [first["id"]])
        self.assertEqual(self.request(params={"pendingFor": "me"})["items"], [])
        self.assertEqual(self.request(params={"pendingFor": "partner"})["items"][0]["id"], first["id"])
        unseen = self.request(params={"status": "unseen"})["items"]
        self.assertEqual([item["id"] for item in unseen], [second["id"]])
        self.assertTrue(unseen[0]["locked"])
        self.assertFalse(unseen[0]["canAnswer"])
        self.assertEqual(self.request(params={"status": "partial"})["items"][0]["id"], first["id"])
        catalog = self.request(path="/weekly-questions/catalog", params={"category": "divertidas"})
        self.assertEqual([item["id"] for item in catalog["items"]], [second["id"]])
        for params in ({"limit": "0"}, {"limit": "101"}, {"limit": "no"},
                       {"nextToken": "invalid"}, {"status": "other"}, {"pendingFor": "other"}):
            self.request(params=params, expected=400)

    def test_missing_question_and_malformed_routes_return_explicit_errors(self):
        missing = "00000000-0000-0000-0000-000000000001"
        self.request(path=f"/weekly-questions/{missing}", expected=404)
        self.request(path="/weekly-questions/not-a-uuid", expected=400)
        self.request(path="/weekly-questions/catalog/extra/path", expected=404)
        self.request(path=f"/weekly-questions/{missing}/extra/path", expected=404)

    def test_database_errors_are_not_treated_as_empty_catalog_or_conflicts(self):
        with patch.object(self.store, "query", side_effect=aws_error("ProvisionedThroughputExceededException")):
            self.request(path="/weekly-questions/current", expected=503)
        self.create()
        error = ClientError({
            "Error": {"Code": "TransactionCanceledException", "Message": "test"},
            "CancellationReasons": [{"Code": "ValidationError"}],
        }, "TransactWriteItems")
        with patch.object(self.store, "transact_write_items", side_effect=error):
            self.request(path="/weekly-questions/current", expected=503)

    def test_repeated_conflicts_return_409_without_partial_writes(self):
        self.create()
        with patch.object(self.store, "transact_write_items",
                          side_effect=aws_error("ConditionalCheckFailedException")) as transaction:
            self.request(path="/weekly-questions/current", expected=409)
        self.assertEqual(transaction.call_count, 4)
        self.assertFalse(any(item["SK"].startswith(("WEEK#", "HISTORY#"))
                             for item in self.store.items.values()))


if __name__ == "__main__":
    unittest.main()
