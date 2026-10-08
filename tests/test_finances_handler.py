import importlib.util
import json
import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch


ROOT = Path(__file__).resolve().parents[1]
COMMON_LAYER = ROOT / "lambdas" / "layers" / "baselayer" / "python"
HANDLER_PATH = ROOT / "lambdas" / "functions" / "FinancesCRUD" / "handler.py"
sys.path.insert(0, str(COMMON_LAYER))
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")


def load_handler():
    resource = MagicMock()
    resource.Table.return_value = MagicMock()
    with patch("boto3.resource", return_value=resource):
        spec = importlib.util.spec_from_file_location("finances_handler_test", HANDLER_PATH)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module, resource


def event(path="/finances", method="GET", sub="user-1", email="uno@example.com", body=None):
    claims = {"sub": sub, "email": email, "name": "Usuario Uno"}
    return {
        "rawPath": path,
        "body": json.dumps(body or {}),
        "requestContext": {
            "http": {"method": method},
            "authorizer": {"jwt": {"claims": claims}},
        },
    }


def lambda_context():
    return SimpleNamespace(function_name="finances-crud-test")


class FinancesHandlerTests(unittest.TestCase):
    def setUp(self):
        self.handler, self.resource = load_handler()
        self.table = self.handler.table

    def test_request_without_jwt_is_rejected(self):
        response = self.handler.lambda_handler(
            {"rawPath": "/finances", "requestContext": {"http": {"method": "GET"}}},
            lambda_context(),
        )

        self.assertEqual(response["statusCode"], 401)
        self.assertEqual(json.loads(response["body"])["error"], "Usuario no autenticado")

    def test_user_from_another_couple_is_rejected(self):
        self.table.get_item.side_effect = [{}, {}, {}]

        response = self.handler.lambda_handler(event(), lambda_context())

        self.assertEqual(response["statusCode"], 403)
        self.assertEqual(self.table.get_item.call_count, 3)

    def test_email_membership_is_promoted_to_user_membership(self):
        self.table.get_item.side_effect = [{}, {"Item": {"coupleId": "couple-1"}}]

        couple_pk, user = self.handler._resolve_couple_context(event())

        self.assertEqual(couple_pk, "PAREJA#couple-1")
        self.assertEqual(user["sub"], "user-1")
        promoted_item = self.table.put_item.call_args.kwargs["Item"]
        self.assertEqual(promoted_item["PK"], "USUARIO#user-1")
        self.assertEqual(promoted_item["coupleId"], "couple-1")

    def test_init_is_atomic_and_uses_cop(self):
        user = {"sub": "user-1", "email": "uno@example.com", "name": "Uno"}

        result, status = self.handler.init_couple(
            user,
            "Uno",
            "Dos",
            "uno@example.com",
            "dos@example.com",
        )

        self.assertEqual(status, 201)
        self.assertEqual(result["currency"], "COP")
        transaction = self.resource.meta.client.transact_write_items.call_args.kwargs
        self.assertEqual(len(transaction["TransactItems"]), 4)
        couple_item = transaction["TransactItems"][0]["Put"]["Item"]
        self.assertEqual(couple_item["currency"]["S"], "COP")

    def test_created_by_body_value_is_ignored(self):
        self.table.get_item.side_effect = [
            {"Item": {"coupleId": "couple-1"}},
        ]
        created = {"gastoId": "expense-1"}
        request = event(
            path="/finances/gastos",
            method="POST",
            body={
                "title": "Cena",
                "amount": 10,
                "date": "2026-10-08T12:00:00+00:00",
                "category": "dateNights",
                "createdBy": "forged@example.com",
            },
        )

        with patch.object(
            self.handler,
            "create_expense",
            return_value=(created, 201),
        ) as create_expense:
            response = self.handler.lambda_handler(request, lambda_context())

        self.assertEqual(response["statusCode"], 201)
        args = create_expense.call_args.args
        self.assertEqual(args[-2:], ("user-1", "uno@example.com"))

    def test_update_rejects_immutable_fields(self):
        result, status = self.handler.update_expense(
            "PAREJA#couple-1",
            "expense-1",
            {"createdBy": "attacker"},
        )

        self.assertEqual(status, 400)
        self.assertIn("createdBy", result["error"])
        self.table.update_item.assert_not_called()

    def test_moving_expense_recalculates_old_and_new_months(self):
        current_expense = {
            "title": "Cena",
            "amount": self.handler.Decimal("10"),
            "date": "2026-09-30T20:00:00+00:00",
            "category": "dateNights",
            "monthYear": "2026-09",
        }
        self.table.update_item.return_value = {
            "Attributes": {"monthYear": "2026-10"}
        }

        with (
            patch.object(self.handler, "get_expense", return_value=(current_expense, 200)),
            patch.object(self.handler, "persist_monthly_stats") as persist_monthly_stats,
        ):
            _, status = self.handler.update_expense(
                "PAREJA#couple-1",
                "expense-1",
                {"date": "2026-10-01T01:00:00+00:00"},
            )

        self.assertEqual(status, 200)
        persist_monthly_stats.assert_has_calls(
            [
                call("PAREJA#couple-1", "2026-09"),
                call("PAREJA#couple-1", "2026-10"),
            ],
            any_order=True,
        )

    def test_budget_month_requires_yyyy_mm(self):
        errors = self.handler.validate_budget({"monthYear": "2026-13", "amount": 10})
        self.assertIn("monthYear debe tener formato YYYY-MM", errors)


if __name__ == "__main__":
    unittest.main()
