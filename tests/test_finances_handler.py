import importlib.util
import json
import os
import sys
import unittest
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

from botocore.exceptions import ClientError


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
        self.table.get_item.return_value = {}
        self.table.query.return_value = {"Items": []}

    def test_request_without_jwt_is_rejected(self):
        response = self.handler.lambda_handler(
            {"rawPath": "/finances", "requestContext": {"http": {"method": "GET"}}},
            lambda_context(),
        )

        self.assertEqual(response["statusCode"], 401)
        self.assertEqual(json.loads(response["body"])["error"], "Usuario no autenticado")

    def test_missing_goal_idempotency_key_is_logged_without_sensitive_data(self):
        request = event(path="/finances/metas", method="POST",
                        body={"name": "Ir a Argentina", "targetAmount": 50000})
        request["headers"] = {"Authorization": "Bearer secret-token"}
        request["requestContext"]["requestId"] = "api-request-1"
        user = {"sub": "user-1", "email": "uno@example.com"}
        with (
            patch.object(self.handler, "_resolve_couple_context", return_value=("PAREJA#couple-1", user)),
            patch.object(self.handler, "logger") as logger,
        ):
            response = self.handler.lambda_handler(request, lambda_context())

        self.assertEqual(response["statusCode"], 400)
        logs = [json.loads(entry.args[1]) for entry in logger.log.call_args_list]
        rejection = next(item for item in logs if item["message"] == "Operacion de ahorros rechazada")
        self.assertIn("Idempotency-Key", rejection["reason"])
        self.assertEqual(rejection["api_request_id"], "api-request-1")
        self.assertTrue(logs[0]["authorization_present"])
        self.assertFalse(logs[0]["idempotency_key_present"])
        for sensitive_value in ("secret-token", "Ir a Argentina", "50000", "uno@example.com"):
            self.assertNotIn(sensitive_value, str(logger.mock_calls))

    def test_goal_aws_error_logs_only_safe_service_metadata(self):
        request = event(path="/finances/metas", method="POST",
                        body={"name": "Ir a Argentina", "targetAmount": 50000})
        request["headers"] = {"Idempotency-Key": "private-request-key"}
        error = ClientError({
            "Error": {"Code": "AccessDeniedException", "Message": "sensitive-service-detail"},
            "ResponseMetadata": {"RequestId": "aws-request-1"},
        }, "TransactWriteItems")
        with (
            patch.object(self.handler, "_resolve_couple_context", return_value=("PAREJA#couple-1", {})),
            patch.object(self.handler, "create_savings_goal", side_effect=error),
            patch.object(self.handler, "logger") as logger,
        ):
            response = self.handler.lambda_handler(request, lambda_context())

        self.assertEqual(response["statusCode"], 500)
        failure = json.loads(logger.log.call_args.args[1])
        self.assertEqual(failure["level"], "🔴")
        self.assertEqual(failure["error_code"], "AccessDeniedException")
        self.assertEqual(failure["operation"], "TransactWriteItems")
        self.assertEqual(failure["aws_request_id"], "aws-request-1")
        self.assertEqual(failure["stage"], "savings_route")
        self.assertNotIn("sensitive-service-detail", str(logger.mock_calls))
        self.assertNotIn("private-request-key", str(logger.mock_calls))

    def test_successful_goal_response_is_logged(self):
        request = event(path="/finances/metas", method="POST",
                        body={"name": "Ir a Argentina", "targetAmount": 50000})
        request["headers"] = {"Idempotency-Key": "request-key"}
        with (
            patch.object(self.handler, "_resolve_couple_context", return_value=("PAREJA#couple-1", {})),
            patch.object(self.handler, "create_savings_goal", return_value=({"goalId": "goal-1"}, 201)),
            patch.object(self.handler, "logger") as logger,
        ):
            response = self.handler.lambda_handler(request, lambda_context())

        self.assertEqual(response["statusCode"], 201)
        completion = json.loads(logger.log.call_args.args[1])
        self.assertEqual(completion["message"], "Respuesta de creacion de meta preparada")
        self.assertEqual(completion["status_code"], 201)

    def test_user_from_another_couple_is_rejected(self):
        self.table.get_item.side_effect = [{}, {}, {}]

        response = self.handler.lambda_handler(event(), lambda_context())

        self.assertEqual(response["statusCode"], 403)
        self.assertEqual(self.table.get_item.call_count, 2)

    def test_email_membership_is_promoted_to_user_membership(self):
        self.table.get_item.side_effect = [{}, {"Item": {"coupleId": "couple-1"}}]

        couple_pk, user = self.handler._resolve_couple_context(event())

        self.assertEqual(couple_pk, "PAREJA#couple-1")
        self.assertEqual(user["sub"], "user-1")
        promoted_item = self.table.put_item.call_args.kwargs["Item"]
        self.assertEqual(promoted_item["PK"], "USUARIO#user-1")
        self.assertEqual(promoted_item["coupleId"], "couple-1")

    def test_access_token_without_email_uses_user_membership(self):
        self.table.get_item.return_value = {
            "Item": {"coupleId": "couple-1"}
        }
        request = event()
        del request["requestContext"]["authorizer"]["jwt"]["claims"]["email"]

        couple_pk, user = self.handler._resolve_couple_context(request)

        self.assertEqual(couple_pk, "PAREJA#couple-1")
        self.assertEqual(user["sub"], "user-1")
        self.assertEqual(user["email"], "")

    def test_legacy_email_membership_is_found_without_email_claim(self):
        self.table.get_item.side_effect = [{}, {"Item": {"coupleId": "couple-legacy"}}]
        request = event()
        del request["requestContext"]["authorizer"]["jwt"]["claims"]["email"]
        request["headers"] = {"Authorization": "Bearer access-token"}

        with patch.object(self.handler, "_email_from_access_token", return_value="uno@example.com"):
            couple_pk, user = self.handler._resolve_couple_context(request)

        self.assertEqual(couple_pk, "PAREJA#couple-legacy")
        self.assertEqual(user["sub"], "user-1")
        promoted_item = self.table.put_item.call_args.kwargs["Item"]
        self.assertEqual(promoted_item["PK"], "USUARIO#user-1")
        self.assertEqual(promoted_item["coupleId"], "couple-legacy")

    def test_legacy_default_expense_is_found_without_meta(self):
        self.table.query.return_value = {
            "Items": [{
                "PK": "PAREJA#DEFAULT",
                "SK": "GASTO#legacy-expense",
                "createdBy": "uno@example.com",
            }]
        }
        self.table.get_item.side_effect = [{}, {}]

        couple_pk, _user = self.handler._resolve_couple_context(event())

        self.assertEqual(couple_pk, "PAREJA#DEFAULT")
        self.assertEqual(
            self.table.put_item.call_args.kwargs["Item"]["coupleId"], "DEFAULT"
        )

    def test_init_adopts_default_partition_when_email_owns_legacy_expense(self):
        self.table.query.return_value = {
            "Items": [{
                "PK": "PAREJA#DEFAULT",
                "SK": "GASTO#legacy-expense",
                "createdBy": "uno@example.com",
            }]
        }
        self.table.get_item.return_value = {}
        user = {"sub": "user-1", "email": "uno@example.com", "name": "Uno"}

        result, status = self.handler.init_couple(
            user, "Uno", "Dos", "uno@example.com", "dos@example.com"
        )

        self.assertEqual(status, 201)
        self.assertEqual(result["coupleId"], "DEFAULT")
        actions = self.resource.meta.client.transact_write_items.call_args.kwargs["TransactItems"]
        couple_item = actions[0]["Put"]["Item"]
        self.assertEqual(couple_item["PK"]["S"], "PAREJA#DEFAULT")

    def test_configured_couple_adopts_default_rows_with_unknown_or_missing_owner(self):
        self.table.query.return_value = {
            "Items": [
                {"PK": "PAREJA#DEFAULT", "SK": "GASTO#unknown", "createdBy": "unknown"},
                {"PK": "PAREJA#DEFAULT", "SK": "GASTO#null", "createdBy": None},
            ]
        }
        user = {"sub": "alan-sub", "email": "alan.lopezg9707@gmail.com", "name": "Alan"}

        with patch.object(
            self.handler,
            "LEGACY_FINANCES_EMAILS",
            frozenset({"alan.lopezg9707@gmail.com", "mariana.delgado6@icloud.com"}),
        ):
            result, status = self.handler.init_couple(
                user,
                "Alan",
                "Mariana",
                "alan.lopezg9707@gmail.com",
                "mariana.delgado6@icloud.com",
            )

        self.assertEqual(status, 201)
        self.assertEqual(result["coupleId"], "DEFAULT")
        actions = self.resource.meta.client.transact_write_items.call_args.kwargs["TransactItems"]
        self.assertEqual(actions[0]["Put"]["Item"]["PK"]["S"], "PAREJA#DEFAULT")

    def test_configured_users_resolve_default_before_old_hashed_membership(self):
        self.table.get_item.side_effect = [
            {"Item": {"coupleId": "old-hashed-couple"}},
            {"Item": {"user1": {"email": "alan.lopezg9707@gmail.com"}}},
        ]

        with patch.object(
            self.handler,
            "LEGACY_FINANCES_EMAILS",
            frozenset({"alan.lopezg9707@gmail.com", "mariana.delgado6@icloud.com"}),
        ):
            couple_pk, _user = self.handler._resolve_couple_context(
                event(email="alan.lopezg9707@gmail.com")
            )

        self.assertEqual(couple_pk, "PAREJA#DEFAULT")

    def test_init_preserves_email_based_couple_id(self):
        self.table.get_item.return_value = {}
        user = {"sub": "user-1", "email": "uno@example.com", "name": "Uno"}

        result, status = self.handler.init_couple(
            user, "Uno", "Dos", "uno@example.com", "dos@example.com"
        )

        expected_id = self.handler._couple_id_for_emails("uno@example.com", "dos@example.com")
        self.assertEqual(status, 201)
        self.assertEqual(result["coupleId"], expected_id)
        actions = self.resource.meta.client.transact_write_items.call_args.kwargs["TransactItems"]
        self.assertEqual(len(actions), 4)
        user_membership = actions[3]["Put"]["Item"]
        self.assertNotIn("email", user_membership)

    def test_init_resolves_missing_claim_email_from_access_token(self):
        request = event(
            path="/finances/init",
            method="POST",
            body={
                "user1Email": "uno@example.com",
                "user2Email": "dos@example.com",
                "user1Name": "Uno",
                "user2Name": "Dos",
            },
        )
        del request["requestContext"]["authorizer"]["jwt"]["claims"]["email"]
        request["headers"] = {"Authorization": "Bearer access-token"}

        with patch.object(self.handler, "_email_from_access_token", return_value="uno@example.com"):
            response = self.handler.lambda_handler(request, lambda_context())

        self.assertEqual(response["statusCode"], 201)

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

    def _savings_state(self, accumulated="0", cushion="0", order=None, version=1):
        goal_id = "goal-home"
        order = [goal_id] if order is None else order
        config = {"goalOrder": order, "version": version, "currency": "COP"}
        goals = {
            goal_id: {
                "goalId": goal_id,
                "name": "Casa",
                "targetAmount": Decimal("100"),
                "accumulatedAmount": Decimal(accumulated),
                "status": "pending",
                "createdAt": "2026-10-01T00:00:00+00:00",
                "updatedAt": "2026-10-01T00:00:00+00:00",
            }
        } if goal_id in order else {}
        cushion_item = {
            "goalId": "COLCHON",
            "name": "Colchón",
            "accumulatedAmount": Decimal(cushion),
            "status": "cushion",
        }
        return config, goals, cushion_item

    def _capture_savings_builder(self):
        captured = {}

        def run_builder(_couple_pk, _key_hash, _request_hash, builder):
            actions, result, status = builder()
            captured["actions"] = actions
            return result, status, False

        return captured, run_builder

    def _contribution(self, amount, state, note=None):
        captured, run_builder = self._capture_savings_builder()
        user = {"sub": "user-1", "email": "uno@example.com"}
        with (
            patch.object(self.handler, "_read_savings_state", return_value=state),
            patch.object(self.handler, "_run_idempotent_transaction", side_effect=run_builder),
            patch.object(self.handler, "get_savings_summary", return_value={"cushion": {"balance": 0}}),
        ):
            response, status = self.handler.register_savings_contribution(
                "PAREJA#couple-1",
                user,
                {"amount": amount, "note": note, "date": "2026-10-08T12:00:00Z"},
                "request-key-hash",
            )
        return response, status, captured["actions"]

    def test_partial_contribution_stays_on_active_goal(self):
        response, status, actions = self._contribution(Decimal("40"), self._savings_state())

        self.assertEqual(status, 201)
        self.assertEqual(response["allocatedToGoal"], Decimal("40"))
        self.assertEqual(response["sentToCushion"], Decimal("0"))
        self.assertIsNone(response["completedGoal"])
        self.assertEqual(response["activeGoalId"], "goal-home")
        self.assertEqual(len(actions), 4)

    def test_exact_contribution_completes_goal_and_activates_next(self):
        config, goals, cushion = self._savings_state(accumulated="60")
        config["goalOrder"] = ["goal-home", "goal-car"]
        goals["goal-car"] = {
            **goals["goal-home"],
            "goalId": "goal-car",
            "name": "Carro",
            "targetAmount": Decimal("200"),
            "accumulatedAmount": Decimal("0"),
        }

        response, _, actions = self._contribution(Decimal("40"), (config, goals, cushion))

        self.assertEqual(response["completedGoal"]["goalId"], "goal-home")
        self.assertEqual(response["activeGoalId"], "goal-car")
        self.assertEqual(response["sentToCushion"], Decimal("0"))
        config_action = actions[0]["Update"]
        self.assertEqual(config_action["ExpressionAttributeValues"][":goalOrder"], ["goal-car"])

    def test_excess_goes_directly_to_cushion_with_source_reference_and_note(self):
        response, _, actions = self._contribution(Decimal("50"), self._savings_state(accumulated="95"), "Bono")

        self.assertEqual(response["allocatedToGoal"], Decimal("5"))
        self.assertEqual(response["sentToCushion"], Decimal("45"))
        self.assertIsNone(response["activeGoalId"])
        movements = [action["Put"]["Item"] for action in actions if "Put" in action and action["Put"]["Item"].get("movementId")]
        self.assertEqual(len(movements), 2)
        surplus = movements[1]
        self.assertEqual(surplus["automaticReference"], "Sobrante de: Casa")
        self.assertEqual(surplus["note"], "Bono")
        self.assertEqual(surplus["sourceGoalId"], "goal-home")
        self.assertEqual(sum((item["amount"] for item in movements), Decimal("0")), Decimal("50"))

    def test_contribution_without_pending_goals_goes_to_cushion_without_surplus_reference(self):
        config, goals, cushion = self._savings_state(order=[])
        response, _, actions = self._contribution(Decimal("25"), (config, goals, cushion))

        self.assertEqual(response["allocatedToGoal"], Decimal("0"))
        self.assertEqual(response["sentToCushion"], Decimal("25"))
        movements = [action["Put"]["Item"] for action in actions if "Put" in action and action["Put"]["Item"].get("movementId")]
        self.assertEqual(len(movements), 1)
        self.assertEqual(movements[0]["movementType"], "cushion_contribution")
        self.assertNotIn("automaticReference", movements[0])

    def test_valid_cushion_withdrawal_records_resulting_balance(self):
        captured, run_builder = self._capture_savings_builder()
        user = {"sub": "user-1", "email": "uno@example.com"}
        config, goals, cushion = self._savings_state(order=[], cushion="80")
        with (
            patch.object(self.handler, "_read_savings_state", return_value=(config, goals, cushion)),
            patch.object(self.handler, "_run_idempotent_transaction", side_effect=run_builder),
            patch.object(self.handler, "get_savings_summary", return_value={"cushion": {"balance": 30}}),
        ):
            result, status = self.handler.withdraw_from_cushion(
                "PAREJA#couple-1",
                user,
                {"amount": 50, "note": "Emergencia", "date": "2026-10-08T12:00:00Z"},
                "request-key-hash",
            )

        self.assertEqual(status, 201)
        self.assertEqual(result["balanceAfter"], Decimal("30"))
        self.assertEqual(captured["actions"][1]["Update"]["ExpressionAttributeValues"][":newBalance"], Decimal("30"))

    def test_withdrawal_over_cushion_balance_is_rejected(self):
        config, goals, cushion = self._savings_state(order=[], cushion="20")
        captured, run_builder = self._capture_savings_builder()
        user = {"sub": "user-1", "email": "uno@example.com"}
        with (
            patch.object(self.handler, "_read_savings_state", return_value=(config, goals, cushion)),
            patch.object(self.handler, "_run_idempotent_transaction", side_effect=run_builder),
        ):
            with self.assertRaises(self.handler.SavingsError) as error:
                self.handler.withdraw_from_cushion(
                    "PAREJA#couple-1",
                    user,
                    {"amount": 21, "note": "Emergencia", "date": "2026-10-08T12:00:00Z"},
                    "request-key-hash",
                )

        self.assertEqual(error.exception.status_code, 422)
        self.assertNotIn("actions", captured)

    def test_normal_goal_cannot_be_archived_as_cushion(self):
        with self.assertRaises(self.handler.SavingsError) as error:
            self.handler.archive_savings_goal("PAREJA#couple-1", "COLCHON")
        self.assertEqual(error.exception.status_code, 409)

    def test_cushion_and_config_are_created_automatically_once(self):
        self.table.get_item.side_effect = [
            {"Item": {"PK": "PAREJA#couple-1", "SK": "META", "currency": "COP"}},
            {},
            {},
            {"Item": {"goalOrder": [], "version": 0, "currency": "COP"}},
            {"Item": {"goalId": "COLCHON", "name": "Colchón", "accumulatedAmount": Decimal("0")}},
        ]
        self.table.query.return_value = {"Items": []}
        with (
            patch.object(self.handler, "query_all", return_value=[]),
            patch.object(self.handler, "_latest_savings_movements", return_value=[]),
        ):
            result = self.handler.get_savings_summary("PAREJA#couple-1")

        self.assertEqual(result["currency"], "COP")
        self.assertEqual(result["cushion"]["goalId"], "COLCHON")
        self.assertEqual(self.table.put_item.call_count, 2)

    def test_cushion_cannot_be_edited_archived_or_reordered(self):
        config, goals, cushion = self._savings_state(order=[])
        with (
            patch.object(self.handler, "_read_savings_state", return_value=(config, goals, cushion)),
            self.assertRaises(self.handler.SavingsError),
        ):
            self.handler.update_savings_goal("PAREJA#couple-1", "COLCHON", {"name": "Fondo"})

        with self.assertRaises(self.handler.SavingsError):
            self.handler.archive_savings_goal("PAREJA#couple-1", "COLCHON")

        with patch.object(self.handler, "_read_savings_state", return_value=(config, goals, cushion)):
            with self.assertRaises(self.handler.SavingsError):
                self.handler.reorder_savings_goals("PAREJA#couple-1", ["COLCHON"])

    def test_withdrawal_cannot_target_a_normal_goal(self):
        with self.assertRaises(self.handler.SavingsError) as error:
            self.handler.withdraw_from_cushion(
                "PAREJA#couple-1",
                {"sub": "user-1", "email": "uno@example.com"},
                {
                    "amount": 1,
                    "note": "Motivo",
                    "date": "2026-10-08T12:00:00Z",
                    "goalId": "goal-home",
                },
                "request-key-hash",
            )
        self.assertEqual(error.exception.status_code, 400)

    def test_editing_target_to_accumulated_balance_completes_goal(self):
        config, goals, cushion = self._savings_state(accumulated="100")
        captured = {}

        def execute(actions):
            captured["actions"] = actions

        with (
            patch.object(self.handler, "_read_savings_state", return_value=(config, goals, cushion)),
            patch.object(self.handler, "_execute_savings_transaction", side_effect=execute),
            patch.object(self.handler, "get_savings_summary", return_value={"activeGoal": None}),
        ):
            result, status = self.handler.update_savings_goal(
                "PAREJA#couple-1",
                "goal-home",
                {"targetAmount": 100},
            )

        self.assertEqual(status, 200)
        self.assertEqual(result["goal"]["status"], "completed")
        self.assertEqual(captured["actions"][0]["Update"]["ExpressionAttributeValues"][":goalOrder"], [])
        self.assertIn(":completed", captured["actions"][1]["Update"]["ExpressionAttributeValues"])

    def test_reorder_changes_only_config_and_rejects_duplicates(self):
        config, goals, cushion = self._savings_state()
        config["goalOrder"] = ["goal-home", "goal-car"]
        goals["goal-car"] = {
            **goals["goal-home"],
            "goalId": "goal-car",
            "name": "Carro",
        }
        captured = {}

        def execute(actions):
            captured["actions"] = actions

        with (
            patch.object(self.handler, "_read_savings_state", return_value=(config, goals, cushion)),
            patch.object(self.handler, "_execute_savings_transaction", side_effect=execute),
            patch.object(self.handler, "get_savings_summary", return_value={"activeGoal": {"goalId": "goal-car"}}),
        ):
            result, status = self.handler.reorder_savings_goals("PAREJA#couple-1", ["goal-car", "goal-home"])
            with self.assertRaises(self.handler.SavingsError):
                self.handler.reorder_savings_goals("PAREJA#couple-1", ["goal-car", "goal-car"])

        self.assertEqual(status, 200)
        self.assertEqual(result["activeGoal"]["goalId"], "goal-car")
        self.assertEqual(len(captured["actions"]), 1)
        self.assertEqual(captured["actions"][0]["Update"]["Key"]["SK"], "AHORROS#CONFIG")

    def test_wedding_budget_link_is_reserved_atomically(self):
        config, goals, cushion = self._savings_state(order=[])
        captured, run_builder = self._capture_savings_builder()
        self.table.get_item.return_value = {}
        user = {"sub": "user-1", "email": "uno@example.com"}
        with (
            patch.object(self.handler, "_read_savings_state", return_value=(config, goals, cushion)),
            patch.object(self.handler, "_run_idempotent_transaction", side_effect=run_builder),
            patch.object(self.handler, "get_savings_summary", return_value={"currency": "COP"}),
        ):
            result, status = self.handler.create_savings_goal(
                "PAREJA#couple-1",
                user,
                {"name": "Boda", "targetAmount": 1000, "bodaId": "wedding-1"},
                "request-key-hash",
            )

        self.assertEqual(status, 201)
        self.assertTrue(result["goal"]["isLinkedToWeddingBudget"])
        link_items = [action["Put"]["Item"] for action in captured["actions"] if "Put" in action and action["Put"]["Item"].get("SK", "").startswith("VINCULO_BODA#")]
        self.assertEqual(link_items[0]["SK"], "VINCULO_BODA#wedding-1")

    def test_concurrent_contributions_retry_against_latest_balance(self):
        initial_state = self._savings_state(accumulated="0", version=1)
        concurrently_updated_state = self._savings_state(accumulated="40", version=2)
        user = {"sub": "user-1", "email": "uno@example.com"}
        cancellation = ClientError(
            {"Error": {"Code": "TransactionCanceledException", "Message": "conflict"}},
            "TransactWriteItems",
        )
        states = [initial_state, concurrently_updated_state]
        actions_seen = []

        def build_and_capture(_couple_pk, _key_hash, _request_hash, builder):
            actions, result, status = builder()
            actions_seen.append(actions)
            return result, status, False

        with (
            patch.object(self.handler, "_get_idempotency_record", side_effect=[None, None]),
            patch.object(self.handler, "_read_savings_state", side_effect=states),
            patch.object(self.handler, "_execute_savings_transaction", side_effect=[cancellation, None]),
            patch.object(self.handler, "get_savings_summary", return_value={"activeGoal": {"goalId": "goal-home"}}),
        ):
            result, status = self.handler.register_savings_contribution(
                "PAREJA#couple-1",
                user,
                {"amount": 40, "date": "2026-10-08T12:00:00Z"},
                "request-key-hash",
            )

        self.assertEqual(status, 201)
        self.assertEqual(len(actions_seen), 2)
        self.assertEqual(result["goalBalance"], Decimal("80"))
        self.assertEqual(actions_seen[1][0]["Update"]["ExpressionAttributeValues"][":expectedVersion"], 2)

    def test_idempotency_key_replays_same_request(self):
        self.table.get_item.return_value = {
            "Item": {
                "requestHash": "payload-hash",
                "statusCode": 201,
                "result": {"operationId": "operation-1"},
            }
        }
        builder = MagicMock(side_effect=AssertionError("builder must not run on replay"))

        result, status, replay = self.handler._run_idempotent_transaction(
            "PAREJA#couple-1",
            "idempotency-hash",
            "payload-hash",
            builder,
        )

        self.assertEqual(result["operationId"], "operation-1")
        self.assertEqual(status, 200)
        self.assertTrue(replay)
        builder.assert_not_called()

    def test_idempotency_key_cannot_be_reused_with_different_payload(self):
        self.table.get_item.return_value = {
            "Item": {"requestHash": "original-hash", "result": {}}
        }

        with self.assertRaises(self.handler.SavingsError) as error:
            self.handler._run_idempotent_transaction(
                "PAREJA#couple-1",
                "idempotency-hash",
                "different-hash",
                MagicMock(),
            )

        self.assertEqual(error.exception.status_code, 409)


if __name__ == "__main__":
    unittest.main()
