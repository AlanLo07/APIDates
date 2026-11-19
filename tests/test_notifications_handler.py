import hashlib
import importlib.util
import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
COMMON_LAYER = ROOT / "lambdas" / "layers" / "baselayer" / "python"
HANDLER_PATH = ROOT / "lambdas" / "functions" / "NotificationsCRUD" / "handler.py"
sys.path.insert(0, str(COMMON_LAYER))
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-2")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")


def load_handler():
    with patch("boto3.resource"), patch("boto3.client"):
        spec = importlib.util.spec_from_file_location("notifications_handler_test", HANDLER_PATH)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module


def make_event(method, path, body=None, sub="user-sub"):
    return {
        "rawPath": path,
        "requestContext": {
            "http": {"method": method},
            "authorizer": {"jwt": {"claims": {"sub": sub}}},
        },
        "body": json.dumps(body or {}),
    }


class NotificationsHandlerTests(unittest.TestCase):
    def setUp(self):
        self.handler = load_handler()

    def test_registration_uses_cognito_sub_and_hashes_fid(self):
        fid = "sample-firebase-installation-id"
        response = self.handler.lambda_handler(
            make_event("POST", "/notifications/devices", {
                "fid": fid,
                "platform": "android",
                "userId": "attacker-sub",
            }),
            None,
        )

        item = self.handler.devices_table.put_item.call_args.kwargs["Item"]
        self.assertEqual(response["statusCode"], 201)
        self.assertEqual(item["userId"], "user-sub")
        self.assertEqual(item["fidHash"], hashlib.sha256(fid.encode()).hexdigest())
        self.assertEqual(item["fid"], fid)
        self.assertNotIn(fid, response["body"])

    def test_registration_rejects_unsupported_platform(self):
        response = self.handler.lambda_handler(
            make_event("POST", "/notifications/devices", {
                "fid": "sample-firebase-installation-id",
                "platform": [],
            }),
            None,
        )

        self.assertEqual(response["statusCode"], 400)
        self.handler.devices_table.put_item.assert_not_called()

    def test_registration_requires_authenticated_sub(self):
        response = self.handler.lambda_handler(
            make_event("POST", "/notifications/devices", {
                "fid": "sample-firebase-installation-id",
                "platform": "web",
            }, sub=""),
            None,
        )

        self.assertEqual(response["statusCode"], 401)
        self.handler.devices_table.put_item.assert_not_called()

    def test_unregister_is_conditioned_on_cognito_sub(self):
        response = self.handler.lambda_handler(
            make_event("DELETE", "/notifications/devices", {"fid": "sample-firebase-installation-id"}),
            None,
        )

        delete = self.handler.devices_table.delete_item.call_args.kwargs
        self.assertEqual(response["statusCode"], 200)
        self.assertEqual(delete["ExpressionAttributeValues"], {":user_id": "user-sub"})

    def test_test_push_queries_only_authenticated_user_devices(self):
        self.handler.devices_table.query.return_value = {
            "Items": [{"fid": "device-fid", "fidHash": "fid-hash"}]
        }
        with patch.object(self.handler, "_send_fcm") as send_fcm:
            response = self.handler.lambda_handler(
                make_event("POST", "/notifications/test", {}),
                None,
            )

        self.assertEqual(response["statusCode"], 200)
        self.assertEqual(json.loads(response["body"]), {"sent": 1, "failed": 0})
        self.handler.devices_table.query.assert_called_once()
        send_fcm.assert_called_once()


if __name__ == "__main__":
    unittest.main()