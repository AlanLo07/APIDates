import importlib.util
import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
COMMON_LAYER = ROOT / "lambdas" / "layers" / "baselayer" / "python"
HANDLER_PATH = ROOT / "lambdas" / "functions" / "AuthCRUD" / "handler.py"
sys.path.insert(0, str(COMMON_LAYER))
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_EC2_METADATA_DISABLED", "true")
os.environ.setdefault("COGNITO_USER_POOL_ID", "test-user-pool")
os.environ.setdefault("COGNITO_CLIENT_ID", "test-client")


def load_handler():
    with patch("boto3.client") as client:
        spec = importlib.util.spec_from_file_location("auth_handler_test", HANDLER_PATH)
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module


class AuthHandlerTests(unittest.TestCase):
    def test_me_loads_missing_profile_claims_from_cognito(self):
        handler = load_handler()
        handler.cognito.get_user.return_value = {
            "Username": "user@example.com",
            "UserAttributes": [
                {"Name": "sub", "Value": "user-sub"},
                {"Name": "email", "Value": "user@example.com"},
                {"Name": "name", "Value": "User Name"},
            ],
        }
        event = {
            "headers": {"authorization": "Bearer access-token"},
            "requestContext": {
                "authorizer": {"jwt": {"claims": {"sub": "user-sub", "token_use": "access"}}}
            },
        }

        response = handler.get_me(event)

        self.assertEqual(response["statusCode"], 200)
        self.assertEqual(
            json.loads(response["body"]),
            {
                "sub": "user-sub",
                "username": "user@example.com",
                "email": "user@example.com",
                "name": "User Name",
            },
        )
        handler.cognito.get_user.assert_called_once_with(AccessToken="access-token")