"""Registro de dispositivos FCM y envío de notificaciones de prueba."""
import hashlib
import json
import logging
import os
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from common.utils import build_response, log_event, parse_body

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_NAME = os.environ.get("NOTIFICATION_DEVICES_TABLE", "NotificationDevices")
FCM_SECRET_ARN = os.environ.get("FCM_CREDENTIALS_SECRET_ARN", "")
USER_INDEX = "userId-index"
FCM_APP_NAME = "apdates-fcm-demo"
ALLOWED_PLATFORMS = {"android", "ios", "web"}

dynamodb = boto3.resource("dynamodb")
devices_table = dynamodb.Table(TABLE_NAME)
secrets_manager = boto3.client("secretsmanager")
_firebase_app = None
_firebase_messaging = None


def _user_id(event: dict) -> str:
    claims = (
        event.get("requestContext", {})
        .get("authorizer", {})
        .get("jwt", {})
        .get("claims", {})
    )
    return str(claims.get("sub") or "").strip()


def _read_body(event: dict) -> dict:
    body = parse_body(event)
    if not isinstance(body, dict):
        raise ValueError("El body debe ser un objeto JSON")
    return body


def _normalized_fid(body: dict) -> str:
    fid = body.get("fid")
    if not isinstance(fid, str):
        raise ValueError("El campo 'fid' es requerido")
    fid = fid.strip()
    if not fid or len(fid) > 4096:
        raise ValueError("El FID debe tener entre 1 y 4096 caracteres")
    return fid


def _register_device(user_id: str, body: dict):
    fid = _normalized_fid(body)
    platform = body.get("platform")
    if not isinstance(platform, str) or platform not in ALLOWED_PLATFORMS:
        raise ValueError("'platform' debe ser android, ios o web")

    fid_hash = hashlib.sha256(fid.encode("utf-8")).hexdigest()
    devices_table.put_item(Item={
        "fidHash": fid_hash,
        "fid": fid,
        "userId": user_id,
        "platform": platform,
        "updatedAt": datetime.now(timezone.utc).isoformat(),
    })
    log_event(logger, "🟢", "Dispositivo FCM registrado", platform=platform)
    return build_response(201, {"message": "Dispositivo registrado"})


def _unregister_device(user_id: str, body: dict):
    fid_hash = hashlib.sha256(_normalized_fid(body).encode("utf-8")).hexdigest()
    try:
        devices_table.delete_item(
            Key={"fidHash": fid_hash},
            ConditionExpression="userId = :user_id",
            ExpressionAttributeValues={":user_id": user_id},
        )
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            return build_response(404, {"message": "Dispositivo no encontrado"})
        raise
    log_event(logger, "🟢", "Dispositivo FCM eliminado")
    return build_response(200, {"message": "Dispositivo eliminado"})


def _get_firebase_messaging():
    global _firebase_app, _firebase_messaging
    if _firebase_messaging is not None:
        return _firebase_app, _firebase_messaging
    if not FCM_SECRET_ARN:
        raise RuntimeError("No está configurado el secreto de credenciales FCM")

    secret = secrets_manager.get_secret_value(SecretId=FCM_SECRET_ARN)
    service_account = json.loads(secret["SecretString"])

    import firebase_admin
    from firebase_admin import credentials, messaging

    try:
        _firebase_app = firebase_admin.get_app(FCM_APP_NAME)
    except ValueError:
        _firebase_app = firebase_admin.initialize_app(
            credentials.Certificate(service_account),
            name=FCM_APP_NAME,
        )
    _firebase_messaging = messaging
    return _firebase_app, _firebase_messaging


def _send_fcm(fid: str, title: str, message_body: str):
    app, messaging = _get_firebase_messaging()
    message = messaging.Message(
        notification=messaging.Notification(title=title, body=message_body),
        fid=fid,
    )
    return messaging.send(message, app=app)


def _send_test_notification(user_id: str, body: dict):
    title = body.get("title", "APIDates: prueba FCM")
    message_body = body.get("body", "Las notificaciones están conectadas.")
    if not isinstance(title, str) or not title.strip() or len(title) > 100:
        raise ValueError("'title' debe tener entre 1 y 100 caracteres")
    if not isinstance(message_body, str) or not message_body.strip() or len(message_body) > 500:
        raise ValueError("'body' debe tener entre 1 y 500 caracteres")

    result = devices_table.query(
        IndexName=USER_INDEX,
        KeyConditionExpression=Key("userId").eq(user_id),
    )
    devices = result.get("Items", [])
    if not devices:
        return build_response(404, {"message": "No hay dispositivos registrados"})

    sent = 0
    failed = 0
    for device in devices:
        try:
            _send_fcm(device["fid"], title.strip(), message_body.strip())
            sent += 1
        except Exception as exc:
            failed += 1
            if type(exc).__name__ == "UnregisteredError":
                try:
                    devices_table.delete_item(
                        Key={"fidHash": device["fidHash"]},
                        ConditionExpression="userId = :user_id",
                        ExpressionAttributeValues={":user_id": user_id},
                    )
                except ClientError as delete_error:
                    if delete_error.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
                        raise
            log_event(logger, "🟡", "Fallo al enviar push", error_type=type(exc).__name__)

    log_event(logger, "🟢" if sent else "🔴", "Envío de prueba FCM completado", sent=sent, failed=failed)
    status_code = 200 if sent else 502
    return build_response(status_code, {"sent": sent, "failed": failed})


def lambda_handler(event, context):
    method = (event.get("requestContext", {}).get("http", {}).get("method") or "").upper()
    path = event.get("rawPath", "")
    log_event(logger, "⚪️", "Solicitud de notificaciones", method=method, path=path)

    user_id = _user_id(event)
    if not user_id:
        return build_response(401, {"message": "Usuario no autenticado"})

    try:
        body = _read_body(event)
        if method == "POST" and path == "/notifications/devices":
            return _register_device(user_id, body)
        if method == "DELETE" and path == "/notifications/devices":
            return _unregister_device(user_id, body)
        if method == "POST" and path == "/notifications/test":
            return _send_test_notification(user_id, body)
        return build_response(404, {"message": "Ruta no encontrada"})
    except ValueError as exc:
        return build_response(400, {"message": str(exc)})
    except Exception as exc:
        log_event(logger, "🔴", "Error en notificaciones", error_type=type(exc).__name__)
        return build_response(500, {"message": "No fue posible procesar la solicitud"})