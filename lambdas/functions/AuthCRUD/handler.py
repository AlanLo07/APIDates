"""
handler.py — Autenticación de usuarios usando Amazon Cognito (User Pool).

Rutas:
- POST /auth/signup              -> Registra usuario (email, password, name)
- POST /auth/confirm             -> Confirma cuenta con código enviado por email
- POST /auth/resend-code         -> Reenvía código de confirmación
- POST /auth/login               -> Login (email, password) -> tokens JWT
- POST /auth/refresh             -> Renueva tokens con refreshToken
- POST /auth/forgot-password     -> Inicia recuperación de contraseña
- POST /auth/confirm-forgot-password -> Confirma nueva contraseña con código
- POST /auth/logout              -> Invalida el accessToken (global sign out)
- GET  /auth/me                  -> Datos del usuario autenticado (requiere JWT)

No maneja contraseñas ni tokens en DynamoDB: Cognito es la única fuente de verdad.
"""
import json
import logging
import os

import boto3
from botocore.exceptions import ClientError

from common.utils import build_response, log_event, parse_body  # type: ignore

logger = logging.getLogger()
logger.setLevel(logging.INFO)

USER_POOL_ID = os.environ["COGNITO_USER_POOL_ID"]
CLIENT_ID = os.environ["COGNITO_CLIENT_ID"]

cognito = boto3.client("cognito-idp")

# ─── Mapeo de errores comunes de Cognito -> mensajes claros ──────────────────

COGNITO_ERROR_MESSAGES = {
    "UsernameExistsException": ("El correo ya está registrado", 409),
    "UserNotFoundException": ("Usuario no encontrado", 404),
    "NotAuthorizedException": ("Credenciales inválidas", 401),
    "CodeMismatchException": ("Código de verificación incorrecto", 400),
    "ExpiredCodeException": ("El código expiró, solicita uno nuevo", 400),
    "UserNotConfirmedException": ("La cuenta aún no ha sido confirmada", 403),
    "InvalidPasswordException": ("La contraseña no cumple los requisitos mínimos", 400),
    "LimitExceededException": ("Demasiados intentos, intenta más tarde", 429),
    "TooManyRequestsException": ("Demasiadas solicitudes, intenta más tarde", 429),
    "InvalidParameterException": ("Parámetros inválidos", 400),
    "AliasExistsException": ("El correo ya está asociado a otra cuenta", 409),
}


def _handle_cognito_error(e: ClientError, action: str) -> dict:
    """🔴 Traduce errores de Cognito a una respuesta HTTP estándar."""
    code = e.response["Error"]["Code"]
    message, status = COGNITO_ERROR_MESSAGES.get(code, ("Error de autenticación", 400))
    log_event(logger, "🔴", f"Error en {action}", cognito_error=code)
    return build_response(status, {"error": message})


# ─── Operaciones de auth ──────────────────────────────────────────────────────

def signup(email: str, password: str, name: str | None) -> dict:
    attrs = [{"Name": "email", "Value": email}]
    if name:
        attrs.append({"Name": "name", "Value": name})

    response = cognito.sign_up(
        ClientId=CLIENT_ID,
        Username=email,
        Password=password,
        UserAttributes=attrs,
    )
    log_event(logger, "🟢", "Usuario registrado", email_domain=email.split("@")[-1])
    return build_response(201, {
        "message": "Registro exitoso, revisa tu correo para confirmar la cuenta",
        "userSub": response["UserSub"],
        "confirmed": response.get("UserConfirmed", False),
    })


def confirm_signup(email: str, code: str) -> dict:
    cognito.confirm_sign_up(ClientId=CLIENT_ID, Username=email, ConfirmationCode=code)
    log_event(logger, "🟢", "Cuenta confirmada", email_domain=email.split("@")[-1])
    return build_response(200, {"message": "Cuenta confirmada con éxito"})


def resend_code(email: str) -> dict:
    cognito.resend_confirmation_code(ClientId=CLIENT_ID, Username=email)
    log_event(logger, "🔵", "Código de confirmación reenviado", email_domain=email.split("@")[-1])
    return build_response(200, {"message": "Código reenviado"})


def login(email: str, password: str) -> dict:
    response = cognito.initiate_auth(
        ClientId=CLIENT_ID,
        AuthFlow="USER_PASSWORD_AUTH",
        AuthParameters={"USERNAME": email, "PASSWORD": password},
    )
    result = response["AuthenticationResult"]
    log_event(logger, "🟢", "Login exitoso", email_domain=email.split("@")[-1])
    return build_response(200, {
        "idToken": result["IdToken"],
        "accessToken": result["AccessToken"],
        "refreshToken": result["RefreshToken"],
        "expiresIn": result["ExpiresIn"],
    })


def refresh_tokens(refresh_token: str) -> dict:
    response = cognito.initiate_auth(
        ClientId=CLIENT_ID,
        AuthFlow="REFRESH_TOKEN_AUTH",
        AuthParameters={"REFRESH_TOKEN": refresh_token},
    )
    result = response["AuthenticationResult"]
    log_event(logger, "🔵", "Tokens renovados")
    return build_response(200, {
        "idToken": result["IdToken"],
        "accessToken": result["AccessToken"],
        "expiresIn": result["ExpiresIn"],
    })


def forgot_password(email: str) -> dict:
    cognito.forgot_password(ClientId=CLIENT_ID, Username=email)
    log_event(logger, "🔵", "Recuperación de contraseña iniciada", email_domain=email.split("@")[-1])
    return build_response(200, {"message": "Código de recuperación enviado al correo"})


def confirm_forgot_password(email: str, code: str, new_password: str) -> dict:
    cognito.confirm_forgot_password(
        ClientId=CLIENT_ID,
        Username=email,
        ConfirmationCode=code,
        Password=new_password,
    )
    log_event(logger, "🟢", "Contraseña restablecida", email_domain=email.split("@")[-1])
    return build_response(200, {"message": "Contraseña actualizada con éxito"})


def logout(access_token: str) -> dict:
    cognito.global_sign_out(AccessToken=access_token)
    log_event(logger, "🟢", "Sesión cerrada")
    return build_response(200, {"message": "Sesión cerrada con éxito"})


def get_me(event: dict) -> dict:
    """🔵 Lee los claims del JWT ya validado por el authorizer de API Gateway."""
    claims = (
        event.get("requestContext", {})
        .get("authorizer", {})
        .get("jwt", {})
        .get("claims", {})
    )
    if not claims:
        return build_response(401, {"error": "No autenticado"})
    return build_response(200, {
        "sub": claims.get("sub"),
        "email": claims.get("email"),
        "name": claims.get("name"),
    })


# ─── Handler principal ────────────────────────────────────────────────────────

def lambda_handler(event, context):
    """🔵 Enruta las peticiones de autenticación."""
    method = event.get("requestContext", {}).get("http", {}).get("method", "")
    path = event.get("rawPath", "")

    logger.info(json.dumps({
        "level": "⚪️",
        "message": "Solicitud AuthCRUD",
        "method": method,
        "path": path,
        "function": getattr(context, "function_name", "unknown"),
    }, ensure_ascii=False))

    try:
        if method == "GET" and path == "/auth/me":
            return get_me(event)

        body = parse_body(event)

        if method == "POST" and path == "/auth/signup":
            required = {"email", "password"}
            if not required.issubset(body):
                return build_response(400, {"error": "Faltan campos requeridos (email, password)"})
            return signup(body["email"], body["password"], body.get("name"))

        if method == "POST" and path == "/auth/confirm":
            required = {"email", "code"}
            if not required.issubset(body):
                return build_response(400, {"error": "Faltan campos requeridos (email, code)"})
            return confirm_signup(body["email"], body["code"])

        if method == "POST" and path == "/auth/resend-code":
            if "email" not in body:
                return build_response(400, {"error": "email requerido"})
            return resend_code(body["email"])

        if method == "POST" and path == "/auth/login":
            required = {"email", "password"}
            if not required.issubset(body):
                return build_response(400, {"error": "Faltan campos requeridos (email, password)"})
            return login(body["email"], body["password"])

        if method == "POST" and path == "/auth/refresh":
            if "refreshToken" not in body:
                return build_response(400, {"error": "refreshToken requerido"})
            return refresh_tokens(body["refreshToken"])

        if method == "POST" and path == "/auth/forgot-password":
            if "email" not in body:
                return build_response(400, {"error": "email requerido"})
            return forgot_password(body["email"])

        if method == "POST" and path == "/auth/confirm-forgot-password":
            required = {"email", "code", "newPassword"}
            if not required.issubset(body):
                return build_response(400, {"error": "Faltan campos requeridos (email, code, newPassword)"})
            return confirm_forgot_password(body["email"], body["code"], body["newPassword"])

        if method == "POST" and path == "/auth/logout":
            if "accessToken" not in body:
                return build_response(400, {"error": "accessToken requerido"})
            return logout(body["accessToken"])

        return build_response(404, {"error": f"Ruta no encontrada: {method} {path}"})

    except ClientError as e:
        return _handle_cognito_error(e, path)
    except ValueError as e:
        log_event(logger, "🟡", "Body inválido", error=str(e))
        return build_response(400, {"error": str(e)})
    except Exception as e:  # noqa: BLE001
        log_event(logger, "🔴", "Error inesperado en AuthCRUD", error=str(e))
        return build_response(500, {"error": "Error interno del servidor"})
