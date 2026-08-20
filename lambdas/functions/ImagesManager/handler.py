import json
import logging
import boto3
import os
import uuid
from botocore.config import Config

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3', config=Config(signature_version='s3v4'))
BUCKET_NAME = os.environ.get('BUCKET_NAME')

HOME_MASCOT_PREFIX = 'assets/home-mascot-images/'

def build_response(status_code, body):
    return {
        'statusCode': status_code,
        'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
        'body': json.dumps(body)
    }

def lambda_handler(event, context):
    method = event.get('requestContext', {}).get('http', {}).get('method', '')
    raw_path = event.get('rawPath', '')
    logger.info(json.dumps({
        'level': '⚪️',
        'message': 'Solicitud ImagesManager',
        'method': method,
        'path': raw_path,
        'bucket_configured': bool(BUCKET_NAME),
        'function': getattr(context, 'function_name', 'unknown'),
    }, ensure_ascii=False))

    if method == 'OPTIONS':
        logger.info(json.dumps({'level': '🟢', 'message': 'Preflight CORS ImagesManager'}, ensure_ascii=False))
        return build_response(200, {})

    try:
        if method == 'GET' and raw_path.endswith('/home-mascot-images'):
            return list_home_mascot_images()

        logger.info(json.dumps({'level': '🔵', 'message': 'Generando URL de carga ImagesManager'}, ensure_ascii=False))
        return create_upload_url(event)
    except ValueError as e:
        logger.warning(json.dumps({'level': '🟡', 'message': 'Validación fallida en ImagesManager', 'error': str(e)}, ensure_ascii=False))
        return build_response(400, {'error': str(e)})
    except Exception as e:
        logger.exception(json.dumps({'level': '🔴', 'message': 'Error inesperado en ImagesManager'}, ensure_ascii=False))
        return build_response(500, {'error': str(e)})


def create_upload_url(event):
    body = json.loads(event.get('body', '{}'))
    file_name = body.get('fileName', 'image.jpg')
    file_type = body.get('fileType', 'image/jpeg')

    if not BUCKET_NAME:
        logger.error(json.dumps({'level': '🔴', 'message': 'BUCKET_NAME no configurado en ImagesManager'}, ensure_ascii=False))
        raise ValueError('Bucket no configurado')

    # Generar un nombre único para evitar colisiones
    extension = file_name.split('.')[-1]
    unique_key = f"uploads/{uuid.uuid4()}.{extension}"

    # Generar URL firmada para PUT
    presigned_url = s3_client.generate_presigned_url(
        'put_object',
        Params={
            'Bucket': BUCKET_NAME,
            'Key': unique_key,
            'ContentType': file_type
        },
        ExpiresIn=3600 # 1 hora
    )

    logger.info(json.dumps({
        'level': '🟢',
        'message': 'URL de carga generada',
        'file_type': file_type,
        'key': unique_key,
    }, ensure_ascii=False))

    return build_response(200, {
        'uploadUrl': presigned_url,
        'key': unique_key,
        'finalUrl': f"https://{BUCKET_NAME}.s3.amazonaws.com/{unique_key}"
    })


def list_home_mascot_images():
    if not BUCKET_NAME:
        logger.error(json.dumps({'level': '🔴', 'message': 'BUCKET_NAME no configurado al listar imágenes'}, ensure_ascii=False))
        raise ValueError('Bucket no configurado')

    urls = []
    continuation_token = None
    page_count = 0

    while True:
        page_count += 1
        kwargs = {'Bucket': BUCKET_NAME, 'Prefix': HOME_MASCOT_PREFIX}
        if continuation_token:
            kwargs['ContinuationToken'] = continuation_token

        result = s3_client.list_objects_v2(**kwargs)
        logger.info(json.dumps({
            'level': '🔵',
            'message': 'Página S3 consultada',
            'page': page_count,
            'objects': len(result.get('Contents', [])),
            'truncated': bool(result.get('IsTruncated')),
        }, ensure_ascii=False))
        for obj in result.get('Contents', []):
            key = obj['Key']
            # Ignorar el "folder" vacío que representa el prefijo
            if key == HOME_MASCOT_PREFIX:
                continue
            urls.append(f"https://{BUCKET_NAME}.s3.amazonaws.com/{key}")

        if not result.get('IsTruncated'):
            break
        continuation_token = result.get('NextContinuationToken')

    logger.info(json.dumps({
        'level': '🟢',
        'message': 'Imágenes listadas',
        'pages': page_count,
        'count': len(urls),
    }, ensure_ascii=False))
    return build_response(200, {'items': urls, 'count': len(urls)})