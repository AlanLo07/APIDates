import json
import boto3
import os
import uuid
from botocore.config import Config

s3_client = boto3.client('s3', config=Config(signature_version='s3v4'))
BUCKET_NAME = os.environ.get('BUCKET_NAME')

HOME_MASCOT_PREFIX = 'home-mascot-images/'

def build_response(status_code, body):
    return {
        'statusCode': status_code,
        'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
        'body': json.dumps(body)
    }

def lambda_handler(event, context):
    method = event.get('requestContext', {}).get('http', {}).get('method', '')
    raw_path = event.get('rawPath', '')

    if method == 'OPTIONS':
        return build_response(200, {})

    try:
        if method == 'GET' and raw_path.endswith('/home-mascot-images'):
            return list_home_mascot_images()

        return create_upload_url(event)
    except Exception as e:
        return build_response(500, {'error': str(e)})


def create_upload_url(event):
    body = json.loads(event.get('body', '{}'))
    file_name = body.get('fileName', 'image.jpg')
    file_type = body.get('fileType', 'image/jpeg')

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

    return build_response(200, {
        'uploadUrl': presigned_url,
        'key': unique_key,
        'finalUrl': f"https://{BUCKET_NAME}.s3.amazonaws.com/{unique_key}"
    })


def list_home_mascot_images():
    urls = []
    continuation_token = None

    while True:
        kwargs = {'Bucket': BUCKET_NAME, 'Prefix': HOME_MASCOT_PREFIX}
        if continuation_token:
            kwargs['ContinuationToken'] = continuation_token

        result = s3_client.list_objects_v2(**kwargs)
        for obj in result.get('Contents', []):
            key = obj['Key']
            # Ignorar el "folder" vacío que representa el prefijo
            if key == HOME_MASCOT_PREFIX:
                continue
            urls.append(f"https://{BUCKET_NAME}.s3.amazonaws.com/{key}")

        if not result.get('IsTruncated'):
            break
        continuation_token = result.get('NextContinuationToken')

    return build_response(200, {'items': urls, 'count': len(urls)})