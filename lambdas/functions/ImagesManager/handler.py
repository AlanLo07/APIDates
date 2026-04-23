import json
import boto3
import os
import uuid
from botocore.config import Config

s3_client = boto3.client('s3', config=Config(signature_version='s3v4'))
BUCKET_NAME = os.environ.get('BUCKET_NAME')

def build_response(status_code, body):
    return {
        'statusCode': status_code,
        'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'},
        'body': json.dumps(body)
    }

def lambda_handler(event, context):
    try:
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

    except Exception as e:
        return build_response(500, {'error': str(e)})