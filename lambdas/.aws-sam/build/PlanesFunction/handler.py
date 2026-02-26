import json
import boto3
import os
from botocore.exceptions import ClientError
from decimal import Decimal

# Inicializamos el recurso de DynamoDB fuera del handler para reutilizar la conexión
dynamodb = boto3.resource('dynamodb')
TABLE_NAME = os.environ.get('TABLE_NAME', 'Planes')
table = dynamodb.Table(TABLE_NAME)

def lambda_handler(event, context):
    """
    Handler principal que rutea las peticiones según el método HTTP
    """
    http_method = event['requestContext']['http']['method']
    print(f"HTTP Method: {http_method}")
    path_parameters = event.get('pathParameters') or {}
    item_id = path_parameters.get('id')
    print(f"Path Parameters: {path_parameters}", "item id {item_id}")
    try:
        if http_method == 'GET':
            if item_id:
                return get_item(item_id)
            return get_all_items()
        
        elif http_method == 'POST':
            try:
                body = json.loads(event.get('body', '{}'))
            except Exception:
                body = event.get('body', '{}')
            if type(body) is list:
                for item in body:
                    create_item(item)
                else:
                    return build_response(201, {'message': 'Elementos creados'})
            return create_item(body)
        
        elif http_method == 'PUT':
            body = json.loads(event.get('body', '{}'))
            for item in body:
                print(f"item {item["nombre"]}")
                item_id = item.get('nombre', None)
                update_item(item_id, item)
            else:
                return build_response(200, {'message': 'Elementos actualizados'})
            
        elif http_method == 'DELETE':
            return delete_item(item_id)

        elif http_method == 'PATCH':
            body = table.scan()
            print(f"Body: {body}")
            body = body.get("Items",[])
            print(f"Updating all items with rating 0.0 {body}")
            for item in body:
                item["rating"] = Decimal('0.0')
                table.put_item(Item=item)
        
        else:
            return build_response(405, {'message': 'Método no permitido'})
            
    except Exception as e:
        print(f"Error: {e}")
        return build_response(500, {'error': str(e)})

# --- Funciones CRUD ---

def get_item(item_id):
    result = table.get_item(Key={'id': item_id})
    if 'Item' in result:
        return build_response(200, result['Item'])
    return build_response(404, {'message': 'No encontrado'})

def get_all_items():
    # Nota: Scan es costoso en tablas grandes, pero para 100 registros es ideal
    result = table.scan()
    print(f"Scan result: {result}")
    return build_response(200, result.get('Items', []))

def create_item(data):
    # Aquí podrías generar un ID único si no viene en el JSON
    typeLocation = data.get('typeLocation','')
    data["typeLocation"] = typeLocation
    data["isVisited"] = data.get('isVisited',False)
    print(f"Creating item: {data}")
    table.put_item(Item=data)
    return build_response(201, {'message': 'Creado con éxito'})

def update_item(item_id, data):
    # Aseguramos que el id del body coincida con el de la URL
    data['id'] = item_id
    table.put_item(Item=data)
    return build_response(200, {'message': 'Actualizado con éxito'})

def delete_item(item_id):
    table.delete_item(Key={'id': item_id})
    return build_response(200, {'message': 'Eliminado con éxito'})

# --- Helper para respuestas ---

def build_response(status_code, body):
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
        },
        'body': body
    }