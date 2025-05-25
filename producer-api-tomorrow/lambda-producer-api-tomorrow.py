import json
import requests
import boto3
import os

# Configurações
LOCATION = "patos pb"
TOMORROW_API_KEY = os.getenv('TOMORROW_API_KEY')

url = f"https://api.tomorrow.io/v4/weather/realtime?location={LOCATION}&apikey={TOMORROW_API_KEY}"
headers = {
    "accept": "application/json",
    "accept-encoding": "deflate, gzip, br"
}

STREAM_NAME = "broker"
REGION = "us-east-1"

kinesis_client = boto3.client('kinesis', region_name=REGION)

def lambda_handler(event, context):
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    weather_data = response.json()

    kinesis_client.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(weather_data),
        PartitionKey="partition_key"
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Dados enviados ao Kinesis com sucesso')
    }
