import json
import base64
import os
import boto3

sns_client = boto3.client('sns')

SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:415407093357:alerts-data-climate'

PRECIPITATION_PROBABILITY_THRESHOLD = int(os.environ.get('PRECIPITATION_PROBABILITY_THRESHOLD', 70))
WIND_SPEED_THRESHOLD = float(os.environ.get('WIND_SPEED_THRESHOLD', 15))  # km/h
WIND_GUST_THRESHOLD = float(os.environ.get('WIND_GUST_THRESHOLD', 20))    # km/h
RAIN_INTENSITY_THRESHOLD = float(os.environ.get('RAIN_INTENSITY_THRESHOLD', 10))  # mm/h
SNOW_INTENSITY_THRESHOLD = float(os.environ.get('SNOW_INTENSITY_THRESHOLD', 5))   # mm/h
HIGH_TEMP_THRESHOLD = float(os.environ.get('HIGH_TEMP_THRESHOLD', 40))    # °C
LOW_TEMP_THRESHOLD = float(os.environ.get('LOW_TEMP_THRESHOLD', -5))      # °C
UV_INDEX_THRESHOLD = int(os.environ.get('UV_INDEX_THRESHOLD', 7))         # risco alto
LOW_VISIBILITY_THRESHOLD = float(os.environ.get('LOW_VISIBILITY_THRESHOLD', 1))   # km
LOW_PRESSURE_THRESHOLD = float(os.environ.get('LOW_PRESSURE_THRESHOLD', 1000))    # hPa

def lambda_handler(event, context):
    if 'Records' not in event:
        print("No records found in the event.")
        return {
            'statusCode': 400,
            'body': json.dumps('No records found in the event')
        }

    for record in event['Records']:
        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        data = json.loads(payload)

        values = data['data']['values']
        location = data['location']["name"]
        precipitation_probability = values.get('precipitationProbability', 0)
        wind_speed = values.get('windSpeed', 0)
        wind_gust = values.get('windGust', 0)
        rain_intensity = values.get('rainIntensity', 0)
        snow_intensity = values.get('snowIntensity', 0)
        freezing_rain_intensity = values.get('freezingRainIntensity', 0)
        temperature = values.get('temperature', 0)
        uv_index = values.get('uvIndex', 0)
        visibility = values.get('visibility', 100)
        pressure_sea_level = values.get('pressureSeaLevel', 1013)

        alerts = []
        
        parts = [p.strip() for p in location.split(",")]
        
        city = parts[0]
        state = parts[-3]

        location = f"{city}, {state}"

        if precipitation_probability >= PRECIPITATION_PROBABILITY_THRESHOLD:
            alerts.append(f"🌧 Alta probabilidade de chuva em {location} ({precipitation_probability}%) — leve seu guarda-chuva!")
        if wind_speed >= WIND_SPEED_THRESHOLD:
            alerts.append(f"💨 Ventos fortes esperados em {location} ({wind_speed} km/h) — cuidado ao sair de casa.")
        if wind_gust >= WIND_GUST_THRESHOLD:
            alerts.append(f"🌬 Rajadas de vento intensas em {location} ({wind_gust} km/h) — risco de quedas de galhos e objetos.")
        if rain_intensity >= RAIN_INTENSITY_THRESHOLD:
            alerts.append(f"🌧 Chuva intensa prevista em {location} ({rain_intensity} mm/h) — alagamentos podem ocorrer.")
        if snow_intensity >= SNOW_INTENSITY_THRESHOLD:
            alerts.append(f"❄️ Nevasca em potencial em {location} ({snow_intensity} mm/h) — dirija com cautela.")
        if freezing_rain_intensity > 0:
            alerts.append(f"🧊 Chuva congelante detectada em {location} ({freezing_rain_intensity} mm/h) — superfícies escorregadias.")
        if temperature >= HIGH_TEMP_THRESHOLD:
            alerts.append(f"🔥 Calor extremo em {location} ({temperature}°C) — hidrate-se e evite exposição solar prolongada.")
        if temperature <= LOW_TEMP_THRESHOLD:
            alerts.append(f"🥶 Frio intenso em {location} ({temperature}°C) — vista-se adequadamente para evitar hipotermia.")
        if uv_index >= UV_INDEX_THRESHOLD:
            alerts.append(f"🌞 Índice UV muito alto em {location} ({uv_index}) — use protetor solar e evite o sol ao meio-dia.")
        if visibility <= LOW_VISIBILITY_THRESHOLD:
            alerts.append(f"🌫 Baixa visibilidade em {location} ({visibility} km) — cuidado ao dirigir.")
        if pressure_sea_level <= LOW_PRESSURE_THRESHOLD:
            alerts.append(f"⚠️ Pressão atmosférica baixa em {location} ({pressure_sea_level} hPa) — pode indicar mudanças bruscas no clima.")

        if alerts:
            message = "⚠️ Alerta de Evento Climático Extremo:\n\n" + "\n".join(alerts)
            response = sns_client.publish(
                TopicArn=SNS_TOPIC_ARN,
                Message=message,
                Subject='⚠️ Alerta de Evento Climático Extremo'
            )
            print(f"SNS response: {response}")
        else:
            print("Alerta não enviado")

    return {
        'statusCode': 200,
        'body': json.dumps('Processed Kinesis records and sent to SNS if thresholds exceeded')
    }