import json, boto3, urllib.request, time 
from datetime import datetime

dynamodb = boto3.resource("dynamodb", region_name="us-east-2")
table = dynamodb.Table("Anomaly_Detector_Results")

def lambda_handler(event, context):
    city = event["name"]
    lat = event["lat"]
    lon = event["lon"]
    today = datetime.utcnow().strftime("%Y-%m-%d")
 
    start_ms = int(time.time() * 1000)
 
    url = (f"https://api.open-meteo.com/v1/forecast"
           f"?latitude={lat}&longitude={lon}"
           f"&current_weather=true"
           f"&daily=precipitation_sum,windspeed_10m_max"
           f"&timezone=America%2FNew_York&forecast_days=1")
 
    data = fetch_json(url)
    cw = data["current_weather"]
    temp_f = round((cw["temperature"] * 9/5) + 32, 1)
    wind = cw.get("windspeed", 0)
    rain = data.get("daily", {}).get("precipitation_sum", [0])[0] or 0.0
 
    end_ms      = int(time.time() * 1000)
    duration_ms = end_ms - start_ms
 
    table.put_item(Item={
        "city": city,
        "date_source": f"{today}#forecast",
        "date": today,
        "source": "forecast",
        "temp_f": str(temp_f),
        "wind_mph": str(wind),
        "rain_in": str(rain),
        "duration_ms": str(duration_ms),
    })
 
    print(f"{city} forecast: {temp_f}F | {duration_ms}ms")
    return {"city": city, "source": "forecast", "duration_ms": duration_ms}
 
def fetch_json(url):
    with urllib.request.urlopen(url, timeout=15) as r:
        return json.loads(r.read().decode("utf-8"))
