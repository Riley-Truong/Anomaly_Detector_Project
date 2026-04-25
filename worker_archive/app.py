import json, boto3, urllib.request, time
from datetime import datetime, timedelta
 
dynamodb = boto3.resource("dynamodb", region_name="us-east-2")
table = dynamodb.Table("Anomaly_Detector_Results")
 
def lambda_handler(event, context):
    city = event["name"]
    lat = event["lat"]
    lon = event["lon"]
    today = datetime.utcnow().strftime("%Y-%m-%d")
    now = datetime.utcnow()
 
    start_ms = int(time.time() * 1000)
    temps, rains = [], []
 
    for years_back in range(1, 6):
        try:
            past = now.replace(year=now.year - years_back)
            start = (past - timedelta(days=3)).strftime("%Y-%m-%d")
            end = (past + timedelta(days=3)).strftime("%Y-%m-%d")
            url = (f"https://archive-api.open-meteo.com/v1/archive"
                     f"?latitude={lat}&longitude={lon}"
                     f"&start_date={start}&end_date={end}"
                     f"&daily=temperature_2m_max,precipitation_sum"
                     f"&timezone=America%2FNew_York")
            data  = fetch_json(url)
            for t in data.get("daily", {}).get("temperature_2m_max", []):
                if t: temps.append(round((t * 9/5) + 32, 1))
            for r in data.get("daily", {}).get("precipitation_sum", []):
                if r is not None: rains.append(r)
        except Exception as e:
            print(f"Warning year -{years_back}: {e}")
 
    avg_temp = round(sum(temps)/len(temps), 1) if temps else 0.0
    avg_rain = round(sum(rains)/len(rains), 2) if rains else 0.0
    duration_ms = int(time.time() * 1000) - start_ms
 
    table.put_item(Item={
        "city": city,
        "date_source": f"{today}#archive",
        "date": today,
        "source": "archive",
        "avg_temp_f": str(avg_temp),
        "avg_rain_in": str(avg_rain),
        "duration_ms": str(duration_ms),
    })
 
    print(f"{city} archive avg: {avg_temp}F | {duration_ms}ms")
    return {"city": city, "source": "archive", "duration_ms": duration_ms}
 
def fetch_json(url):
    with urllib.request.urlopen(url, timeout=15) as r:
        return json.loads(r.read().decode("utf-8"))
