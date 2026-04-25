import json, boto3, urllib.request, time
from datetime import datetime
 
dynamodb = boto3.resource("dynamodb", region_name="us-east-2")
table= dynamodb.Table("Anomaly_Detector_Results")
 
HEADERS = {"User-Agent": "AnomalyDetector/2.0 (student-project)"}
 
def lambda_handler(event, context):
    city= event["name"]
    lat   = event["lat"]
    lon   = event["lon"]
    today = datetime.utcnow().strftime("%Y-%m-%d")
 
    start_ms    = int(time.time() * 1000)
    alerts      = []
    has_warning = False
    obs_temp_f  = None
 
    try:
        # Step 1: Get the forecast zone for this lat/lon
        points = fetch_json(f"https://api.weather.gov/points/{lat},{lon}")
        obs_url = points["properties"].get("observationStations", "")
 
        # Step 2: Get nearest observation station current reading
        if obs_url:
            stations   = fetch_json(obs_url)
            if stations.get("features"):
                sid        = stations["features"][0]["properties"]["stationIdentifier"]
                obs        = fetch_json(f"https://api.weather.gov/stations/{sid}/observations/latest")
                temp_c     = obs["properties"]["temperature"]["value"]
                if temp_c is not None:
                    obs_temp_f = round((temp_c * 9/5) + 32, 1)
 
        # Step 3: Check for active alerts in the area
        alert_data = fetch_json(f"https://api.weather.gov/alerts/active?point={lat},{lon}")
        for feature in alert_data.get("features", []):
            event_name = feature["properties"].get("event", "")
            alerts.append(event_name)
            if any(w in event_name.lower() for w in ["warning", "advisory", "watch"]):
                has_warning = True
 
    except Exception as e:
        print(f"NWS error for {city}: {e}")
 
    duration_ms = int(time.time() * 1000) - start_ms
 
    table.put_item(Item={
        "city":        city,
        "date_source": f"{today}#nws",
        "date":        today,
        "source":      "nws",
        "obs_temp_f":  str(obs_temp_f) if obs_temp_f else "N/A",
        "alerts":      json.dumps(alerts),
        "has_warning": has_warning,
        "duration_ms": str(duration_ms),
    })
 
    print(f"{city} NWS: {obs_temp_f}F, {len(alerts)} alerts | {duration_ms}ms")
    return {"city": city, "source": "nws", "duration_ms": duration_ms}
 
def fetch_json(url):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode("utf-8"))
