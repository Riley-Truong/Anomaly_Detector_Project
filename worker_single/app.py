import json
import boto3
import urllib.request
import urllib.parse
import time
from datetime import datetime, timedelta

dynamodb      = boto3.resource('dynamodb', region_name='us-east-2')
table         = dynamodb.Table('Anomaly_Detector_Results')
s3_client     = boto3.client('s3', region_name='us-east-2')
timing_table  = dynamodb.Table('AD_Time_Results')
S3_BUCKET     = 'anomaly-detector-dashboard'

ATLANTA_LAT   = 33.7490
ATLANTA_LON   = -84.3880
SEARCH_RADIUS = 50000
CITY_LIMIT    = 30

HEADERS = {'User-Agent': 'AnomalyDetector/2.0 (student-project)'}


def lambda_handler(event, context):
    today      = datetime.utcnow().strftime('%Y-%m-%d')
    total_start = int(time.time() * 1000)

    print(f"SingleWorker: processing all 30 cities with 3 different sources...")

    cities = get_cities_near_atlanta()
    if len(cities) < 30:
        cities = get_fallback_cities()

    # Split cities into three groups — same assignment as the 3-worker system
    group_a = cities[0:10]   # forecast
    group_b = cities[10:20]  # archive
    group_c = cities[20:30]  # nws

    segment_times = []

    # ── Segment A: forecast (cities 1-10) ──────────────────────────
    seg_start = int(time.time() * 1000)
    for city in group_a:
        try:
            url = (f"https://api.open-meteo.com/v1/forecast"
                   f"?latitude={city['lat']}&longitude={city['lon']}"
                   f"&current_weather=true"
                   f"&daily=precipitation_sum"
                   f"&timezone=America%2FNew_York&forecast_days=1")
            data   = fetch_json(url)
            temp_c = data['current_weather']['temperature']
            temp_f = round((temp_c * 9/5) + 32, 1)
            rain   = data.get('daily', {}).get('precipitation_sum', [0])[0] or 0.0
            table.put_item(Item={
                'city':        city['name'],
                'date':        today,
                'source':      'forecast',
                'temp_f':      str(temp_f),
                'rain_in':     str(rain),
                'duration_ms': str(int(time.time() * 1000) - seg_start),
            })
            print(f"Forecast: {city['name']} = {temp_f}F")
        except Exception as e:
            print(f"Forecast error {city['name']}: {e}")

    seg_a_ms = int(time.time() * 1000) - seg_start
    segment_times.append({'source': 'forecast', 'cities': 10, 'duration_ms': seg_a_ms})
    print(f"Segment A (forecast) done: {seg_a_ms}ms")

    # ── Segment B: archive (cities 11-20) ──────────────────────────
    seg_start = int(time.time() * 1000)
    for city in group_b:
        try:
            now   = datetime.utcnow()
            temps = []
            rains = []
            for years_back in range(1, 6):
                past  = now.replace(year=now.year - years_back)
                start = (past - timedelta(days=3)).strftime('%Y-%m-%d')
                end   = (past + timedelta(days=3)).strftime('%Y-%m-%d')
                url   = (f"https://archive-api.open-meteo.com/v1/archive"
                         f"?latitude={city['lat']}&longitude={city['lon']}"
                         f"&start_date={start}&end_date={end}"
                         f"&daily=temperature_2m_max,precipitation_sum"
                         f"&timezone=America%2FNew_York")
                data  = fetch_json(url)
                for t in data.get('daily', {}).get('temperature_2m_max', []):
                    if t: temps.append(round((t * 9/5) + 32, 1))
                for r in data.get('daily', {}).get('precipitation_sum', []):
                    if r is not None: rains.append(r)
            avg_temp = round(sum(temps)/len(temps), 1) if temps else 0.0
            avg_rain = round(sum(rains)/len(rains), 2) if rains else 0.0
            table.put_item(Item={
                'city':        city['name'],
                'date':        today,
                'source':      'archive',
                'avg_temp_f':  str(avg_temp),
                'avg_rain_in': str(avg_rain),
                'duration_ms': str(int(time.time() * 1000) - seg_start),
            })
            print(f"Archive: {city['name']} avg={avg_temp}F")
        except Exception as e:
            print(f"Archive error {city['name']}: {e}")

    seg_b_ms = int(time.time() * 1000) - seg_start
    segment_times.append({'source': 'archive', 'cities': 10, 'duration_ms': seg_b_ms})
    print(f"Segment B (archive) done: {seg_b_ms}ms")

    # ── Segment C: NWS (cities 21-30) ──────────────────────────────
    seg_start = int(time.time() * 1000)
    for city in group_c:
        obs_temp_f  = None
        alerts      = []
        has_warning = False
        try:
            points  = fetch_json(f"https://api.weather.gov/points/{city['lat']},{city['lon']}")
            obs_url = points['properties'].get('observationStations', '')
            if obs_url:
                stations = fetch_json(obs_url)
                if stations.get('features'):
                    sid    = stations['features'][0]['properties']['stationIdentifier']
                    obs    = fetch_json(f"https://api.weather.gov/stations/{sid}/observations/latest")
                    temp_c = obs['properties']['temperature']['value']
                    if temp_c is not None:
                        obs_temp_f = round((temp_c * 9/5) + 32, 1)
            alert_data = fetch_json(f"https://api.weather.gov/alerts/active?point={city['lat']},{city['lon']}")
            for feature in alert_data.get('features', []):
                event_name = feature['properties'].get('event', '')
                alerts.append(event_name)
                if any(w in event_name.lower() for w in ['warning', 'advisory', 'watch']):
                    has_warning = True
        except Exception as e:
            print(f"NWS error {city['name']}: {e}")
        table.put_item(Item={
            'city':        city['name'],
            'date':        today,
            'source':      'nws',
            'obs_temp_f':  str(obs_temp_f) if obs_temp_f else 'N/A',
            'alerts':      json.dumps(alerts),
            'has_warning': has_warning,
            'duration_ms': str(int(time.time() * 1000) - seg_start),
        })
        print(f"NWS: {city['name']} obs={obs_temp_f}F alerts={len(alerts)}")

    seg_c_ms = int(time.time() * 1000) - seg_start
    segment_times.append({'source': 'nws', 'cities': 10, 'duration_ms': seg_c_ms})
    print(f"Segment C (NWS) done: {seg_c_ms}ms")

    # ── Save total timing ───────────────────────────────────────────
    total_ms = int(time.time() * 1000) - total_start
    print(f"SingleWorker TOTAL: {total_ms}ms")

    try:
        timing_table.put_item(Item={
            'runID':       f"{today}#single",
            'src':         'single_worker_total',
            'date':        today,
            'mode':        'single',
            'cities':      '30',
            'workers':     '1',
            'duration_ms': str(total_ms),
            'breakdown':   json.dumps(segment_times),
        })
    except Exception as e:
        print(f"Could not save to DynamoDB: {e}")

    try:
        timing_data = {
            'mode':        'single',
            'date':        today,
            'total_ms':    total_ms,
            'workers':     1,
            'total_cities': 30,
            'timings':     segment_times,
        }
        s3_client.put_object(
            Bucket      = S3_BUCKET,
            Key         = f"timing/{today}_single.json",
            Body        = json.dumps(timing_data).encode('utf-8'),
            ContentType = 'application/json',
        )
        print(f"Wrote timing/{today}_single.json")
    except Exception as e:
        print(f"Could not write to S3: {e}")

    return {
        'statusCode': 200,
        'total_ms':   total_ms,
        'timings':    segment_times,
    }


# ── Helpers ───────────────────────────────────────────────────────

def fetch_json(url):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.loads(r.read().decode('utf-8'))


def get_cities_near_atlanta():
    query = f"""
    [out:json];
    node(around:{SEARCH_RADIUS},{ATLANTA_LAT},{ATLANTA_LON})["place"~"city|town"];
    out {CITY_LIMIT};
    """
    url = "https://overpass-api.de/api/interpreter?data=" + urllib.parse.quote(query.strip())
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=15) as response:
            data = json.loads(response.read().decode('utf-8'))
        cities = []
        for element in data.get('elements', []):
            if 'tags' in element and 'name' in element['tags']:
                cities.append({
                    'name': element['tags']['name'],
                    'lat':  element['lat'],
                    'lon':  element['lon']
                })
        return cities if cities else get_fallback_cities()
    except Exception as e:
        print(f"Overpass failed: {e}")
        return get_fallback_cities()


def get_fallback_cities():
    return [
        {'name': 'Atlanta',        'lat': 33.7490, 'lon': -84.3880},
        {'name': 'Marietta',       'lat': 33.9526, 'lon': -84.5499},
        {'name': 'Roswell',        'lat': 34.0232, 'lon': -84.3616},
        {'name': 'Alpharetta',     'lat': 34.0754, 'lon': -84.2941},
        {'name': 'Decatur',        'lat': 33.7748, 'lon': -84.2963},
        {'name': 'Smyrna',         'lat': 33.8840, 'lon': -84.5144},
        {'name': 'Sandy Springs',  'lat': 33.9304, 'lon': -84.3733},
        {'name': 'Kennesaw',       'lat': 34.0234, 'lon': -84.6155},
        {'name': 'Peachtree City', 'lat': 33.3965, 'lon': -84.5949},
        {'name': 'Duluth',         'lat': 34.0032, 'lon': -84.1447},
        {'name': 'Lawrenceville',  'lat': 33.9526, 'lon': -83.9880},
        {'name': 'Woodstock',      'lat': 34.1015, 'lon': -84.5194},
        {'name': 'Canton',         'lat': 34.2368, 'lon': -84.4913},
        {'name': 'Cumming',        'lat': 34.2073, 'lon': -84.1400},
        {'name': 'Gainesville',    'lat': 34.2979, 'lon': -83.8241},
        {'name': 'Buford',         'lat': 34.1218, 'lon': -83.9913},
        {'name': 'Cartersville',   'lat': 34.1651, 'lon': -84.7996},
        {'name': 'Douglasville',   'lat': 33.7515, 'lon': -84.7477},
        {'name': 'Newnan',         'lat': 33.3807, 'lon': -84.7997},
        {'name': 'Griffin',        'lat': 33.2468, 'lon': -84.2641},
        {'name': 'McDonough',      'lat': 33.4473, 'lon': -84.1474},
        {'name': 'Stockbridge',    'lat': 33.5440, 'lon': -84.2338},
        {'name': 'Conyers',        'lat': 33.6676, 'lon': -84.0177},
        {'name': 'Covington',      'lat': 33.5968, 'lon': -83.8602},
        {'name': 'Monroe',         'lat': 33.7954, 'lon': -83.7135},
        {'name': 'Winder',         'lat': 33.9937, 'lon': -83.7196},
        {'name': 'Jasper',         'lat': 34.4676, 'lon': -84.4285},
        {'name': 'Dallas',         'lat': 33.9237, 'lon': -84.8399},
        {'name': 'Villa Rica',     'lat': 33.7315, 'lon': -84.9177},
        {'name': 'Carrollton',     'lat': 33.5801, 'lon': -85.0766},
    ]