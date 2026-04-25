import json
import boto3
import urllib.request
import urllib.parse
import time
from datetime import datetime

lambda_client = boto3.client('lambda', region_name='us-east-2')
dynamodb      = boto3.resource('dynamodb', region_name='us-east-2')
timing_table  = dynamodb.Table('AD_Time_Results')

# Atlanta coordinates — center point for the city search
ATLANTA_LAT = 33.7490
ATLANTA_LON = -84.3880

# How far out to search in meters (50000 = 50km radius around Atlanta)
SEARCH_RADIUS = 50000

# How many cities to pull — keeps it manageable and within free tier
CITY_LIMIT = 30

# Worker function names — must match exactly what is in template.yaml
WORKERS = [
    'AnomalyDetectorWorkerForecast',
    'AnomalyDetectorWorkerArchive',
    'AnomalyDetectorWorkerNWS',
]


def get_cities_near_atlanta():
    """
    Calls the Overpass API (free, no account needed) to find
    cities and towns within SEARCH_RADIUS meters of Atlanta.
    Returns a list of dicts: [{"name": "Marietta", "lat": 33.95, "lon": -84.55}, ...]
    """
    query = f"""
    [out:json];
    node(around:{SEARCH_RADIUS},{ATLANTA_LAT},{ATLANTA_LON})["place"~"city|town"];
    out {CITY_LIMIT};
    """

    url = "https://overpass-api.de/api/interpreter?data=" + urllib.parse.quote(query.strip())

    try:
        req = urllib.request.Request(
            url,
            headers={'User-Agent': 'AnomalyDetector/2.0 (student-project)'}
        )
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

        print(f"Found {len(cities)} cities near Atlanta")
        return cities

    except Exception as e:
        print(f"Overpass API failed: {e}. Falling back to hardcoded Atlanta-area cities.")
        return get_fallback_cities()


def get_fallback_cities():
    """
    Hardcoded fallback list in case the Overpass API is unreachable.
    These are real Atlanta-area cities with verified coordinates.
    The coordinator uses this automatically if the API call fails.
    """
    return [
        {'name': 'Atlanta',         'lat': 33.7490, 'lon': -84.3880},
        {'name': 'Marietta',        'lat': 33.9526, 'lon': -84.5499},
        {'name': 'Roswell',         'lat': 34.0232, 'lon': -84.3616},
        {'name': 'Alpharetta',      'lat': 34.0754, 'lon': -84.2941},
        {'name': 'Decatur',         'lat': 33.7748, 'lon': -84.2963},
        {'name': 'Smyrna',          'lat': 33.8840, 'lon': -84.5144},
        {'name': 'Sandy Springs',   'lat': 33.9304, 'lon': -84.3733},
        {'name': 'Kennesaw',        'lat': 34.0234, 'lon': -84.6155},
        {'name': 'Peachtree City',  'lat': 33.3965, 'lon': -84.5949},
        {'name': 'Duluth',          'lat': 34.0032, 'lon': -84.1447},
        {'name': 'Lawrenceville',   'lat': 33.9526, 'lon': -83.9880},
        {'name': 'Woodstock',       'lat': 34.1015, 'lon': -84.5194},
        {'name': 'Canton',          'lat': 34.2368, 'lon': -84.4913},
        {'name': 'Cumming',         'lat': 34.2073, 'lon': -84.1400},
        {'name': 'Gainesville',     'lat': 34.2979, 'lon': -83.8241},
        {'name': 'Buford',          'lat': 34.1218, 'lon': -83.9913},
        {'name': 'Cartersville',    'lat': 34.1651, 'lon': -84.7996},
        {'name': 'Douglasville',    'lat': 33.7515, 'lon': -84.7477},
        {'name': 'Newnan',          'lat': 33.3807, 'lon': -84.7997},
        {'name': 'Griffin',         'lat': 33.2468, 'lon': -84.2641},
        {'name': 'McDonough',       'lat': 33.4473, 'lon': -84.1474},
        {'name': 'Stockbridge',     'lat': 33.5440, 'lon': -84.2338},
        {'name': 'Conyers',         'lat': 33.6676, 'lon': -84.0177},
        {'name': 'Covington',       'lat': 33.5968, 'lon': -83.8602},
        {'name': 'Monroe',          'lat': 33.7954, 'lon': -83.7135},
        {'name': 'Winder',          'lat': 33.9937, 'lon': -83.7196},
        {'name': 'Jasper',          'lat': 34.4676, 'lon': -84.4285},
        {'name': 'Dallas',          'lat': 33.9237, 'lon': -84.8399},
        {'name': 'Villa Rica',      'lat': 33.7315, 'lon': -84.9177},
        {'name': 'Carrollton',      'lat': 33.5801, 'lon': -85.0766},
        {'name': 'FAILED TEST', 'lat':33.5893, 'lon':-93.9043}
    ]


def lambda_handler(event, context):
    trigger_source = event.get('source', 'manual')
    today    = datetime.utcnow().strftime('%Y-%m-%d')
    start_ms = int(time.time() * 1000)

    print(f"Triggered by: {trigger_source}")
    print(f"Starting daily run for {today}")

    # Fetch Atlanta-area cities dynamically — no JSON file needed
    cities = get_cities_near_atlanta()

    if not cities:
        print("No cities found. Aborting run.")
        return {'statusCode': 500, 'message': 'No cities returned'}

    print(f"Launching {len(cities)} cities x {len(WORKERS)} workers...")
    launched = 0

    for city in cities:
        for worker_name in WORKERS:
            try:
                lambda_client.invoke(
                    FunctionName   = worker_name,
                    InvocationType = 'Event',  # async — all run simultaneously
                    Payload        = json.dumps(city).encode('utf-8')
                )
                launched += 1
            except Exception as e:
                print(f"Failed to launch {worker_name} for {city['name']}: {e}")

    duration_ms = int(time.time() * 1000) - start_ms

    try:
        timing_table.put_item(Item={
            'runID':       f"{today}#parallel",
            'src':         'coordinator_dispatch',
            'date':        today,
            'mode':        'parallel',
            'cities':      str(len(cities)),
            'workers_per_city': str(len(WORKERS)),
            'total_invocations': str(launched),
            'duration_ms': str(duration_ms),
        })
    except Exception as e:
        print(f"Could not save timing record: {e}")

    print(f"Dispatch complete: {launched} of {len(cities) * len(WORKERS)} workers launched in {duration_ms}ms")
    return {
        'statusCode':  200,
        'launched':    launched,
        'dispatch_ms': duration_ms,
        'city_count':  len(cities)
    }