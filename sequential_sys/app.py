import json
import boto3
import urllib.request
import urllib.parse
import time
from datetime import datetime

lambda_client = boto3.client('lambda', region_name='us-east-2')
dynamodb      = boto3.resource('dynamodb', region_name='us-east-2')
timing_table  = dynamodb.Table('AD_Time_Results')

ATLANTA_LAT   = 33.7490
ATLANTA_LON   = -84.3880
SEARCH_RADIUS = 50000
CITY_LIMIT    = 30

WORKERS = [
    'AnomalyDetectorWorkerForecast',
    'AnomalyDetectorWorkerArchive',
    'AnomalyDetectorWorkerNWS',
]


def get_cities_near_atlanta():
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
        print(f"Overpass API failed: {e}. Using fallback cities.")
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


def lambda_handler(event, context):
    today       = datetime.utcnow().strftime('%Y-%m-%d')
    total_start = int(time.time() * 1000)

    # Fetch the same 30 Atlanta-area cities as the coordinator
    # No cap — full 30 cities so the comparison is fair
    cities = get_cities_near_atlanta()

    print(f"Sequential benchmark: {len(cities)} cities x {len(WORKERS)} workers")
    print(f"Total calls to make: {len(cities) * len(WORKERS)} (one at a time)")

    completed  = 0
    failed     = 0

    for city in cities:
        for worker_name in WORKERS:
            try:
                # RequestResponse = wait for this worker to fully finish
                # before moving to the next one — this is sequential
                lambda_client.invoke(
                    FunctionName   = worker_name,
                    InvocationType = 'RequestResponse',
                    Payload        = json.dumps(city).encode('utf-8')
                )
                completed += 1
            except Exception as e:
                print(f"Error: {worker_name} for {city['name']}: {e}")
                failed += 1

            print(f"Done {completed} of {len(cities) * len(WORKERS)}: {city['name']} | {worker_name}")

    total_ms = int(time.time() * 1000) - total_start

    print(f"Sequential TOTAL: {total_ms}ms")
    print(f"Completed: {completed}  Failed: {failed}")

    # Save clean timing record — no breakdown column
    try:
        timing_table.put_item(Item={
            'runID':             f"{today}#sequential",
            'src':               'sequential_total',
            'date':              today,
            'mode':              'sequential',
            'cities':            str(len(cities)),
            'workers_per_city':  str(len(WORKERS)),
            'total_invocations': str(len(cities) * len(WORKERS)),
            'completed':         str(completed),
            'failed':            str(failed),
            'duration_ms':       str(total_ms),
        })
    except Exception as e:
        print(f"Could not save timing record: {e}")

    return {
        'statusCode':        200,
        'total_ms':          total_ms,
        'city_count':        len(cities),
        'total_invocations': len(cities) * len(WORKERS),
        'completed':         completed,
        'failed':            failed,
    }