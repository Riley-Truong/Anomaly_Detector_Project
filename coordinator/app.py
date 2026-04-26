import json
import boto3
import urllib.request
import urllib.parse
import time
from datetime import datetime

lambda_client = boto3.client('lambda', region_name='us-east-2')
dynamodb      = boto3.resource('dynamodb', region_name='us-east-2')
timing_table  = dynamodb.Table('AD_Time_Results')
s3_client     = boto3.client('s3', region_name='us-east-2')
S3_BUCKET     = 'anomaly-detector-dashboard'

ATLANTA_LAT   = 33.7490
ATLANTA_LON   = -84.3880
SEARCH_RADIUS = 50000
CITY_LIMIT    = 30

# Each worker handles a different source AND a different slice of cities
WORKER_ASSIGNMENTS = [
    {
        'function': 'AnomalyDetectorWorkerForecast',
        'source':   'forecast',
        'slice':    (0, 10)     # cities index 0 through 9
    },
    {
        'function': 'AnomalyDetectorWorkerArchive',
        'source':   'archive',
        'slice':    (10, 20)    # cities index 10 through 19
    },
    {
        'function': 'AnomalyDetectorWorkerNWS',
        'source':   'nws',
        'slice':    (20, 30)    # cities index 20 through 29
    },
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
            url, headers={'User-Agent': 'AnomalyDetector/2.0 (student-project)'}
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
        print(f"Overpass API failed: {e}. Using fallback.")
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
    trigger_source = event.get('source', 'manual')
    today          = datetime.utcnow().strftime('%Y-%m-%d')
    start_ms       = int(time.time() * 1000)

    print(f"Triggered by: {trigger_source}")
    print(f"Starting parallel run for {today}")

    all_cities = get_cities_near_atlanta()

    if len(all_cities) < 30:
        print(f"Warning: only got {len(all_cities)} cities, padding with fallback")
        all_cities = get_fallback_cities()

    launched = 0

    # Fire all three workers simultaneously
    # Each worker gets its own slice of cities AND its own data source
    for assignment in WORKER_ASSIGNMENTS:
        start_idx = assignment['slice'][0]
        end_idx   = assignment['slice'][1]
        city_slice = all_cities[start_idx:end_idx]

        payload = {
            'cities': city_slice,
            'source': assignment['source'],
            'date':   today
        }

        try:
            lambda_client.invoke(
                FunctionName   = assignment['function'],
                InvocationType = 'Event',   # async = all fire at the same time
                Payload        = json.dumps(payload).encode('utf-8')
            )
            launched += 1
            print(f"Launched {assignment['function']} with {len(city_slice)} cities ({assignment['source']})")
        except Exception as e:
            print(f"Failed to launch {assignment['function']}: {e}")

    duration_ms = int(time.time() * 1000) - start_ms

    # Save timing to DynamoDB
    try:
        timing_table.put_item(Item={
            'runID':             f"{today}#parallel",
            'src':               'coordinator_dispatch',
            'date':              today,
            'mode':              'parallel',
            'workers':           str(launched),
            'cities_per_worker': '10',
            'total_cities':      str(len(all_cities)),
            'duration_ms':       str(duration_ms),
            'trigger':           trigger_source,
        })
    except Exception as e:
        print(f"Could not save timing: {e}")

    # Save timing to S3 for dashboard
    try:
        timing_data = {
            'mode':              'parallel',
            'date':              today,
            'dispatch_ms':       duration_ms,
            'workers':           launched,
            'cities_per_worker': 10,
            'total_cities':      len(all_cities),
            'trigger':           trigger_source,
        }
        s3_client.put_object(
            Bucket      = S3_BUCKET,
            Key         = f"timing/{today}_parallel.json",
            Body        = json.dumps(timing_data).encode('utf-8'),
            ContentType = 'application/json',
        )
        print(f"Wrote parallel timing to S3")
    except Exception as e:
        print(f"Could not write to S3: {e}")

    print(f"Dispatched {launched} workers in {duration_ms}ms")
    return {
        'statusCode':  200,
        'launched':    launched,
        'dispatch_ms': duration_ms
    }