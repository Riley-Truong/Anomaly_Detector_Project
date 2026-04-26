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

WORKER_ASSIGNMENTS = [
    {'function': 'AnomalyDetectorWorkerForecast', 'source': 'forecast', 'slice': (0, 10)},
    {'function': 'AnomalyDetectorWorkerArchive',  'source': 'archive',  'slice': (10, 20)},
    {'function': 'AnomalyDetectorWorkerNWS',      'source': 'nws',      'slice': (20, 30)},
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
        return cities
    except Exception as e:
        print(f"Overpass failed: {e}. Using fallback.")
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
    timings     = []

    all_cities = get_cities_near_atlanta()
    if len(all_cities) < 30:
        all_cities = get_fallback_cities()

    print(f"Sequential benchmark: {len(WORKER_ASSIGNMENTS)} workers, one at a time")

    for assignment in WORKER_ASSIGNMENTS:
        start_idx  = assignment['slice'][0]
        end_idx    = assignment['slice'][1]
        city_slice = all_cities[start_idx:end_idx]

        payload = {
            'cities': city_slice,
            'source': assignment['source'],
            'date':   today
        }

        w_start = int(time.time() * 1000)
        print(f"Starting {assignment['function']} ({assignment['source']}, {len(city_slice)} cities)...")

        try:
            # RequestResponse = wait for this worker to fully finish
            # before moving to the next one — this is what makes it sequential
            response = lambda_client.invoke(
                FunctionName   = assignment['function'],
                InvocationType = 'RequestResponse',
                Payload        = json.dumps(payload).encode('utf-8')
            )
            result = json.loads(response['Payload'].read())
            print(f"Finished {assignment['source']}: {result}")
        except Exception as e:
            print(f"Error on {assignment['function']}: {e}")
            result = {'error': str(e)}

        w_dur = int(time.time() * 1000) - w_start
        timings.append({
            'worker':   assignment['function'],
            'source':   assignment['source'],
            'cities':   len(city_slice),
            'duration_ms': w_dur
        })
        print(f"{assignment['source']} completed in {w_dur}ms")

    total_ms = int(time.time() * 1000) - total_start
    print(f"Sequential TOTAL: {total_ms}ms")

    # Save to DynamoDB
    try:
        timing_table.put_item(Item={
            'runID':             f"{today}#sequential",
            'src':               'sequential_total',
            'date':              today,
            'mode':              'sequential',
            'workers':           str(len(WORKER_ASSIGNMENTS)),
            'cities_per_worker': '10',
            'total_cities':      '30',
            'duration_ms':       str(total_ms),
            'breakdown':         json.dumps(timings),
        })
    except Exception as e:
        print(f"Could not save to DynamoDB: {e}")

    # Save to S3 for dashboard
    try:
        timing_data = {
            'mode':              'sequential',
            'date':              today,
            'total_ms':          total_ms,
            'workers':           len(WORKER_ASSIGNMENTS),
            'cities_per_worker': 10,
            'total_cities':      30,
            'timings':           timings,
        }
        s3_client.put_object(
            Bucket      = S3_BUCKET,
            Key         = f"timing/{today}_sequential.json",
            Body        = json.dumps(timing_data).encode('utf-8'),
            ContentType = 'application/json',
        )
        print(f"Wrote sequential timing to S3")
    except Exception as e:
        print(f"Could not write to S3: {e}")

    return {
        'statusCode':  200,
        'total_ms':    total_ms,
        'timings':     timings
    }