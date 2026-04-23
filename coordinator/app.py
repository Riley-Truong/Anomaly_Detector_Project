import json
import boto3
import os
import urllib.request
import urllib.parse
import time
 
lambda_client = boto3.client('lambda', region_name='us-east-2')
WORKER_FUNCTION_NAME = 'AnomalyDetectorWorker'
 
def get_cities_in_radius(lat, lon, radius_meters=50000, limit=30):
    """Fetches dynamic cities around a target coordinate using OpenStreetMap."""
    
    # Overpass Query: Find nodes (places) around our radius that are marked as cities or towns
    query = f"""
    [out:json];
    node(around:{radius_meters},{lat},{lon})["place"~"city|town"];
    out {limit};
    """
    
    # Format the URL safely
    url = "https://overpass-api.de/api/interpreter?data=" + urllib.parse.quote(query.strip())
    
    try:
        # Overpass requires a User-Agent header, otherwise they might block the request
        req = urllib.request.Request(url, headers={'User-Agent': 'AWS-Lambda-Weather-App/1.0'})
        
        with urllib.request.urlopen(req, timeout=10) as response:
            data = json.loads(response.read().decode('utf-8'))
            
        cities = []
        for element in data.get('elements', []):
            if 'tags' in element and 'name' in element['tags']:
                cities.append({
                    "name": element['tags']['name'],
                    "lat": element['lat'],
                    "lon": element['lon']
                })
        return cities
        
    except Exception as e:
        print(f"Failed to fetch cities from Overpass API: {e}")
        return []

def lambda_handler(event, context):
    target_lat = 33.7490
    target_lon = -84.3880

    print("Fetching dynamic city list...")
    cities = get_cities_in_radius(target_lat, target_lon)
    
    if not cities:
        return {
            'statusCode': 500,
            'message': 'Failed to fetch cities. Check logs.'
        }

    print(f'Launching {len(cities)} workers in parallel...')
    launched = 0
 
    for city in cities:
        try:
            lambda_client.invoke(
                FunctionName = WORKER_FUNCTION_NAME,
                InvocationType = 'Event',
                Payload = json.dumps(city).encode('utf-8')
            )
            launched += 1
            time.sleep(0.5)
        except Exception as e:
            print(f'Failed to launch worker for {city["name"]}: {e}')
 
    print(f'Successfully launched {launched} of {len(cities)} workers.')
    return {
        'statusCode': 200,
        'message': f'Launched {launched} workers'
    }
