import urllib.request
import json
 
lat = 33.749
lon = -84.388
 
url = (f'https://api.open-meteo.com/v1/forecast'
       f'?latitude={lat}&longitude={lon}'
       f'&current_weather=true'
       f'&daily=precipitation_sum'
       f'&timezone=America%2FNew_York'
       f'&forecast_days=1')
 
with urllib.request.urlopen(url) as response:
    data = json.loads(response.read().decode())
 
temp_c = data['current_weather']['temperature']
temp_f = round((temp_c * 9/5) + 32, 1)
rain   = data['daily']['precipitation_sum'][0]
 
print(f'Atlanta today: {temp_f}F, {rain} inches of rain')
