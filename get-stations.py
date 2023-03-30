import json
import time
import urllib.request

from kafka import KafkaProducer

API_KEY = "XXX"  # Se créer un compte pour obtenir une clé API
url = "https://api.jcdecaux.com/vls/v1/stations?apiKey={}".format(API_KEY)

producer = KafkaProducer(bootstrap_servers="localhost:9092")

empty_stations = {}

while True:
    response = urllib.request.urlopen(url)
    stations = json.loads(response.read().decode())

    for station in stations:
        producer.send("velib-stations", json.dumps(station).encode(), key=str(station["number"]).encode())

        station_id = station['number']
        num_bikes_available = station['available_bikes']
        station_was_empty = empty_stations.get(station_id, False)

        if num_bikes_available == 0 and not station_was_empty:
            empty_stations[station_id] = True
            message = {
                'station_id': station_id,
                'address': station['address'],
                'city': station['contract_name'],
                'is_empty': True
            }
            producer.send('empty-stations', json.dumps(message).encode())

        elif num_bikes_available > 0 and station_was_empty:
            empty_stations[station_id] = False
            message = {
                'station_id': station_id,
                'address': station['address'],
                'city': station['contract_name'],
                'is_empty': False
            }
            producer.send('empty-stations', json.dumps(message).encode())

    print("Produced {} station records".format(len(stations)))
    time.sleep(1)
