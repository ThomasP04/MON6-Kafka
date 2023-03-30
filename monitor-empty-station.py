import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'empty-stations',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

empty_stations = {}

for message in consumer:
    station_id = message.value['station_id']
    city = message.value['city']

    empty_stations[city] = empty_stations.get(city, 0) + (1 if message.value['is_empty'] else -1)

    if message.value['is_empty']:
        address = message.value['address']
        empty_count = empty_stations[city]
        print(f'La station {station_id} ({address}) à {city} est vide. Il y a {empty_count} stations vides à {city}.')
    elif not message.value['is_empty']:
        address = message.value['address']
        empty_count = empty_stations[city]
        print(f'La station {station_id} ({address}) à {city} est disponible. Il y a {empty_count} stations vides à {city}.')
