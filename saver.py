import yaml
from kafka import KafkaConsumer
import json
import psycopg2

config = []

# adapted from https://stackoverflow.com/questions/1773805/how-can-i-parse-a-yaml-file-in-python
with open("config.yaml", 'r') as stream:
    try:
        config = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print("error parsing config")
        print(exc)
        exit(1)

# print(config)

topic = ""
try:
    topic = config["kafka"]["topic"]
except Exception as e:
    print("'kafka.topic' key not found in config")
    print(e)
    exit(3)

print(topic)


consumer = {}
try:
    consumer = KafkaConsumer(topic, bootstrap_servers=config["kafka"]["url"])
except Exception as e:
    print("'kafka.url' key not found in config ")
    print(e)
    exit(2)

conn = {}
try:
    host = config["postgres"]["host"]
    port = config["postgres"]["port"]
    user = config["postgres"]["user"]
    password = config["postgres"]["password"]
    database = config["postgres"]["database"]
    # from https://www.postgresqltutorial.com/postgresql-python/connect/
    conn = psycopg2.connect(host=host, database=database, user=user, password=password)
except Exception as e:
    print("error when trying to read postgres config or connect to db:")
    print(e)
    exit(4)

consumer.subscribe([topic])
query = "INSERT INTO " + config["postgres"]["tablename"] + "(url, status, timestamp) VALUES(%s, %s, %s)"

try:
    cur = conn.cursor()
    for msg in consumer:
        data = json.loads(msg.value.decode('utf-8'))
        url = data["url"]
        status = data["status"]
        timestamp = data["time"]
        conn.execute(query, (url, status, timestamp))
finally:
    conn.close()
