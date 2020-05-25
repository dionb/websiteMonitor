import requests
import yaml
from kafka import KafkaProducer
import json
import time
import re
import urllib3

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

# print(topic)

producer = {}
try:
    producer = KafkaProducer(bootstrap_servers=config["kafka"]["url"])
except Exception as e:
    print("'kafka.url' key not found in config ")
    print(e)
    exit(2)

try:
    for site in config['websites']:
        if not site.startswith("http"):
            site = "https://" + site

        regex = ""
        if " " in site:
            parts = site.split(" ")
            site = parts[0]
            regex = parts[1]

        try:
            result = requests.get(site)
        except requests.exceptions.ConnectionError as e:
            print("unable to connect to url: " + site)
            print(e)
            continue

        if regex != "":
            m = re.search(regex, result.text)
            if m is None:
                result.status_code = 600  # set to a non-existant error code

        print(site + " " + str(result.status_code))
        jsonbytes = json.dumps({
                               "url": site,
                               "status": result.status_code,
                               "time": time.time()
                               }).encode('utf-8')
        producer.send(topic, jsonbytes)
except Exception as e:
    print(e)
finally:
    producer.flush()
