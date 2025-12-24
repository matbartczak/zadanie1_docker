import openmeteo_requests
import requests_cache
from retry_requests import retry
from datetime import datetime
from opensearchpy import OpenSearch, helpers
import time
import os
import sys
import threading
import socket

# ---------------- CONFIG ----------------

HEARTBEAT = "/tmp/heartbeat"
WATCHDOG_TIMEOUT = 1800      # 30 minutes
SLEEP_INTERVAL = 900         # 15 minutes
SOCKET_TIMEOUT = 20          # seconds

# ----------------------------------------

socket.setdefaulttimeout(SOCKET_TIMEOUT)


def touch_heartbeat():
    with open(HEARTBEAT, "w") as f:
        f.write(str(time.time()))


def watchdog():
    """Kills the process if heartbeat is stale"""
    while True:
        time.sleep(30)
        if not os.path.exists(HEARTBEAT):
            continue

        age = time.time() - os.path.getmtime(HEARTBEAT)
        if age > WATCHDOG_TIMEOUT:
            print(f"WATCHDOG: heartbeat stale ({age:.0f}s). Exiting.")
            sys.exit(1)   # <-- Docker restart trigger


threading.Thread(target=watchdog, daemon=True).start()


def detect_weather_anomaly(record: dict) -> dict:
    anomalies = []

    temp = record["temperature"]
    wind = record["wind_speed"]
    gusts = record["wind_gusts"]
    pressure = record["surface_pressure"]
    humidity = record["humidity"]
    clouds = record["cloud_cover"]

    if temp > 35:
        anomalies.append("HIGH_TEMPERATURE")
    elif temp < -25:
        anomalies.append("LOW_TEMPERATURE")

    if wind > 20:
        anomalies.append("STRONG_WIND")
    if gusts > 30:
        anomalies.append("STRONG_WIND_GUSTS")

    if pressure < 980:
        anomalies.append("LOW_PRESSURE")
    elif pressure > 1040:
        anomalies.append("HIGH_PRESSURE")

    if clouds == 0 and humidity > 90:
        anomalies.append("INCONSISTENT_CLOUD_HUMIDITY")

    record["anomaly"] = bool(anomalies)
    record["anomaly_types"] = anomalies
    return record


print("APP STARTING...")
touch_heartbeat()
time.sleep(60)

while True:

    touch_heartbeat()

    client = OpenSearch(
        hosts=[{"host": "opensearch", "port": 9200, "scheme": "https"}],
        http_compress=True,
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False,
        http_auth=("admin", "QWERTYadmin123!@#"),
        timeout=10,
        max_retries=3,
        retry_on_timeout=True,
    )

    query = {"size": 500, "query": {"match_all": {}}}
    resp = client.search(index="python-cities-index", body=query)
    city_doc = resp["hits"]["hits"]

    ids = [
        {
            "id": c["_source"]["id"],
            "city": c["_source"]["city"],
            "country": c["_source"]["country"],
            "location": c["_source"]["location"],
        }
        for c in city_doc
    ]

    lats = [c["location"][1] for c in ids]
    lngs = [c["location"][0] for c in ids]

    touch_heartbeat()

    cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    responses = openmeteo.weather_api(
        "https://api.open-meteo.com/v1/forecast",
        {
            "latitude": lats,
            "longitude": lngs,
            "current": [
                "temperature_2m",
                "relative_humidity_2m",
                "wind_speed_10m",
                "wind_direction_10m",
                "wind_gusts_10m",
                "cloud_cover",
                "surface_pressure",
            ],
            "timezone": "Europe/Berlin",
        },
    )

    touch_heartbeat()

    result = []
    for meta, response in zip(ids, responses):
        current = response.Current()
        ts = int(datetime.fromtimestamp(current.Time()).strftime("%Y%m%d%H%M%S"))

        data = {
            "city": meta["city"],
            "country": meta["country"],
            "location": meta["location"],
            "datetime_id": ts,
            "temperature": current.Variables(0).Value(),
            "humidity": current.Variables(1).Value(),
            "wind_speed": current.Variables(2).Value(),
            "wind_direction": current.Variables(3).Value(),
            "wind_gusts": current.Variables(4).Value(),
            "cloud_cover": current.Variables(5).Value(),
            "surface_pressure": current.Variables(6).Value(),
        }

        result.append(
            (f"{meta['id']}_{ts}", detect_weather_anomaly(data))
        )

    actions = [
        {
            "_op_type": "update",
            "_index": "python-weather5-index",
            "_id": doc_id,
            "doc": doc,
            "doc_as_upsert": True,
        }
        for doc_id, doc in result
    ]

    helpers.bulk(client, actions, refresh=False, request_timeout=30)

    print(f"[INFO] Executed at {datetime.now()} | docs: {len(actions)}")

    # ---- SAFE SLEEP ----
    end = time.time() + SLEEP_INTERVAL
    while time.time() < end:
        touch_heartbeat()
        time.sleep(5)
