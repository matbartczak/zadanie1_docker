import openmeteo_requests
import requests_cache
from retry_requests import retry
import pandas as pd
from datetime import datetime
from opensearchpy import OpenSearch, helpers
import time

df = pd.read_csv("europe_cities.csv")

lats = df["lat"].to_list()
lngs = df['lng'].to_list()
ids = df.loc[:, ['id','city','country']].values.tolist()
print("APP STARTING...")
time.sleep(60)
while True:
   
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)


    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lats,
        "longitude": lngs,
        "current": ["temperature_2m", "relative_humidity_2m", "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m", "cloud_cover", "surface_pressure"],
        "timezone": "Europe/Berlin",
        
    }
    responses = openmeteo.weather_api(url, params=params)

    temp = dict()
    result = list()

    for id,response,lat,lng in zip(ids,responses,lats, lngs):

        current = response.Current()
        temp = dict()
        temp_datetime_id = int(datetime.fromtimestamp(current.Time()).strftime('%Y%m%d%H%M%S'))
        temp["city"] = id[1]
        temp["country"] = id[2]
        temp['location'] = [lng, lat]
        temp["datetime_id"] = temp_datetime_id
        temp["temperature"] = round(current.Variables(0).Value(), 2) 
        temp["humidity"] = current.Variables(1).Value()
        temp["wind_speed"] =  current.Variables(2).Value()
        temp["wind_direction"] = current.Variables(3).Value()
        temp["wind_gusts"] = current.Variables(4).Value()
        temp["cloud_cover"] = current.Variables(5).Value()
        temp["surface_pressure"] = round(current.Variables(6).Value(), 2) 
        result.append( (f"{str(id[0])}_{str(temp_datetime_id)}", temp) ) #tuple(id, dict)


    host = 'opensearch'
    port = 9200

    # Create the client with SSL/TLS and hostname verification disabled.
    client = OpenSearch(
        hosts = [{'host': host, 'port': port, 'scheme': 'https'}],
        http_compress = True, # enables gzip compression for request bodies
        use_ssl = True,
        verify_certs = False,
        ssl_assert_hostname = False,
        ssl_show_warn = False,
        http_auth=('admin', 'QWERTYadmin123!@#'),  # login/admin password
    )

    actions = [
        {
            '_op_type': 'update',
            '_index': 'python-weather5-index',
            '_id': res[0],                    # document id
            'doc': res[1],
            'doc_as_upsert': True          # if doc doesn't exist -> create
        } for res in result
    ]

    success, failed = helpers.bulk(client, actions, refresh=True)
    print(f"Success: {success}, Failed: {failed}")
    print(f"[INFO] Script executed at {datetime.now()}")
    time.sleep(900)