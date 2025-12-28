import openmeteo_requests
import requests_cache
from retry_requests import retry

from datetime import datetime
from opensearchpy import OpenSearch, helpers

host = 'localhost'
port = 9200

# Create the client with SSL/TLS and hostname verification disabled.
client = OpenSearch(
    hosts = [{'host': host, 'port': port}],
    http_compress = True, # enables gzip compression for request bodies
    use_ssl = True,
    verify_certs = False,
    ssl_assert_hostname = False,
    ssl_show_warn = False,
    http_auth=('admin', 'QWERTYadmin123!@#'),  # login/admin password
)

query = {
  "size": 1,
  "query": {
    "bool": {
      "filter": [
        { "term": { "city": "Łódź" } }
      ]
    }
  },
  "sort": [
    { "datetime_id": { "order": "desc" } }
  ]
}


resp = client.search(index="python-weather5-index", body=query)
print(resp["hits"]["hits"])