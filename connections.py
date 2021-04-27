import certifi

from elasticsearch_dsl import connections

from elasticsearch import RequestsHttpConnection
from requests_aws4auth import AWS4Auth

from es_components.config import ELASTIC_SEARCH_URLS
from es_components.config import ELASTIC_SEARCH_TIMEOUT
from es_components.config import ELASTIC_SEARCH_USE_SSL

from es_components.config import AWS_ES_ACCESS_KEY_ID
from es_components.config import AWS_ES_SECRET_ACCESS_KEY


def init_es_connection():
    es_connection_config = {
        "hosts": ELASTIC_SEARCH_URLS,
        "timeout": ELASTIC_SEARCH_TIMEOUT,
        "use_ssl": ELASTIC_SEARCH_USE_SSL,
        "ca_certs": certifi.where()
    }
    if AWS_ES_ACCESS_KEY_ID and AWS_ES_SECRET_ACCESS_KEY:
        es_connection_config["http_auth"] = AWS4Auth(AWS_ES_ACCESS_KEY_ID, AWS_ES_SECRET_ACCESS_KEY, 'us-east-1', 'es')
        es_connection_config["verify_certs"] = True
        es_connection_config["connection_class"] = RequestsHttpConnection

    connections.configure(default=es_connection_config)
