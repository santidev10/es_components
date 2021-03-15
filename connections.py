import certifi

from elasticsearch_dsl import connections

from elasticsearch import RequestsHttpConnection
from requests_aws4auth import AWS4Auth

from es_components.config import ELASTIC_SEARCH_URLS
from es_components.config import ELASTIC_SEARCH_TIMEOUT
from es_components.config import ELASTIC_SEARCH_USE_SSL


def init_es_connection():
    connections.configure(
        default={
            "hosts": ELASTIC_SEARCH_URLS,
            "timeout": ELASTIC_SEARCH_TIMEOUT,
            "use_ssl": ELASTIC_SEARCH_USE_SSL,
            "ca_certs": certifi.where()
        }
    )
