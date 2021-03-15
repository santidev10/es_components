import certifi

from elasticsearch_dsl import connections

from elasticsearch import RequestsHttpConnection
from requests_aws4auth import AWS4Auth

from es_components.config import ELASTIC_SEARCH_URLS
from es_components.config import ELASTIC_SEARCH_TIMEOUT
from es_components.config import ELASTIC_SEARCH_USE_SSL
from es_components.config import ELASTIC_SEARCH_VERIFY_CERTS
from es_components.config import AWS_ES_ACCESS_KEY_ID
from es_components.config import AWS_ES_SECRET_ACCESS_KEY


def init_es_connection():
    connections.configure(
        default={
            "hosts": ELASTIC_SEARCH_URLS,
            "timeout": ELASTIC_SEARCH_TIMEOUT,
            "use_ssl": ELASTIC_SEARCH_USE_SSL,
            "ca_certs": certifi.where(),
            "http_auth": AWS4Auth(AWS_ES_ACCESS_KEY_ID, AWS_ES_SECRET_ACCESS_KEY, "us-east-1", "es"),
            "verify_certs": ELASTIC_SEARCH_VERIFY_CERTS,
            "connection_class": RequestsHttpConnection
        }
    )
