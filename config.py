import os

from .datetime_service import datetime_service


date = datetime_service.now().strftime("%Y%m%d")

CHANNEL_INDEX_NAME = f"channels_{date}"
CHANNEL_INDEX_PREFIX = "channels_"
CHANNEL_DOC_TYPE = "channel"

VIDEO_INDEX_NAME = f"videos_{date}"
VIDEO_INDEX_PREFIX = "videos_"
VIDEO_DOC_TYPE = "video"

KEYWORD_INDEX_NAME = f"keywords_{date}"
KEYWORD_INDEX_PREFIX = "keywords_"
KEYWORD_DOC_TYPE = "keyword"

ES_REQUEST_LIMIT = int(os.getenv("ES_REQUEST_LIMIT", "10000"))
ES_CHUNK_SIZE = int(os.getenv("ES_CHUNK_SIZE", "500"))
ES_BULK_REFRESH_OPTION = os.getenv("ES_BULK_REFRESH_OPTION", "wait_for")

ELASTIC_SEARCH_URLS = os.getenv("ELASTIC_SEARCH_URLS", "").split(",")
ELASTIC_SEARCH_TIMEOUT = int(os.getenv("ELASTIC_SEARCH_TIMEOUT", "300"))
ELASTIC_SEARCH_USE_SSL = os.getenv("ELASTIC_SEARCH_USE_SSL", "1") == "1"
