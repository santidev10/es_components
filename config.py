import os


CHANNEL_INDEX_NAME = "channels"
CHANNEL_INDEX_PREFIX = "channels_"
CHANNEL_DOC_TYPE = "channel"

VIDEO_INDEX_NAME = "videos"
VIDEO_INDEX_PREFIX = "videos_"
VIDEO_DOC_TYPE = "video"

KEYWORD_INDEX_NAME = "keywords"
KEYWORD_INDEX_PREFIX = "keywords_"
KEYWORD_DOC_TYPE = "keyword"

TRANSCRIPT_INDEX_NAME = "transcripts"
TRANSCRIPT_INDEX_PREFIX = "transcripts_"
TRANSCRIPT_DOC_TYPE = "transcript"

VIDEO_LANGUAGE_INDEX_NAME = "videolanguage"
VIDEO_LANGUAGE_INDEX_PREFIX = "videolanguage_"
VIDEO_LANGUAGE_DOC_TYPE = "videolanguage"

ES_REQUEST_LIMIT = int(os.getenv("ES_REQUEST_LIMIT", "10000"))
# VIQ2-161: Trying to fix: circuit_breaking_exception; orig: "ES_CHUNK_SIZE", "500"
ES_CHUNK_SIZE = int(os.getenv("ES_CHUNK_SIZE", "400"))
ES_BULK_REFRESH_OPTION = os.getenv("ES_BULK_REFRESH_OPTION", "wait_for")

ES_MAX_CHUNK_BYTES = int(os.getenv("ES_MAX_CHUNK_BYTES", "10485760"))

ELASTIC_SEARCH_URLS = os.getenv("ELASTIC_SEARCH_URLS", "").split(",")
ELASTIC_SEARCH_TIMEOUT = int(os.getenv("ELASTIC_SEARCH_TIMEOUT", "300"))
ELASTIC_SEARCH_USE_SSL = os.getenv("ELASTIC_SEARCH_USE_SSL", "1") == "1"

AWS_ES_ACCESS_KEY_ID = os.getenv("AWS_ES_ACCESS_KEY_ID", "")
AWS_ES_SECRET_ACCESS_KEY = os.getenv("AWS_ES_SECRET_ACCESS_KEY", "")
