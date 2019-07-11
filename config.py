import os

CHANNEL_INDEX_NAME = "channels"
CHANNEL_DOC_TYPE = "channel"

VIDEO_INDEX_NAME = "videos"
VIDEO_DOC_TYPE = "video"

KEYWORD_INDEX_NAME = "keywords"
KEYWORD_DOC_TYPE = "keyword"

ES_REQUEST_LIMIT = os.getenv("ES_REQUEST_LIMIT", 10000)
ES_CHUNK_SIZE = os.getenv("ES_CHUNK_SIZE", 500)
ES_BULK_REFRESH_OPTION = os.getenv("ES_BULK_REFRESH_OPTION", "wait_for")
