class ES_DICT_FIELDS:
    DOC = "doc"
    DOC_AS_UPSERT = "doc_as_upsert"
    OP_TYPE = "_op_type"
    SOURCE = "_source"
    VERSION = "_version"


class SECTIONS:
    MAIN = "main"

    ADS_STATS = "ads_stats"
    ANALYTICS = "analytics"
    GENERAL_DATA = "general_data"
    MONETIZATION = "monetization"
    SOCIAL = "social"
    STATS = "stats"
    TRANSCRIPTS = "transcripts"
    CMS = "cms"
    CHANNEL = "channel"
    DELETED = "deleted"

    ANALYTICS_SCHEDULE = "analytics_schedule"
    GENERAL_DATA_SCHEDULE = "general_data_schedule"
    STATS_SCHEDULE = "stats_schedule"
    TRANSCRIPTS_SCHEDULE = "transcripts_schedule"


class TIMESTAMP_FIELDS:
    CREATED_AT = "created_at"
    UPDATED_AT = "updated_at"


class FILTER_OPERATORS:
    GREATER_THAN = "gt"
    LESS_THAN = "lt"


class SORT_DIRECTIONS:
    ASCENDING = "asc"
    DESCENDING = "desc"


class FILTER_INCLUDE_EMPTY:
    FIRST = "first"
    LAST = "last"
    NO = "no"

    ALL = (FIRST, LAST, NO)


MAIN_ID_FIELD = "main.id"
VIDEO_CHANNEL_ID_FIELD = "channel.id"
CONTENT_OWNER_ID_FIELD = "cms.content_owner_id"
