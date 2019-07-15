class EsDictFields:
    DOC = "doc"
    DOC_AS_UPSERT = "doc_as_upsert"
    OP_TYPE = "_op_type"
    SOURCE = "_source"
    VERSION = "_version"


class Sections:
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

    ANALYTICS_SCHEDULE = "analytics_schedule"
    GENERAL_DATA_SCHEDULE = "general_data_schedule"
    STATS_SCHEDULE = "stats_schedule"
    TRANSCRIPTS_SCHEDULE = "transcripts_schedule"


class TimestampFields:
    CREATED_AT = "created_at"
    UPDATED_AT = "updated_at"


class FilterOperators:
    GREATER_THAN = "gt"
    GREATER_EQUAL_THAN = "gte"
    LESS_THAN = "lt"
    LESS_EQUAL_THAN = "lte"


class SortDirections:
    ASCENDING = "asc"
    DESCENDING = "desc"


class FilterIncludeEmpty:
    FIRST = "first"
    LAST = "last"
    # pylint: disable=invalid-name
    NO = "no"
    # pylint: enable=invalid-name

    ALL = (FIRST, LAST, NO)


MAIN_ID_FIELD = "main.id"
VIDEO_CHANNEL_ID_FIELD = "channel.id"
CONTENT_OWNER_ID_FIELD = "cms.content_owner_id"
