class EsDictFields:
    DOC = "doc"
    DOC_AS_UPSERT = "doc_as_upsert"
    OP_TYPE = "_op_type"
    SOURCE = "_source"
    VERSION = "_version"
    PROPERTIES = "properties"


class Sections:
    MAIN = "main"
    DELETED = "deleted"

    ADS_STATS = "ads_stats"
    ANALYTICS = "analytics"
    GENERAL_DATA = "general_data"
    MONETIZATION = "monetization"
    SOCIAL = "social"
    STATS = "stats"
    CAPTIONS = "captions"
    CMS = "cms"
    CHANNEL = "channel"
    CUSTOM_PROPERTIES = "custom_properties"

    ANALYTICS_SCHEDULE = "analytics_schedule"
    GENERAL_DATA_SCHEDULE = "general_data_schedule"
    STATS_SCHEDULE = "stats_schedule"
    CAPTIONS_SCHEDULE = "captions_schedule"

    SEGMENTS = "segments"


class TimestampFields:
    CREATED_AT = "created_at"
    UPDATED_AT = "updated_at"


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


FORCED_FILTER_OUDATED_DAYS = 7
MAIN_ID_FIELD = "main.id"
VIDEO_CHANNEL_ID_FIELD = "channel.id"
CONTENT_OWNER_ID_FIELD = "cms.content_owner_id"
SEGMENTS_UUID_FIELD = "segments.uuid"
