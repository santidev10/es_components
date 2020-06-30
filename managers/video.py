from collections import OrderedDict
from typing import List

from pycountry import languages

from es_components.config import ES_CHUNK_SIZE
from es_components.constants import CONTENT_OWNER_ID_FIELD
from es_components.constants import MAIN_ID_FIELD
from es_components.constants import Sections
from es_components.constants import SortDirections
from es_components.constants import TimestampFields
from es_components.constants import VIDEO_CHANNEL_ID_FIELD
from es_components.managers.base import BaseManager
from es_components.models.channel import Channel
from es_components.models.video import Video
from es_components.query_builder import QueryBuilder
from es_components.monitor import Emergency
from es_components.monitor import Warnings
from es_components.utils import add_brand_safety_labels
from es_components.utils import add_sentiment_labels
from es_components.languages import LANGUAGES


RANGE_AGGREGATION = (
    "stats.views",
    "stats.last_day_views",
    "stats.channel_subscribers",
    "ads_stats.video_view_rate",
    "ads_stats.ctr",
    "ads_stats.ctr_v",
    "ads_stats.average_cpv",
    "ads_stats.average_cpm",
    "ads_stats.video_quartile_100_rate",
    "general_data.youtube_published_at"
)

COUNT_AGGREGATION = (
    "general_data.country",
    "general_data.country_code",
    "general_data.category",
    "general_data.language",
    "general_data.lang_code",
    "general_data.iab_categories",
    "brand_safety",
    "stats.flags",
    "task_us_data.age_group",
    "task_us_data.content_type",
    "task_us_data.gender",
    "stats.sentiment"
)

COUNT_EXISTS_AGGREGATION = ("stats.flags", "custom_captions.items", "captions", "task_us_data",
                            "monetization.is_monetizable")
COUNT_MISSING_AGGREGATION = ("stats.flags", "custom_captions.items", "captions", "task_us_data")

PERCENTILES_AGGREGATION = (
    "stats.views",
    "stats.last_day_views",
    "stats.channel_subscribers",
    "ads_stats.video_view_rate",
    "ads_stats.ctr",
    "ads_stats.ctr_v",
    "ads_stats.average_cpv",
    "ads_stats.average_cpm",
    "ads_stats.video_quartile_100_rate",
    "stats.sentiment"
)

MINIMUM_AGGREGATION_COUNT = 10


class VideoManager(BaseManager):
    allowed_sections = BaseManager.allowed_sections\
                       + (Sections.GENERAL_DATA, Sections.STATS, Sections.ANALYTICS,
                          Sections.CHANNEL, Sections.CAPTIONS, Sections.MONETIZATION,
                          Sections.ADS_STATS, Sections.CMS, Sections.ANALYTICS_SCHEDULE,
                          Sections.ADS_STATS_SCHEDULE, Sections.CAPTIONS_SCHEDULE,
                          Sections.BRAND_SAFETY, Sections.CUSTOM_CAPTIONS, Sections.TASK_US_DATA)
    model = Video
    forced_filter_section_oudated = Sections.GENERAL_DATA
    range_aggregation_fields = RANGE_AGGREGATION
    count_aggregation_fields = COUNT_AGGREGATION
    percentiles_aggregation_fields = PERCENTILES_AGGREGATION
    count_exists_aggregation_fields = COUNT_EXISTS_AGGREGATION
    count_missing_aggregation_fields = COUNT_MISSING_AGGREGATION

    def get_all_video_ids_generator(self, channel_id):
        _query = self.by_channel_ids_query(channel_id)
        videos_generator = self.model.search().source(Sections.MAIN).query(_query).scan()
        yield from (video.main.id for video in videos_generator)

    def by_channel_ids_query(self, channels_ids, invert=False):
        query = QueryBuilder().build()
        query = query.must_not() if invert else query.must()
        query = query.terms() if isinstance(channels_ids, list) else query.term()
        query = query.field(VIDEO_CHANNEL_ID_FIELD).value(channels_ids)
        values = query.get()
        return values

    def by_content_owner_ids_query(self, content_owner_ids):
        query = QueryBuilder().build().must()
        query = query.terms() if isinstance(content_owner_ids, list) else query.term()
        query = query.field(CONTENT_OWNER_ID_FIELD).value(content_owner_ids)
        values = query.get()
        return values

    def forced_filters(self, include_deleted=False):
        return super(VideoManager, self).forced_filters(include_deleted=include_deleted) &\
               self._filter_existent_section(Sections.GENERAL_DATA)

    def get_never_updated_generator(self, outdated_at, never_updated_section, channels_ids=None,
                                    content_owner_ids=None):

        control_section = self._get_control_section()
        field_updated_at = f"{control_section}.{TimestampFields.UPDATED_AT}"

        _filter_outdated = QueryBuilder().build().must().range().field(field_updated_at).lt(outdated_at).get()
        _filter_nonexistent_section = self._filter_nonexistent_section(control_section)
        _filter_never_updated_section = self._filter_nonexistent_section(never_updated_section)

        if channels_ids is not None and content_owner_ids is None:
            _filter = self.by_channel_ids_query(channels_ids)
        elif content_owner_ids is not None and channels_ids is None:
            _filter = self.by_content_owner_ids_query(content_owner_ids)
        else:
            raise AttributeError("One of two parameters must be specified: channel_id OR content_owner_id")

        _filter &= _filter_never_updated_section & _filter_outdated | _filter_nonexistent_section

        _sort = [
            {field_updated_at: {"order": SortDirections.ASCENDING}},
            {MAIN_ID_FIELD: {"order": SortDirections.ASCENDING}},
        ]

        yield from self.search(filters=_filter, sort=_sort).scan()

    def get_total_count_for_channels(self, channels_ids):
        search = self.search(query=self.by_channel_ids_query(channels_ids))
        result = search.execute()
        total = result.hits.total.value
        return total

    def get_total_count_for_content_owners(self, content_owner_ids):
        search = self.search(query=self.by_content_owner_ids_query(content_owner_ids))
        result = search.execute()
        total = result.hits.total.value
        return total

    def get_totals_by_channels(self, channels: List[Channel]):
        channel_ids = [channel.main.id for channel in channels]
        result = self.get_totals_by_channel_ids(channel_ids)
        return result

    def get_totals_by_channel_ids(self, channel_ids: List[str]):
        query = {
            "bool": {
                "must": [
                    {"terms": {"channel.id": channel_ids}}
                ]
            }
        }
        aggrenation_name = "count"
        aggregations = {
            aggrenation_name: {
                "terms": {
                    "field": "channel.id",
                    "size": ES_CHUNK_SIZE,
                },
            }
        }

        result = self.search(query=query) \
                     .update_from_dict({"aggs": aggregations, "size": 0}) \
                     .execute()

        aggregation_buckets = result["aggregations"][aggrenation_name]["buckets"]

        aggregations_map = {bucket["key"]: bucket["doc_count"] for bucket in aggregation_buckets}
        result = OrderedDict((channel_id, aggregations_map.get(channel_id, 0)) for channel_id in channel_ids)

        return result

    def __get_aggregation_dict(self, properties):
        aggregation = {
            **self._get_range_aggs(),
            **self._get_count_aggs(),
        }
        return {
            key: value
            for key, value in aggregation.items()
            if key in properties
        }

    def get_aggregation(self, search=None, size=0, properties=None):
        if not properties:
            return None
        if not search:
            search = self._search()

        search_query = search.to_dict()

        aggregations = self.__get_aggregation_dict(properties)

        aggregations_search = self._search().update_from_dict({
            "size": size,
            "aggs": aggregations
        })
        aggregations_search.update_from_dict(search_query)
        aggregations_result = aggregations_search.execute().aggregations.to_dict()

        count_exists_aggs_result = self._get_count_exists_aggs_result(search, properties)
        aggregations_result.update(count_exists_aggs_result)
        aggregations_result = add_brand_safety_labels(aggregations_result)
        aggregations_result = add_sentiment_labels(aggregations_result)
        aggregations_result = self.adapt_flags_aggregation(aggregations_result)
        aggregations_result = self.adapt_transcripts_aggregation(aggregations_result)
        aggregations_result = self.adapt_iab_categories_aggregation(aggregations_result)
        aggregations_result = self.adapt_country_code_aggregation(aggregations_result)
        aggregations_result = self.adapt_lang_code_aggregation(aggregations_result)
        aggregations_result = self.adapt_age_group_aggregation(aggregations_result)
        aggregations_result = self.adapt_gender_aggregation(aggregations_result)
        aggregations_result = self.adapt_content_type_aggregation(aggregations_result)
        return aggregations_result

    def adapt_lang_code_aggregation(self, aggregations):
        if "general_data.lang_code" in aggregations:
            buckets_to_remove = []
            for bucket in aggregations["general_data.lang_code"]["buckets"]:
                if bucket["doc_count"] < MINIMUM_AGGREGATION_COUNT:
                    buckets_to_remove.append(bucket)
                    continue
                try:
                    bucket["title"] = LANGUAGES[bucket["key"]]
                # pylint: disable=invalid-name
                # pylint: disable=broad-except
                except Exception:
                    language = languages.get(alpha_3=bucket["key"])
                    bucket["title"] = language.name if language else bucket["key"]
                # pylint: enable=invalid-name
                # pylint: enable=broad-except
            for bucket in buckets_to_remove:
                aggregations["general_data.lang_code"]["buckets"].remove(bucket)
        return aggregations

    def adapt_flags_aggregation(self, aggregations):
        if "stats.flags" in aggregations:
            flags_buckets = []
            for bucket in aggregations["stats.flags"]["buckets"]:
                key = bucket["key"]
                if key == "viral":
                    bucket["key"] = "Viral"
                    flags_buckets.append(bucket)
                elif key == "most_liked":
                    bucket["key"] = "Most Liked"
                    flags_buckets.append(bucket)
                elif key == "most_watched":
                    bucket["key"] = "Most Watched"
                    flags_buckets.append(bucket)
            aggregations["stats.flags"]["buckets"] = flags_buckets
            aggregations["flags"] = aggregations.pop("stats.flags")
        return aggregations

    def adapt_transcripts_aggregation(self, aggregations):
        if "custom_captions.items:exists" in aggregations and "captions:exists" in aggregations:
            transcripts_count = aggregations["custom_captions.items:exists"] + aggregations["captions:exists"]
            aggregations["transcripts:exists"] = transcripts_count
            aggregations.pop("custom_captions.items:exists")
            aggregations.pop("captions:exists")
        return aggregations

    def _get_enabled_monitoring_warnings(self):
        warning_few_records_updated = (
            Warnings.FewRecordsUpdated(Sections.GENERAL_DATA, 15, True),
            Warnings.FewRecordsUpdated(Sections.STATS, 30, True),
        )
        return warning_few_records_updated + \
               super(VideoManager, self)._get_enabled_monitoring_warnings()


    def _get_enabled_monitoring_alerts(self):
        return (Emergency.NoneRecordsUpdated((Sections.GENERAL_DATA, Sections.STATS), 1, True),) + \
               super(VideoManager, self)._get_enabled_monitoring_alerts()

    def _get_enabled_monitoring_params_info(self):
        return super(VideoManager, self)._get_enabled_monitoring_params_info() + (True,)
