from pycountry import languages

from es_components.constants import CONTENT_OWNER_ID_FIELD
from es_components.constants import Sections
from es_components.languages import LANGUAGES
from es_components.managers.base import BaseManager
from es_components.models.channel import Channel
from es_components.monitor import Emergency
from es_components.monitor import Warnings
from es_components.query_builder import QueryBuilder
from es_components.utils import add_brand_safety_labels

AGGREGATION_COUNT_SIZE = 100000
AGGREGATION_PERCENTS = tuple(range(10, 100, 10))

RANGE_AGGREGATION = (
    "stats.subscribers",
    "stats.last_30day_subscribers",
    "stats.last_30day_views",
    "stats.views_per_video",
    "stats.sentiment",
    "stats.engage_rate",
    "analytics.age13_17",
    "analytics.age18_24",
    "analytics.age25_34",
    "analytics.age35_44",
    "analytics.age45_54",
    "analytics.age55_64",
    "analytics.age65_",
    "analytics.gender_male",
    "analytics.gender_female",
    "analytics.gender_other",
    "ads_stats.video_view_rate",
    "ads_stats.ctr",
    "ads_stats.ctr_v",
    "ads_stats.average_cpv",
    "ads_stats.average_cpm",
    "ads_stats.video_quartile_100_rate",
)

COUNT_AGGREGATION = (
    "general_data.country_code",
    "general_data.top_category",
    "general_data.top_lang_code",
    "general_data.iab_categories",
    "analytics.is_auth",
    "analytics.is_cms",
    "auth_channel",
    "custom_properties.preferred",
    "brand_safety",
    "task_us_data.age_group",
    "task_us_data.content_quality",
    "task_us_data.content_type",
    "task_us_data.gender",
    "custom_properties.is_tracked",
    "brand_safety.limbo_status",
)

COUNT_EXISTS_AGGREGATION = (
    "ads_stats",
    "monetization.is_monetizable",
    "task_us_data",
    "task_us_data.last_vetted_at",
    "ias_data.ias_verified",
)
COUNT_MISSING_AGGREGATION = (
    "task_us_data",
    "task_us_data.last_vetted_at",
)

PERCENTILES_AGGREGATION = (
    "stats.subscribers",
    "stats.last_30day_subscribers",
    "stats.last_30day_views",
    "stats.views_per_video",
    "ads_stats.video_view_rate",
    "ads_stats.ctr",
    "ads_stats.ctr_v",
    "ads_stats.average_cpv",
    "ads_stats.average_cpm"
)
FORCED_FILTER_MIN_VIDEO_COUNT = 0

MINIMUM_AGGREGATION_COUNT = 5


class ChannelManager(BaseManager):
    allowed_sections = BaseManager.allowed_sections \
                       + (Sections.GENERAL_DATA, Sections.STATS, Sections.ANALYTICS, Sections.AUTH,
                          Sections.MONETIZATION, Sections.SOCIAL, Sections.ADS_STATS, Sections.CMS,
                          Sections.CUSTOM_PROPERTIES, Sections.GENERAL_DATA_SCHEDULE, Sections.SIMILAR_CHANNELS,
                          Sections.STATS_SCHEDULE, Sections.ANALYTICS_SCHEDULE, Sections.ADS_STATS_SCHEDULE,
                          Sections.BRAND_SAFETY, Sections.TASK_US_DATA, Sections.IAS_DATA)
    model = Channel
    forced_filter_section_oudated = Sections.GENERAL_DATA
    range_aggregation_fields = RANGE_AGGREGATION
    count_aggregation_fields = COUNT_AGGREGATION
    percentiles_aggregation_fields = PERCENTILES_AGGREGATION
    count_exists_aggregation_fields = COUNT_EXISTS_AGGREGATION
    count_missing_aggregation_fields = COUNT_MISSING_AGGREGATION
    use_admin_brand_safety_labels = False

    def by_content_owner_ids_query(self, content_owner_ids):
        return QueryBuilder().build().must().terms().field(CONTENT_OWNER_ID_FIELD) \
            .value(content_owner_ids).get()

    def forced_filters(self, include_deleted=False):
        return super(ChannelManager, self).forced_filters(include_deleted=include_deleted) & \
               self._filter_existent_section(Sections.GENERAL_DATA) & \
               (
                   self._filter_existent_section(Sections.CMS) |
                   self._filter_existent_section(Sections.AUTH) |
                   QueryBuilder().build().must().range().field(f"{Sections.STATS}.total_videos_count")
                   .gt(FORCED_FILTER_MIN_VIDEO_COUNT).get()
               )

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

        aggregation = self.__get_aggregation_dict(properties)

        search.update_from_dict({
            "size": size,
            "aggs": aggregation
        })
        aggregations_result = search.execute().aggregations.to_dict()

        count_exists_aggs_result = self._get_count_exists_aggs_result(search, properties)

        aggregations_result.update(count_exists_aggs_result)

        aggregations_result = add_brand_safety_labels(aggregations_result, self.use_admin_brand_safety_labels)
        aggregations_result = self.adapt_iab_categories_aggregation(aggregations_result)
        aggregations_result = self.adapt_country_code_aggregation(aggregations_result)
        aggregations_result = self.adapt_lang_code_aggregation(aggregations_result)
        aggregations_result = self.adapt_age_group_aggregation(aggregations_result)
        aggregations_result = self.adapt_gender_aggregation(aggregations_result)
        aggregations_result = self.adapt_content_type_aggregation(aggregations_result)
        aggregations_result = self.adapt_is_tracked_aggregation(aggregations_result)
        aggregations_result = self.adapt_content_quality_aggregation(aggregations_result)
        aggregations_result = self.adapt_limbo_status_aggregation(aggregations_result)
        aggregations_result = self.adapt_auth_channel_aggregation(aggregations_result)
        return aggregations_result

    def adapt_auth_channel_aggregation(self, aggregations):
        """ sets aggregation for channels with active token in AuthChannel model """
        if "auth_channel" in aggregations:
            query = QueryBuilder().build().must().terms().field("main.id").value(self.context.get("auth_channel_ids")).get()
            query &= self.forced_filters()
            result = self.search(query).count()
            aggregations["auth_channel"]["buckets"] = [{"key": "Auth Channels", "doc_count": result}]
        return aggregations

    def adapt_lang_code_aggregation(self, aggregations):
        if "general_data.top_lang_code" in aggregations:
            buckets_to_remove = []
            for bucket in aggregations["general_data.top_lang_code"]["buckets"]:
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
                aggregations["general_data.top_lang_code"]["buckets"].remove(bucket)
        return aggregations

    def adapt_is_tracked_aggregation(self, aggregations):
        if "custom_properties.is_tracked" in aggregations:
            try:
                aggregations["custom_properties.is_tracked"]["buckets"][0]["key"] = "Tracked Channels"
                aggregations["custom_properties.is_tracked"]["buckets"].pop(1)
            except (KeyError, IndexError):
                pass
        return aggregations

    def _get_enabled_monitoring_warnings(self):
        warning_few_records_updated = (
            Warnings.FewRecordsUpdated(Sections.GENERAL_DATA, 15, True),
            Warnings.FewRecordsUpdated(Sections.STATS, 30, True),
            Warnings.FewRecordsUpdated(Sections.ANALYTICS, 90, True),
            Warnings.FewRecordsUpdated(Sections.ADS_STATS, 0, True),
        )
        return warning_few_records_updated + \
               super(ChannelManager, self)._get_enabled_monitoring_warnings()

    def _get_enabled_monitoring_alerts(self):
        return (Emergency.NoneRecordsUpdated((Sections.GENERAL_DATA, Sections.STATS), 1, True),) + \
               super(ChannelManager, self)._get_enabled_monitoring_alerts()

    def _get_enabled_monitoring_params_info(self):
        skipped_sections = (Sections.GENERAL_DATA,)
        return self.sections, skipped_sections, True


class VettingAdminChannelManager(ChannelManager):
    """
    This manager includes the brand safety aggregation for
    Channels that have a brand safety score of 'Unsuitable'
    """

    use_admin_brand_safety_labels = True

    def _get_count_aggs(self):
        count_aggs = super()._get_count_aggs()
        count_aggs["brand_safety"] = {
            "range": {
                "field": "brand_safety.overall_score",
                "ranges": [
                    {"from": 0, "to": 69.1},
                    {"from": 70, "to": 79.1},
                    {"from": 80, "to": 89.1},
                    {"from": 90, "to": 100.1},
                ]
            }
        }
        return count_aggs
