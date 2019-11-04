from es_components.managers.base import BaseManager
from es_components.models.channel import Channel
from es_components.constants import Sections
from es_components.constants import CONTENT_OWNER_ID_FIELD
from es_components.query_builder import QueryBuilder
from es_components.monitor import Emergency
from es_components.monitor import Warnings
from es_components.utils import add_brand_safety_labels


AGGREGATION_COUNT_SIZE = 100000
AGGREGATION_PERCENTS = tuple(range(10, 100, 10))

RANGE_AGGREGATION = (
    "stats.subscribers",
    "social.facebook_likes",
    "social.twitter_followers",
    "social.instagram_followers",
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
    "ads_stats.average_cpv"
)

COUNT_AGGREGATION = (
    "general_data.country",
    "general_data.top_category",
    "general_data.top_language",
    "analytics.is_auth",
    "analytics.is_cms",
    "custom_properties.preferred",
    "cms.cms_title",
    "brand_safety",
    "stats.channel_group"
)

COUNT_EXISTS_AGGREGATION = (
    "general_data.emails",
    "ads_stats",
    "analytics"
)
COUNT_MISSING_AGGREGATION = ("general_data.emails", "analytics",)

PERCENTILES_AGGREGATION = (
    "stats.subscribers",
    "social.facebook_likes",
    "social.twitter_followers",
    "social.instagram_followers",
    "stats.last_30day_subscribers",
    "stats.last_30day_views",
    "stats.views_per_video",
    "ads_stats.video_view_rate",
    "ads_stats.ctr",
    "ads_stats.ctr_v",
    "ads_stats.average_cpv"
)
FORCED_FILTER_MIN_VIDEO_COUNT = 0


class ChannelManager(BaseManager):
    allowed_sections = BaseManager.allowed_sections\
                       + (Sections.GENERAL_DATA, Sections.STATS, Sections.ANALYTICS, Sections.AUTH,
                          Sections.MONETIZATION, Sections.SOCIAL, Sections.ADS_STATS, Sections.CMS,
                          Sections.CUSTOM_PROPERTIES, Sections.GENERAL_DATA_SCHEDULE, Sections.SIMILAR_CHANNELS,
                          Sections.STATS_SCHEDULE, Sections.ANALYTICS_SCHEDULE, Sections.BRAND_SAFETY,
                          Sections.TASK_US_DATA)
    model = Channel
    forced_filter_section_oudated = Sections.GENERAL_DATA
    range_aggregation_fields = RANGE_AGGREGATION
    count_aggregation_fields = COUNT_AGGREGATION
    percentiles_aggregation_fields = PERCENTILES_AGGREGATION
    count_exists_aggregation_fields = COUNT_EXISTS_AGGREGATION
    count_missing_aggregation_fields = COUNT_MISSING_AGGREGATION

    def by_content_owner_ids_query(self, content_owner_ids):
        return QueryBuilder().build().must().terms().field(CONTENT_OWNER_ID_FIELD)\
            .value(content_owner_ids).get()

    def forced_filters(self):
        return super(ChannelManager, self).forced_filters() & \
               self._filter_existent_section(Sections.GENERAL_DATA) & \
               (
                   self._filter_existent_section(Sections.CMS) |
                   self._filter_existent_section(Sections.AUTH) |
                   QueryBuilder().build().must().range().field(f"{Sections.STATS}.observed_videos_count")
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

        aggregations_result = add_brand_safety_labels(aggregations_result)
        aggregations_result = self.adapt_channel_group(aggregations_result)

        return aggregations_result

    def adapt_channel_group(self, aggregations):
        if "stats.channel_group" in aggregations:
            channel_group_buckets = []
            for bucket in aggregations["stats.channel_group"]["buckets"]:
                key = bucket["key"]
                if key == "influencers":
                    bucket["key"] = "Influencers"
                    channel_group_buckets.append(bucket)
                elif key == "brands":
                    bucket["key"] = "Brands"
                    channel_group_buckets.append(bucket)
                elif key == "media":
                    bucket["key"] = "Media & Entertainment"
                    channel_group_buckets.append(bucket)
            aggregations["stats.channel_group"]["buckets"] = channel_group_buckets
        return aggregations


    def _get_enabled_monitoring_warnings(self):
        warning_no_new_sections = tuple([Warnings.NoNewSections(section) for section in self.sections])
        warning_few_records_updated = (
            Warnings.FewRecordsUpdated(Sections.GENERAL_DATA, 15, True),
            Warnings.FewRecordsUpdated(Sections.STATS, 30, True),
            Warnings.FewRecordsUpdated(Sections.ANALYTICS, 90, True),
            Warnings.FewRecordsUpdated(Sections.ADS_STATS, 0, True),
        )
        return warning_few_records_updated + warning_no_new_sections + \
               super(ChannelManager, self)._get_enabled_monitoring_warnings()

    def _get_enabled_monitoring_alerts(self):
        return (Emergency.NoneRecordsUpdated((Sections.GENERAL_DATA, Sections.STATS), 1, True),) + \
               super(ChannelManager, self)._get_enabled_monitoring_alerts()

    def _get_enabled_monitoring_params_info(self):
        skipped_sections = (Sections.GENERAL_DATA,)
        return self.sections, skipped_sections, True,

