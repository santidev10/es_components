from es_components.managers.base import BaseManager
from es_components.models.channel import Channel
from es_components.constants import Sections
from es_components.constants import CONTENT_OWNER_ID_FIELD
from es_components.query_builder import QueryBuilder


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
    "ads_stats.average_cpv",
)

COUNT_AGGREGATION = (
    "general_data.country",
    "general_data.top_category",
    "general_data.top_language",
    "analytics.is_auth",
    "analytics.is_cms",
    "custom_properties.preferred",
    "analytics.cms_title"
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
    "ads_stats.average_cpv",
)


class ChannelManager(BaseManager):
    allowed_sections = BaseManager.allowed_sections\
                       + (Sections.GENERAL_DATA, Sections.STATS, Sections.ANALYTICS,
                          Sections.MONETIZATION, Sections.SOCIAL, Sections.ADS_STATS, Sections.CMS,
                          Sections.CUSTOM_PROPERTIES, Sections.GENERAL_DATA_SCHEDULE,
                          Sections.STATS_SCHEDULE, Sections.ANALYTICS_SCHEDULE)
    model = Channel
    range_aggregation_fields = RANGE_AGGREGATION
    count_aggregation_fields = COUNT_AGGREGATION
    percentiles_aggregation_fields = PERCENTILES_AGGREGATION
    count_exists_aggregation_fields = COUNT_EXISTS_AGGREGATION
    count_missing_aggregation_fields = COUNT_MISSING_AGGREGATION

    def by_content_owner_ids_query(self, content_owner_ids):
        return QueryBuilder().build().must().terms().field(CONTENT_OWNER_ID_FIELD)\
            .value(content_owner_ids).get()

    def forced_filters(self):
        return super(ChannelManager, self).forced_filters() &\
               self._filter_existent_section(Sections.GENERAL_DATA)

    def __get_aggregation_dict(self, properties):
        aggregation = {
            **self._get_range_aggs(),
            **self._get_count_aggs(),
            **self._get_percentiles_aggs(),
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

        return aggregations_result
