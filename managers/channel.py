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
    "analytics.age_group_13_17",
    "analytics.age_group_18_24",
    "analytics.age_group_25_34",
    "analytics.age_group_35_44",
    "analytics.age_group_45_54",
    "analytics.age_group_55_64",
    "analytics.age_group_65_",
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
    "analytics",
    "analytics.verified",
    "analytics.is_auth",
    "analytics.is_cms",
    "custom_properties.emails",
    "monetization.preferred",
    "ads_stats",
    "analytics.cms_title"
)

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

    def by_content_owner_ids_query(self, content_owner_ids):
        return QueryBuilder().build().must().terms().field(CONTENT_OWNER_ID_FIELD)\
            .value(content_owner_ids).get()

    def forced_filters(self):
        return super(ChannelManager, self).forced_filters() &\
               self._filter_existent_section(Sections.GENERAL_DATA)

    def __get_range_aggs(self):
        range_aggs = {}

        for field in self.range_aggregation_fields:
            range_aggs["{}:min".format(field)] = {
                "min": {"field": field}
            }
            range_aggs["{}:max".format(field)] = {
                "max": {"field": field}
            }
        return range_aggs

    def __get_count_aggs(self):
        count_aggs = {}

        for field in self.count_aggregation_fields:
            count_aggs[field] = {
                "terms": {
                    "size": AGGREGATION_COUNT_SIZE,
                    "field": field,
                    "min_doc_count": 1,
                }
            }
        return count_aggs

    def __get_percentiles_aggs(self):
        percentiles_aggs = {}

        for field in self.percentiles_aggregation_fields:
            percentiles_aggs["{}:percentiles".format(field)] = {
                "percentiles": {
                    "field": field,
                    "percents": AGGREGATION_PERCENTS,
                }
            }
        return percentiles_aggs

    def get_aggregation(self, search=None, size=0):
        if not search:
            search = self._search()

        aggregation = self.__get_range_aggs()
        aggregation.update(self.__get_count_aggs())
        aggregation.update(self.__get_percentiles_aggs())

        search.update_from_dict({
            "size": size,
            "aggs": aggregation
        })
        aggregations_result = search.execute().aggregations

        return aggregations_result
