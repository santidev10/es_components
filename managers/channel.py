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
    "analytics",
    "analytics.verified",
    "analytics.is_auth",
    "analytics.is_cms",
    "custom_properties.preferred",
    "analytics.cms_title"
)

COUNT_EXISTS_AGGREGATION = (
    "custom_properties.emails",
    "ads_stats"
)
COUNT_MISSING_AGGREGATION = ("custom_properties.emails",)

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

    def __get_count_exists_aggs_result(self, search):
        result = {}

        for field in self.count_exists_aggregation_fields:
            exists_filter = self._filter_existent_section(field)
            exists_count = search.filter(exists_filter).count()
            result[f"{field}:exists"] = exists_count

        for field in self.count_missing_aggregation_fields:
            missing_filter = self._filter_nonexistent_section(field)
            missing_count = search.filter(missing_filter).count()
            result[f"{field}:missing"] = missing_count

        return result



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
        aggregations_result = search.execute().aggregations.to_dict()

        count_exists_aggs_result = self.__get_count_exists_aggs_result(search)

        aggregations_result.update(count_exists_aggs_result)

        return aggregations_result
