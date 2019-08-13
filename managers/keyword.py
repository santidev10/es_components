from es_components.constants import Sections
from es_components.managers.base import BaseManager
from es_components.models.keyword import Keyword

RANGE_AGGREGATION = (
    "stats.search_volume",
    "stats.average_cpc",
    "stats.competition",
)
PERCENTILES_AGGREGATION = (
    "stats.search_volume",
    "stats.average_cpc",
    "stats.competition",
)


class KeywordManager(BaseManager):
    count_aggregation_fields = ("stats.top_category",)
    allowed_sections = BaseManager.allowed_sections + (Sections.STATS, Sections.STATS_SCHEDULE)
    model = Keyword
    range_aggregation_fields = RANGE_AGGREGATION
    percentiles_aggregation_fields = PERCENTILES_AGGREGATION

    def forced_filters(self):
        return super(KeywordManager, self).forced_filters() &\
               self._filter_existent_section(Sections.STATS)
