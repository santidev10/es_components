from es_components.constants import Sections
from es_components.managers.base import BaseManager
from es_components.models.keyword import Keyword
from es_components.monitor import Warnings


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
    model = Keyword
    forced_filter_section_oudated = Sections.STATS
    range_aggregation_fields = RANGE_AGGREGATION
    percentiles_aggregation_fields = PERCENTILES_AGGREGATION
    count_aggregation_fields = ("stats.top_category", "stats.is_viral")
    allowed_sections = BaseManager.allowed_sections + (Sections.STATS, Sections.STATS_SCHEDULE)

    def forced_filters(self):
        return super(KeywordManager, self).forced_filters() &\
               self._filter_existent_section(Sections.STATS)

    def _get_enabled_monitoring_warnings(self):
        return (Warnings.FewRecordsUpdated(Sections.STATS, 10),) + \
               super(KeywordManager, self)._get_enabled_monitoring_warnings()

    def _get_enabled_monitoring_params_info(self):
        skipped_sections = (Sections.STATS,)
        return self.sections, skipped_sections, True
