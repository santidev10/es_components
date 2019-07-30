from es_components.constants import Sections
from es_components.managers.base import BaseManager
from es_components.models.keyword import Keyword


class KeywordManager(BaseManager):
    count_aggregation_fields = ("stats.top_category",)
    allowed_sections = BaseManager.allowed_sections + (Sections.STATS, Sections.STATS_SCHEDULE)
    model = Keyword

    def forced_filters(self):
        return super(KeywordManager, self).forced_filters() &\
               self._filter_existent_section(Sections.STATS)

