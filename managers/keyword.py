from es_components.constants import Sections
from es_components.managers.base import BaseManager
from es_components.models.keyword import Keyword


class KeywordManager(BaseManager):
    allowed_sections = BaseManager.allowed_sections + (Sections.STATS, Sections.STATS_SCHEDULE)
    model = Keyword

    def forced_filters(self):
        return super(KeywordManager, self).forced_filters() &\
               self.filter_existent_section(Sections.STATS)

