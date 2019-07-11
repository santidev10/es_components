from es_components.constants import SECTIONS
from es_components.managers.base import BaseManager
from es_components.models.keyword import Keyword


class KeywordManager(BaseManager):
    allowed_sections = BaseManager.allowed_sections + (SECTIONS.STATS, SECTIONS.STATS_SCHEDULE)
    model = Keyword

    def forced_filters(self, updated_at):
        return super(KeywordManager, self).forced_filters(updated_at) &\
               self._filter_existent_section(SECTIONS.STATS)

