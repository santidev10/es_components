from es_components.managers.base import BaseManager
from es_components.models.channel import Channel
from es_components.constants import SECTIONS
from es_components.constants import CONTENT_OWNER_ID_FIELD


class ChannelManager(BaseManager):
    allowed_sections = BaseManager.allowed_sections\
                       + (SECTIONS.GENERAL_DATA, SECTIONS.STATS, SECTIONS.ANALYTICS,
                          SECTIONS.MONETIZATION, SECTIONS.SOCIAL, SECTIONS.ADS_STATS, SECTIONS.CMS,
                          SECTIONS.GENERAL_DATA_SCHEDULE, SECTIONS.STATS_SCHEDULE, SECTIONS.ANALYTICS_SCHEDULE)
    model = Channel

    def by_content_owner_ids_query(self, content_owner_ids):
        return self._filter_term(CONTENT_OWNER_ID_FIELD, content_owner_ids)

    def forced_filters(self, updated_at):
        return super(ChannelManager, self).forced_filters(updated_at) &\
               self._filter_existent_section(SECTIONS.GENERAL_DATA)
