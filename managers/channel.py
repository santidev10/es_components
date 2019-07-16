from es_components.managers.base import BaseManager
from es_components.models.channel import Channel
from es_components.constants import Sections
from es_components.constants import CONTENT_OWNER_ID_FIELD


class ChannelManager(BaseManager):
    allowed_sections = BaseManager.allowed_sections\
                       + (Sections.GENERAL_DATA, Sections.STATS, Sections.ANALYTICS,
                          Sections.MONETIZATION, Sections.SOCIAL, Sections.ADS_STATS, Sections.CMS,
                          Sections.GENERAL_DATA_SCHEDULE, Sections.STATS_SCHEDULE, Sections.ANALYTICS_SCHEDULE)
    model = Channel

    def by_content_owner_ids_query(self, content_owner_ids):
        return self.filter_term(CONTENT_OWNER_ID_FIELD, content_owner_ids)

    def forced_filters(self):
        return super(ChannelManager, self).forced_filters() &\
               self.filter_existent_section(Sections.GENERAL_DATA)
