from elasticsearch_dsl import Boolean
from elasticsearch_dsl import Date
from elasticsearch_dsl import Double
from elasticsearch_dsl import Long
from elasticsearch_dsl import Object

from es_components.config import KEYWORD_INDEX_NAME
from es_components.config import KEYWORD_DOC_TYPE
from es_components.models.base import BaseDocument
from es_components.models.base import BaseInnerDoc
from es_components.models.base import Schedule


class KeywordSectionStats(BaseInnerDoc):
    """ Nested statistics section for Keyword document """
    historydate = Date(index=False)
    video_count = Long()
    views = Long()
    views_history = Long(index=False, multi=True)
    last_30day_views = Long()
    category_views = Object(enabled=False)
    category_last_day_views = Object(enabled=False)
    category_last_7day_views = Object(enabled=False)
    category_last_30day_views = Object(enabled=False)
    search_volume_history = Long(index=False, multi=True)
    search_volume = Long()
    competition = Double()
    average_cpc = Double()
    interests = Long(index=False, multi=True)
    is_viral = Boolean()
    is_aw_keyword = Boolean()


class Keyword(BaseDocument):
    stats = Object(KeywordSectionStats)

    stats_schedule = Object(Schedule)

    class Index:
        name = KEYWORD_INDEX_NAME

    class Meta:
        doc_type = KEYWORD_DOC_TYPE

    def populate_stats(self, **kwargs):
        if not self.stats:
            self.stats = KeywordSectionStats()
        self.stats.update(**kwargs)
