from elasticsearch_dsl import Boolean
from elasticsearch_dsl import Date
from elasticsearch_dsl import Double
from elasticsearch_dsl import Keyword as KeywordField
from elasticsearch_dsl import Long
from elasticsearch_dsl import Object

from es_components.config import KEYWORD_DOC_TYPE
from es_components.config import KEYWORD_INDEX_NAME
from es_components.config import KEYWORD_INDEX_PREFIX
from es_components.config import KEYWORD_REFRESH_INTERVAL
from es_components.models.base import BaseDocument
from es_components.models.base import BaseInnerDocWithHistory
from es_components.models.base import Schedule


class KeywordSectionStats(BaseInnerDocWithHistory):
    """ Nested statistics section for Keyword document """
    fetched_at = Date(index=False)
    historydate = Date(index=False)
    video_count = Long(index=False)
    views = Long(index=False)
    views_history = Long(index=False, multi=True)
    last_30day_views = Long()
    top_language = KeywordField()
    category_views = Object(enabled=False)
    category_last_day_views = Object(enabled=False)
    category_last_7day_views = Object(enabled=False)
    category_last_30day_views = Object(enabled=False)
    top_category = KeywordField()
    top_category_last_day_views = Long()
    top_category_last_7day_views = Long()
    top_category_last_30day_views = Long()
    monthly_searches = Object(enabled=False)
    search_volume = Long()
    competition = Double()
    average_cpc = Double()
    interests = Long(index=False, multi=True)
    is_viral = Boolean()
    is_aw_keyword = Boolean(index=False)  # unused

    class History:
        all = (
            "views",
        )


class Keyword(BaseDocument):
    stats = Object(KeywordSectionStats)

    stats_schedule = Object(Schedule)

    class Index:
        name = KEYWORD_INDEX_NAME
        prefix = KEYWORD_INDEX_PREFIX
        settings = dict(
            refresh_interval=KEYWORD_REFRESH_INTERVAL,
        )

    class Meta:
        doc_type = KEYWORD_DOC_TYPE

    def populate_stats(self, **kwargs):
        if not self.stats:
            self.stats = KeywordSectionStats()
        self.stats.update(**kwargs)
