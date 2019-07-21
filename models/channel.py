from elasticsearch_dsl import Boolean
from elasticsearch_dsl import Date
from elasticsearch_dsl import Double
from elasticsearch_dsl import Float
from elasticsearch_dsl import Keyword
from elasticsearch_dsl import Long
from elasticsearch_dsl import Object
from elasticsearch_dsl import Text

from es_components.config import CHANNEL_INDEX_NAME
from es_components.config import CHANNEL_DOC_TYPE
from es_components.constants import Sections
from es_components.models.base import BaseDocument
from es_components.models.base import BaseInnerDoc
from es_components.models.base import Schedule
from es_components.models.base import CommonSectionAnalytics
from es_components.models.base import BaseInnerDocWithHistory


class ChannelSectionGeneralData(BaseInnerDoc):
    """ Nested general data section for Channel document """
    title = Text()
    description = Text(index=False)
    thumbnail_image_url = Text(index=False)
    country = Keyword()
    youtube_published_at = Date(index=False)
    video_tags = Keyword(index=False, multi=True)
    top_category = Keyword()
    lang_codes = Keyword(index=False, multi=True)
    top_language = Keyword()


class ChannelSectionStats(BaseInnerDocWithHistory):
    """ Nested statistics section for Channel document """
    fetched_at = Date(index=False)
    historydate = Date(index=False)
    last_video_published_at = Date(index=False)
    subscribers = Long()
    subscribers_history = Long(index=False, multi=True)
    last_day_subscribers = Long()
    last_7day_subscribers = Long()
    last_30day_subscribers = Long()
    observed_videos_count = Long(index=False)
    observed_videos_count_history = Long(index=False, multi=True)
    total_videos_count = Long(index=False)
    total_videos_count_history = Long(index=False, multi=True)
    last_30day_observed_videos = Long(index=False)
    last_30day_published_videos = Long(index=False)
    last_365day_published_videos = Long(index=False)
    views = Long(index=False)
    views_history = Long(index=False, multi=True)
    last_day_views = Long()
    last_7day_views = Long()
    last_30day_views = Long()
    views_per_video = Double()
    views_per_video_history = Double(index=False, multi=True)
    observed_videos_views = Long(index=False)
    observed_videos_views_history = Long(index=False, multi=True)
    observed_videos_likes = Long(index=False)
    observed_videos_likes_history = Long(index=False, multi=True)
    observed_videos_dislikes = Long(index=False)
    observed_videos_dislikes_history = Long(index=False, multi=True)
    observed_videos_comments = Long(index=False)
    engage_rate = Double()
    engage_rate_history = Double(index=False, multi=True)
    sentiment = Double()
    sentiment_history = Double(index=False, multi=True)

    class History:
        all = (
            "subscribers", "views", "total_videos_count", "views_per_video", "observed_videos_count",
            "observed_videos_views", "observed_videos_likes", "observed_videos_dislikes", "sentiment", "engage_rate"
        )


class ChannelSectionMonetization(BaseInnerDoc):
    """ Nested monetization section for Channel document """
    rate = Double()
    preferred = Boolean()


class ChannelSectionSocial(BaseInnerDoc):
    """ Nested social section for Channel document """
    facebook_link = Text(index=False)
    facebook_likes = Long()
    twitter_link = Text(index=False)
    twitter_tweets = Long()
    twitter_followers = Long()
    instagram_link = Text(index=False)
    instagram_followers = Long()


class ChannelSectionAdsStats(BaseInnerDoc):
    """ Nested adwords stats section for Channel document """
    video_view_rate_top = Float(index=False)
    video_view_rate_bottom = Float(index=False)
    ctr_top = Float(index=False)
    ctr_bottom = Float(index=False)
    ctr_v_top = Float(index=False)
    ctr_v_bottom = Float(index=False)
    average_cpv_top = Float(index=False)
    average_cpv_bottom = Float(index=False)
    cost = Float(index=False)
    clicks_count = Long(index=False)
    views_count = Long(index=False)
    impressions_spv_count = Long(index=False)
    impressions_spm_count = Long(index=False)
    video_view_rate = Double()
    ctr = Double()
    ctr_v = Double()
    average_cpv = Double()


class ChannelSectionCMS(BaseInnerDoc):
    """ Nested CMS section for Channel document """
    content_owner_id = Keyword(multi=True)


class Channel(BaseDocument):
    general_data = Object(ChannelSectionGeneralData)
    stats = Object(ChannelSectionStats)
    analytics = Object(CommonSectionAnalytics)
    monetization = Object(ChannelSectionMonetization)
    social = Object(ChannelSectionSocial)
    ads_stats = Object(ChannelSectionAdsStats)
    cms = Object(ChannelSectionCMS)

    general_data_schedule = Object(Schedule)
    stats_schedule = Object(Schedule)
    analytics_schedule = Object(Schedule)

    class Index:
        name = CHANNEL_INDEX_NAME

    class Meta:
        doc_type = CHANNEL_DOC_TYPE

    def populate_general_data(self, **kwargs):
        self._populate_section(Sections.GENERAL_DATA, **kwargs)

    def populate_stats(self, **kwargs):
        self._populate_section(Sections.STATS, **kwargs)

    def populate_analytics(self, **kwargs):
        self._populate_section(Sections.ANALYTICS, **kwargs)

    def populate_monetization(self, **kwargs):
        self._populate_section(Sections.MONETIZATION, **kwargs)

    def populate_social(self, **kwargs):
        self._populate_section(Sections.SOCIAL, **kwargs)

    def populate_ads_stats(self, **kwargs):
        self._populate_section(Sections.ADS_STATS, **kwargs)

    def populate_cms(self, **kwargs):
        self._populate_section(Sections.CMS, **kwargs)
