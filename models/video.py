from elasticsearch_dsl import Boolean
from elasticsearch_dsl import Date
from elasticsearch_dsl import Double
from elasticsearch_dsl import Float
from elasticsearch_dsl import InnerDoc
from elasticsearch_dsl import Keyword
from elasticsearch_dsl import Long
from elasticsearch_dsl import Object
from elasticsearch_dsl import Text

from es_components.config import VIDEO_DOC_TYPE
from es_components.config import VIDEO_INDEX_NAME
from es_components.constants import Sections
from es_components.models.base import BaseDocument
from es_components.models.base import BaseInnerDoc
from es_components.models.base import BaseInnerDocWithHistory
from es_components.models.base import Schedule


class VideoSectionGeneralData(BaseInnerDoc):
    """ Nested general data section for Video document """
    title = Text()
    description = Text(index=False)
    thumbnail_image_url = Text(index=False)
    country = Keyword()
    tags = Keyword(multi=True)
    youtube_published_at = Date(index=False)
    category = Keyword()
    lang_code = Keyword()
    language = Keyword()
    duration = Long(index=False)
    license = Keyword()
    is_streaming = Boolean()


class VideoSectionChannel(BaseInnerDoc):
    # pylint: disable=invalid-name
    id = Keyword()
    # pylint: enable=invalid-name
    title = Keyword()


class VideoSectionStats(BaseInnerDocWithHistory):
    """ Nested statistics section for Video document """
    fetched_at = Date(index=False)
    historydate = Date(index=False)
    views = Long()
    views_history = Long(index=False, multi=True)
    last_day_views = Long()
    last_7day_views = Long()
    last_30day_views = Long()
    views_per_day = Double()
    likes = Long()
    likes_history = Long(index=False, multi=True)
    last_day_likes = Long()
    last_7day_likes = Long()
    last_30day_likes = Long()
    dislikes = Long()
    dislikes_history = Long(index=False, multi=True)
    comments = Long()
    comments_history = Long(index=False, multi=True)
    last_day_comments = Long()
    last_7day_comments = Long()
    last_30day_days_comments = Long()
    engage_rate = Long()
    sentiment = Long()
    flags = Keyword(multi=True)
    trends = Keyword(multi=True)
    channel_subscribers = Long()

    class History:
        all = ("views", "likes", "dislikes", "comments")


class VideoSectionAnalytics(BaseInnerDoc):
    """ Nested analytics section for Video document """
    direct_auth = Boolean()
    content_owner_id = Keyword()
    gender = Object(enabled=False)
    age_group = Object(enabled=False)
    demographics = Object(enabled=False)
    country = Object(enabled=False)
    traffic_sources = Object(enabled=False)
    comments = Object(enabled=False)
    views = Object(enabled=False)
    likes = Object(enabled=False)
    dislikes = Object(enabled=False)
    minutes_watched = Object(enabled=False)
    gender_male = Double()
    gender_female = Double()
    age_group_13_17 = Double()
    age_group_18_24 = Double()
    age_group_25_34 = Double()
    age_group_35_44 = Double()
    age_group_45_54 = Double()
    age_group_55_64 = Double()
    age_group_65_ = Double()


class VideoCaptionsItem(InnerDoc):
    text = Text(index=False)
    name = Text(index=False)
    language_code = Text(index=False)
    status = Text(index=False)
    caption_id = Text(index=False)
    youtube_updated_at = Date(index=False)


class VideoSectionTranscripts(BaseInnerDoc):
    items = Object(VideoCaptionsItem, multi=True)


class VideoSectionMonetization(BaseInnerDoc):
    """ Nested monetization section for Video document """
    is_monetizable = Boolean()
    channel_preferred = Boolean()
    ptk = Text()


class VideoSectionAdsStats(BaseInnerDoc):
    """ Nested adwords stats section for Video document """
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


class VideoSectionCMS(BaseInnerDoc):
    """ Nested CMS section for Video document """
    content_owner_id = Keyword()


class Video(BaseDocument):
    general_data = Object(VideoSectionGeneralData)
    stats = Object(VideoSectionStats)
    analytics = Object(VideoSectionAnalytics)
    transcripts = Object(VideoSectionTranscripts)
    monetization = Object(VideoSectionMonetization)
    ads_stats = Object(VideoSectionAdsStats)
    cms = Object(VideoSectionCMS)
    channel = Object(VideoSectionChannel)

    analytics_schedule = Object(Schedule)
    transcripts_schedule = Object(Schedule)

    class Index:
        name = VIDEO_INDEX_NAME

    class Meta:
        doc_type = VIDEO_DOC_TYPE

    def populate_general_data(self, **kwargs):
        self._populate_section(Sections.GENERAL_DATA, **kwargs)

    def populate_stats(self, **kwargs):
        self._populate_section(Sections.STATS, **kwargs)

    def populate_analytics(self, **kwargs):
        self._populate_section(Sections.ANALYTICS, **kwargs)

    def populate_monetization(self, **kwargs):
        self._populate_section(Sections.MONETIZATION, **kwargs)

    def populate_transcripts(self, **kwargs):
        self._populate_section(Sections.TRANSCRIPTS, **kwargs)

    def populate_ads_stats(self, **kwargs):
        self._populate_section(Sections.ADS_STATS, **kwargs)

    def populate_cms(self, **kwargs):
        self._populate_section(Sections.CMS, **kwargs)

    def populate_channel(self, **kwargs):
        self._populate_section(Sections.CHANNEL, **kwargs)
