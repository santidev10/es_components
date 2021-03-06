from elasticsearch_dsl import Boolean
from elasticsearch_dsl import Date
from elasticsearch_dsl import Double
from elasticsearch_dsl import Float
from elasticsearch_dsl import InnerDoc
from elasticsearch_dsl import Integer
from elasticsearch_dsl import Keyword
from elasticsearch_dsl import Long
from elasticsearch_dsl import Object
from elasticsearch_dsl import Text

from es_components.config import VIDEO_DOC_TYPE
from es_components.config import VIDEO_INDEX_NAME
from es_components.config import VIDEO_INDEX_PREFIX
from es_components.constants import Sections
from es_components.models.base import BaseDocument
from es_components.models.base import BaseInnerDoc
from es_components.models.base import BaseInnerDocWithHistory
from es_components.models.base import Schedule
from es_components.models.base import CommonSectionAnalytics


class VideoSectionGeneralData(BaseInnerDoc):
    """ Nested general data section for Video document """
    title = Text()
    description = Text()
    thumbnail_image_url = Text(index=False)
    country_code = Keyword()
    tags = Keyword(multi=True)
    youtube_published_at = Date()
    category = Keyword()
    lang_code = Keyword()
    primary_lang_code = Keyword()
    duration = Long(index=True)
    license = Keyword(index=False)  # unused
    is_streaming = Boolean(index=False)
    iab_categories = Keyword(multi=True)
    age_restricted = Boolean(index=True)
    made_for_kids = Boolean()
    primary_category = Keyword()


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
    views_raw_history = Object(enabled=False)
    last_day_views = Long()
    last_7day_views = Long()
    last_30day_views = Long()
    views_per_day = Double()
    likes = Long()
    likes_history = Long(index=False, multi=True)
    likes_raw_history = Object(enabled=False)
    last_day_likes = Long(index=False)
    last_7day_likes = Long(index=False)
    last_30day_likes = Long(index=False)
    dislikes = Long()
    dislikes_history = Long(index=False, multi=True)
    dislikes_raw_history = Object(enabled=False)
    comments = Long()
    comments_history = Long(index=False, multi=True)
    comments_raw_history = Object(enabled=False)
    last_day_comments = Long(index=False)
    last_7day_comments = Long(index=False)
    last_30day_days_comments = Long(index=False)
    engage_rate = Double()
    engage_rate_history = Double(index=False, multi=True)
    sentiment = Long()
    sentiment_history = Long(index=False, multi=True)
    flags = Keyword(multi=True)
    channel_subscribers = Long()

    class History:
        all = ("views", "likes", "dislikes", "comments", "sentiment", "engage_rate")

    class RawHistory:
        all = ("views", "likes", "dislikes", "comments")


class VideoCaptionsItem(InnerDoc):
    text = Text(index=False)
    name = Text(index=False)
    language_code = Text(index=False)
    status = Text(index=False)
    caption_id = Text(index=False)
    youtube_updated_at = Date(index=False)


class VideoSectionCaptions(BaseInnerDoc):
    items = Object(VideoCaptionsItem, multi=True, enabled=False)


class VideoCustomCaptionsItem(BaseInnerDoc):
    text = Text(index=False)
    language_code = Text()
    source = Keyword()
    is_asr = Boolean()


class VideoSectionCustomCaptions(BaseInnerDoc):
    transcripts_checked_v2 = Boolean()
    watson_job_id = Keyword()
    items = Object(VideoCustomCaptionsItem, multi=True, enabled=True)
    transcripts_checked_tts_url = Boolean()
    has_transcripts = Boolean()


class VideoSectionMonetization(BaseInnerDoc):
    """ Nested monetization section for Video document """
    is_monetizable = Boolean()
    channel_preferred = Boolean()
    ptk = Text(index=False)


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
    video_quartile_25_rate = Double(index=False)
    video_quartile_25_rate_bottom = Double(index=False)
    video_quartile_25_rate_top = Double(index=False)
    video_quartile_50_rate = Double(index=False)
    video_quartile_50_rate_bottom = Double(index=False)
    video_quartile_50_rate_top = Double(index=False)
    video_quartile_75_rate = Double(index=False)
    video_quartile_75_rate_bottom = Double(index=False)
    video_quartile_75_rate_top = Double(index=False)
    video_quartile_100_rate = Double()
    video_quartile_100_rate_bottom = Double(index=False)
    video_quartile_100_rate_top = Double(index=False)
    all_conversions = Double(index=False)
    conversions = Double(index=False)
    view_through_conversions = Long(index=False)
    # max cpc bid value
    cpc_bid = Float(index=False)
    cpc_bid_top = Float(index=False)
    cpc_bid_bottom = Float(index=False)
    # max cpm bid value
    cpm_bid = Float(index=False)
    cpm_bid_top = Float(index=False)
    cpm_bid_bottom = Float(index=False)
    average_cpm = Float()
    average_cpm_top = Float(index=False)
    average_cpm_bottom = Float(index=False)
    average_cpc = Float(index=False)
    average_cpc_top = Float(index=False)
    average_cpc_bottom = Float(index=False)


class VideoSectionCMS(BaseInnerDoc):
    """ Nested CMS section for Video document """
    content_owner_id = Keyword()
    cms_title = Keyword()


class VideoSectionBrandSafety(BaseInnerDoc):
    """ Nested brand safety section for Video document """
    overall_score = Long()
    transcript_language = Keyword(index=False)
    categories = Object()
    rescore = Boolean()  # Flag used if should be rescored by brand safety script
    limbo_status = Boolean()  # Flag used if vetting should be reviewed
    pre_limbo_score = Integer()  # Brand safety script score


class VideoSectionTaskUsData(BaseInnerDoc):
    """ Nested TaskUs Data Section for Video document """
    is_safe = Boolean()
    is_user_generated_content = Boolean()
    scalable = Boolean()
    lang_code = Keyword(index=True)
    language = Keyword()
    iab_categories = Keyword(multi=True)
    age_group = Keyword()
    content_type = Keyword()
    content_quality = Keyword()
    gender = Keyword()
    brand_safety = Keyword(multi=True)
    last_vetted_at = Date()
    mismatched_language = Boolean()


class VideoSectionCustomProperties(BaseInnerDoc):
    """ Nested Custom Properties section for Video document"""
    blocklist = Boolean()


class Video(BaseDocument):
    general_data = Object(VideoSectionGeneralData)
    stats = Object(VideoSectionStats)
    analytics = Object(CommonSectionAnalytics)
    captions = Object(VideoSectionCaptions)
    monetization = Object(VideoSectionMonetization)
    ads_stats = Object(VideoSectionAdsStats)
    cms = Object(VideoSectionCMS)
    channel = Object(VideoSectionChannel)
    brand_safety = Object(VideoSectionBrandSafety)
    custom_captions = Object(VideoSectionCustomCaptions)
    task_us_data = Object(VideoSectionTaskUsData)
    custom_properties = Object(VideoSectionCustomProperties)

    analytics_schedule = Object(Schedule)
    captions_schedule = Object(Schedule)
    ads_stats_schedule = Object(Schedule)

    class Index:
        name = VIDEO_INDEX_NAME
        prefix = VIDEO_INDEX_PREFIX
        settings = dict()

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

    def populate_captions(self, **kwargs):
        self._populate_section(Sections.CAPTIONS, **kwargs)

    def populate_ads_stats(self, **kwargs):
        self._populate_section(Sections.ADS_STATS, **kwargs)

    def populate_cms(self, **kwargs):
        self._populate_section(Sections.CMS, **kwargs)

    def populate_channel(self, **kwargs):
        self._populate_section(Sections.CHANNEL, **kwargs)

    def populate_brand_safety(self, **kwargs):
        self._populate_section(Sections.BRAND_SAFETY, **kwargs)

    def populate_custom_captions(self, **kwargs):
        self._populate_section(Sections.CUSTOM_CAPTIONS, **kwargs)

    def populate_task_us_data(self, **kwargs):
        self._populate_section(Sections.TASK_US_DATA, **kwargs)

    def populate_custom_properties(self, **kwargs):
        self._populate_section(Sections.CUSTOM_PROPERTIES, **kwargs)
