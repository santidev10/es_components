from elasticsearch_dsl import Boolean
from elasticsearch_dsl import Date
from elasticsearch_dsl import Double
from elasticsearch_dsl import Float
from elasticsearch_dsl import Integer
from elasticsearch_dsl import Keyword
from elasticsearch_dsl import Long
from elasticsearch_dsl import Object
from elasticsearch_dsl import Text

from es_components.config import CHANNEL_DOC_TYPE
from es_components.config import CHANNEL_INDEX_NAME
from es_components.config import CHANNEL_INDEX_PREFIX
from es_components.constants import Sections
from es_components.models.base import BaseDocument
from es_components.models.base import BaseInnerDoc
from es_components.models.base import BaseInnerDocWithHistory
from es_components.models.base import CommonSectionAnalytics
from es_components.models.base import Schedule


class ChannelSectionGeneralData(BaseInnerDoc):
    """ Nested general data section for Channel document """
    title = Text()
    description = Text()
    thumbnail_image_url = Text(index=False)
    country_code = Keyword()
    youtube_published_at = Date(index=True)
    video_tags = Keyword(index=False, multi=True)
    top_category = Keyword()
    lang_codes = Keyword(index=True, multi=True)
    top_lang_code = Keyword()
    emails = Keyword(multi=True)
    iab_categories = Keyword(multi=True)
    made_for_kids = Boolean()


class ChannelSectionStats(BaseInnerDocWithHistory):
    """ Nested statistics section for Channel document """
    fetched_at = Date(index=False)
    historydate = Date(index=False)
    last_video_published_at = Date()
    subscribers = Long()
    subscribers_history = Long(index=False, multi=True)
    subscribers_raw_history = Object(enabled=False)
    last_day_subscribers = Long(index=False)  # unused
    last_7day_subscribers = Long(index=False)  # unused
    last_30day_subscribers = Long()
    observed_videos_count = Long()
    observed_videos_count_history = Long(index=False, multi=True)
    total_videos_count = Long()
    total_videos_count_history = Long(index=False, multi=True)
    last_30day_observed_videos = Long(index=False)
    last_30day_published_videos = Long(index=False)
    last_365day_published_videos = Long(index=False)
    views = Long()
    views_history = Long(index=False, multi=True)
    views_raw_history = Object(enabled=False)
    last_day_views = Long()
    last_7day_views = Long()
    last_30day_views = Long()
    views_per_video = Double()
    views_per_video_history = Double(index=False, multi=True)
    observed_videos_likes = Long(index=False)
    observed_videos_likes_history = Long(index=False, multi=True)
    observed_videos_dislikes = Long(index=False)
    observed_videos_dislikes_history = Long(index=False, multi=True)
    observed_videos_comments = Long(index=False)
    engage_rate = Double()
    engage_rate_history = Double(index=False, multi=True)
    sentiment = Double()
    sentiment_history = Double(index=False, multi=True)
    channel_group = Keyword()
    hidden_subscriber_count = Boolean()

    class History:
        all = (
            "subscribers", "views", "total_videos_count", "views_per_video", "observed_videos_count",
            "observed_videos_likes", "observed_videos_dislikes", "sentiment", "engage_rate"
        )

    class RawHistory:
        all = ("subscribers", "views")


class ChannelSectionMonetization(BaseInnerDoc):
    """ Nested monetization section for Channel document """
    is_monetizable = Boolean()
    rate = Double(index=False)
    preferred = Boolean(index=False)


class ChannelSectionSocial(BaseInnerDoc):
    """ Nested social section for Channel document """
    facebook_link = Text(index=False)
    facebook_likes = Long()
    twitter_link = Text(index=False)
    twitter_tweets = Long(index=False)  # unused
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


class ChannelSectionCMS(BaseInnerDoc):
    """ Nested CMS section for Channel document """
    content_owner_id = Keyword(multi=True)
    cms_title = Keyword(multi=True)


class ChannelSectionCustomPropetries(BaseInnerDoc):
    emails = Keyword(multi=True, index=False)  # unused
    preferred = Boolean()
    social_links = Object(enabled=False)
    channel_group = Keyword(index=False)  # unused. copy of stats.channel_group?
    is_tracked = Boolean()


class ChannelSectionBrandSafety(BaseInnerDoc):
    """ Nested brand safety section for Channel document """
    overall_score = Long()
    videos_scored = Long(index=False)
    language = Keyword()
    categories = Object()
    rescore = Boolean()  # Flag used if should be rescored by brand safety script
    limbo_status = Boolean() # Flag used if vetting should be reviewed
    pre_limbo_score = Integer()  # Brand safety script score


class ChannelSectionAuth(BaseInnerDoc):
    pass


class ChannelSectionSimilar(BaseInnerDoc):
    no_cluster = Keyword(multi=True, index=False)
    above = Keyword(multi=True, index=False)
    below = Keyword(multi=True, index=False)
    default = Keyword(multi=True, index=False)


class ChannelSectionTaskUsData(BaseInnerDoc):
    """ Nested TaskUs Data Section for Channel document """
    is_safe = Boolean()
    is_user_generated_content = Boolean()
    scalable = Boolean()
    language = Keyword()
    lang_code = Keyword(index=True)
    iab_categories = Keyword(multi=True)
    age_group = Keyword()
    channel_type = Keyword()
    content_type = Keyword()
    content_quality = Keyword()
    gender = Keyword()
    brand_safety = Keyword(multi=True)
    last_vetted_at = Date()
    mismatched_language = Boolean()


class Channel(BaseDocument):
    general_data = Object(ChannelSectionGeneralData)
    stats = Object(ChannelSectionStats)
    analytics = Object(CommonSectionAnalytics)
    monetization = Object(ChannelSectionMonetization)
    social = Object(ChannelSectionSocial)
    ads_stats = Object(ChannelSectionAdsStats)
    cms = Object(ChannelSectionCMS)
    custom_properties = Object(ChannelSectionCustomPropetries)
    brand_safety = Object(ChannelSectionBrandSafety)
    auth = Object(ChannelSectionAuth)
    similar_channels = Object(ChannelSectionSimilar)
    task_us_data = Object(ChannelSectionTaskUsData)

    general_data_schedule = Object(Schedule)
    stats_schedule = Object(Schedule)
    analytics_schedule = Object(Schedule)
    ads_stats_schedule = Object(Schedule)

    class Index:
        name = CHANNEL_INDEX_NAME
        prefix = CHANNEL_INDEX_PREFIX
        settings = dict()

    class Meta:
        doc_type = CHANNEL_DOC_TYPE

    def to_dict(self, include_meta=False, skip_empty=True):
        """
        By default es_dsl.Document ignores skip_empty flag for inner documents.
        If video_tags contains a list of values in the database, and we would like
        to put an empty array instead of existing list, default implementation
        will not serialize empty array.
        We cannot override existing value with empty array, without direct call.
        """
        res_dict = super(Channel, self).to_dict(include_meta=include_meta, skip_empty=skip_empty)
        if isinstance(self.general_data, ChannelSectionGeneralData) and "_source" in res_dict:
            # Default constructor from dict creates: elasticsearch_dsl.utils.AttrDict,
            # While reading from ES generates: es_components.models.channel.ChannelSectionGeneralData
            # Thus we need to check type beforehand.
            # pylint: disable=unexpected-keyword-arg
            res_dict["_source"]["general_data"] = self.general_data.to_dict(skip_empty=skip_empty)
            res_dict["retry_on_conflict"] = 3  # VIQ2-161: Trying to fix: BulkIndexError
            # pylint: enable=unexpected-keyword-arg
        return res_dict

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

    def populate_auth(self, **kwargs):
        self._populate_section(Sections.AUTH, **kwargs)

    def populate_custom_properties(self, **kwargs):
        self._populate_section(Sections.CUSTOM_PROPERTIES, **kwargs)

    def populate_brand_safety(self, **kwargs):
        self._populate_section(Sections.BRAND_SAFETY, **kwargs)

    def populate_similar_channels(self, **kwargs):
        self._populate_section(Sections.SIMILAR_CHANNELS, **kwargs)

    def populate_task_us_data(self, **kwargs):
        self._populate_section(Sections.TASK_US_DATA, **kwargs)
