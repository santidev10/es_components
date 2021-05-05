from elasticsearch_dsl import Boolean
from elasticsearch_dsl import Date
from elasticsearch_dsl import Integer
from elasticsearch_dsl import Keyword
from elasticsearch_dsl import Object

from es_components.config import VIDEO_LANGUAGE_DOC_TYPE
from es_components.config import VIDEO_LANGUAGE_INDEX_NAME
from es_components.config import VIDEO_LANGUAGE_INDEX_PREFIX
from es_components.constants import Sections
from es_components.models.base import BaseDocument
from es_components.models.base import BaseInnerDoc


class VideoLanguageSectionVideo(BaseInnerDoc):
    """
    Section to store the Video foreign key
    """
    # pylint: disable=invalid-name
    id = Keyword()
    # pylint: enable=invalid-name


class VideoLanguageSectionLanguageDetails(BaseInnerDoc):
    """
    Section to store the processed data for language detection of one language in a video section
    """
    lang_name = Keyword()
    lang_code = Keyword()
    confidence = Integer()


class VideoLanguageSectionGeneralData(BaseInnerDoc):
    """
    Section to store all other data related to the video language detection
    """
    lang_codes = Keyword(multi=True)
    primary_lang_details = Object(VideoLanguageSectionLanguageDetails, enabled=False)
    processed_at = Date()


class VideoLanguageSectionLanguageData(BaseInnerDoc):
    """
    Section to store the processed data for language detection of a video section
    """
    items = Object(VideoLanguageSectionLanguageDetails, multi=True, enabled=False)
    is_reliable = Boolean()


class VideoLanguage(BaseDocument):
    """
    Model for storing Video Language Detection data. A Video has one VideoLanguage
    """
    video = Object(VideoLanguageSectionVideo)
    general_data = Object(VideoLanguageSectionGeneralData)
    title_lang_data = Object(VideoLanguageSectionLanguageData)
    description_lang_data = Object(VideoLanguageSectionLanguageData)

    class Index:
        name = VIDEO_LANGUAGE_INDEX_NAME
        prefix = VIDEO_LANGUAGE_INDEX_PREFIX
        settings = dict()

    class Meta:
        doc_type = VIDEO_LANGUAGE_DOC_TYPE

    def populate_general_data(self, **kwargs):
        self._populate_section(Sections.GENERAL_DATA, **kwargs)

    def populate_video(self, **kwargs):
        self._populate_section(Sections.VIDEO, **kwargs)

    def populate_title_lang_data(self, **kwargs):
        self._populate_section(Sections.TITLE_LANG_DATA, **kwargs)

    def populate_description_lang_data(self, **kwargs):
        self._populate_section(Sections.DESCRIPTION_LANG_DATA, **kwargs)
