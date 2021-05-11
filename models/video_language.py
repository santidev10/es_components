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
    The id of the VideoLanguage.main.id will be the same as the related YT video ID
    """
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

    def populate_title_lang_data(self, **kwargs):
        self._populate_section(Sections.TITLE_LANG_DATA, **kwargs)

    def populate_description_lang_data(self, **kwargs):
        self._populate_section(Sections.DESCRIPTION_LANG_DATA, **kwargs)
