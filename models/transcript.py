from elasticsearch_dsl import Boolean
from elasticsearch_dsl import Date
from elasticsearch_dsl import Integer
from elasticsearch_dsl import Keyword
from elasticsearch_dsl import Object
from elasticsearch_dsl import Text

from es_components.config import TRANSCRIPT_DOC_TYPE
from es_components.config import TRANSCRIPT_INDEX_NAME
from es_components.config import TRANSCRIPT_INDEX_PREFIX
from es_components.constants import Sections
from es_components.models.base import BaseDocument
from es_components.models.base import BaseInnerDoc


class TranscriptSectionVideo(BaseInnerDoc):
    """
    Section to store the Video foreign key
    """
    # pylint: disable=invalid-name
    id = Keyword()
    # pylint: enable=invalid-name


class TranscriptSectionGeneralData(BaseInnerDoc):
    """
    Section to store all other data related to the transcript
    """
    language_code = Text()
    source_type = Keyword()
    is_asr = Boolean()
    processor_version = Integer()
    processed_at = Date()


class TranscriptSectionText(BaseInnerDoc):
    """
    Section to store the processed (xml and other markup removed) raw transcript text
    """
    value = Text(index=False)


class Transcript(BaseDocument):
    """
    Model for storing Video Transcript/Caption data. A Video has many Transcripts
    """
    text = Object(TranscriptSectionText)
    video = Object(TranscriptSectionVideo)
    general_data = Object(TranscriptSectionGeneralData)

    class Index:
        name = TRANSCRIPT_INDEX_NAME
        prefix = TRANSCRIPT_INDEX_PREFIX
        settings = dict()

    class Meta:
        doc_type = TRANSCRIPT_DOC_TYPE

    def populate_general_data(self, **kwargs):
        self._populate_section(Sections.GENERAL_DATA, **kwargs)

    def populate_video(self, **kwargs):
        self._populate_section(Sections.VIDEO, **kwargs)

    def populate_text(self, **kwargs):
        self._populate_section(Sections.TEXT, **kwargs)
