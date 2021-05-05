from es_components.constants import Sections
from es_components.managers.base import BaseManager
from es_components.models.video_language import VideoLanguage
from es_components.query_builder import QueryBuilder


class VideoLanguageManager(BaseManager):

    allowed_sections = BaseManager.allowed_sections \
                       + (Sections.GENERAL_DATA, Sections.VIDEO,
                          Sections.TITLE_LANG_DATA, Sections.DESCRIPTION_LANG_DATA)

    model = VideoLanguage
