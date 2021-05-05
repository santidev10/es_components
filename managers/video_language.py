from es_components.constants import Sections
from es_components.managers.base import BaseManager
from es_components.models.video_language import VideoLanguage


class VideoLanguageManager(BaseManager):

    allowed_sections = (Sections.MAIN, Sections.GENERAL_DATA,
                        Sections.TITLE_LANG_DATA, Sections.DESCRIPTION_LANG_DATA)

    model = VideoLanguage
