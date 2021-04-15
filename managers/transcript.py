from es_components.constants import Sections
from es_components.managers.base import BaseManager
from es_components.models.transcript import Transcript


class TranscriptManager(BaseManager):

    allowed_sections = BaseManager.allowed_sections \
                       + (Sections.GENERAL_DATA, Sections.VIDEO, Sections.TEXT)

    model = Transcript
