from es_components.constants import Sections
from es_components.managers.base import BaseManager
from es_components.models.transcript import Transcript
from es_components.query_builder import QueryBuilder


class TranscriptManager(BaseManager):

    allowed_sections = BaseManager.allowed_sections \
                       + (Sections.GENERAL_DATA, Sections.VIDEO, Sections.TEXT)

    model = Transcript

    def get_by_video_ids(self, video_ids: list):
        query = QueryBuilder().build().must().terms().field(f"{Sections.VIDEO}.id").value(video_ids).get()
        return self.search(query=query)
