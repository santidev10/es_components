from es_components.constants import Sections
from es_components.managers.base import BaseManager
from es_components.models.video_language import VideoLanguage
from es_components.query_builder import QueryBuilder


class VideoLanguageManager(BaseManager):

    allowed_sections = BaseManager.allowed_sections \
                       + (Sections.GENERAL_DATA, Sections.VIDEO,
                          Sections.TITLE_LANG_DATA, Sections.DESCRIPTION_LANG_DATA)

    model = VideoLanguage

    def get_by_video_id(self, video_id):
        query = QueryBuilder().build().must().term().field(f"{Sections.VIDEO}.id").value(video_id).get()
        result = self.search(query=query)
        if result and len(result) > 0:
            result = result[0]
        else:
            result = None
        return result

    def get_or_create_by_video_id(self, video_id):
        result = self.get_by_video_id(video_id)
        if result is None:
            result = VideoLanguage()
            result.populate_video({id: video_id})
            self.upsert(result)
        return result
