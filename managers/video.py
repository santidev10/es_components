from es_components.constants import Sections
from es_components.constants import VIDEO_CHANNEL_ID_FIELD
from es_components.constants import MAIN_ID_FIELD
from es_components.constants import SortDirections
from es_components.constants import TimestampFields
from es_components.constants import CONTENT_OWNER_ID_FIELD
from es_components.managers.base import BaseManager
from es_components.models.video import Video
from es_components.query_builder import QueryBuilder


class VideoManager(BaseManager):
    allowed_sections = BaseManager.allowed_sections\
                       + (Sections.GENERAL_DATA, Sections.STATS, Sections.ANALYTICS,
                          Sections.CHANNEL, Sections.TRANSCRIPTS, Sections.MONETIZATION,
                          Sections.ADS_STATS, Sections.CMS, Sections.ANALYTICS_SCHEDULE,
                          Sections.TRANSCRIPTS_SCHEDULE)
    model = Video

    def get_all_video_ids(self, channel_id):
        return list(self.get_all_video_ids_generator(channel_id))

    def get_all_video_ids_generator(self, channel_id):
        _query = self.by_channel_ids_query(channel_id)
        videos_generator = self.model.search().source(Sections.MAIN).query(_query).scan()
        yield from (video.main.id for video in videos_generator)

    def by_channel_ids_query(self, channels_ids, invert=False):
        query = QueryBuilder().build()
        query = query.must_not() if invert else query.must()
        query = query.terms() if isinstance(channels_ids, list) else query.term()
        query = query.field(VIDEO_CHANNEL_ID_FIELD).value(channels_ids)
        values = query.get()
        return values

    def by_content_owner_ids_query(self, content_owner_ids):
        query = QueryBuilder().build().must()
        query = query.terms() if isinstance(content_owner_ids, list) else query.term()
        query = query.field(CONTENT_OWNER_ID_FIELD).value(content_owner_ids)
        values = query.get()
        return values

    def forced_filters(self):
        return super(VideoManager, self).forced_filters() &\
               self._filter_existent_section(Sections.GENERAL_DATA)

    def get_never_updated_generator(self, outdated_at, never_updated_section, channels_ids=None,
                                    content_owner_ids=None):

        control_section = self._get_control_section()
        field_updated_at = f"{control_section}.{TimestampFields.UPDATED_AT}"

        _filter_outdated = QueryBuilder().build().must().range().field(field_updated_at).lt(outdated_at).get()
        _filter_nonexistent_section = self._filter_nonexistent_section(control_section)
        _filter_never_updated_section = self._filter_nonexistent_section(never_updated_section)

        if channels_ids is not None and content_owner_ids is None:
            _filter = self.by_channel_ids_query(channels_ids)
        elif content_owner_ids is not None and channels_ids is None:
            _filter = self.by_content_owner_ids_query(content_owner_ids)
        else:
            raise AttributeError("One of two parameters must be specified: channel_id OR content_owner_id")

        _filter &= _filter_never_updated_section & _filter_outdated | _filter_nonexistent_section

        _sort = [
            {field_updated_at: {"order": SortDirections.ASCENDING}},
            {MAIN_ID_FIELD: {"order": SortDirections.ASCENDING}},
        ]

        yield from self.search(filters=_filter, sort=_sort).scan()

    def get_total_count_for_channels(self, channels_ids):
        search = self.search(query=self.by_channel_ids_query(channels_ids))
        result = search.execute()
        total = result.hits.total
        return total

    def get_total_count_for_content_owners(self, content_owner_ids):
        search = self.search(query=self.by_content_owner_ids_query(content_owner_ids))
        result = search.execute()
        total = result.hits.total
        return total

    def _search_nonexistent_section_records(self, query, limit):
        control_section = self._get_control_section()
        field_updated_at = f"{control_section}.{TimestampFields.UPDATED_AT}"

        filter_nonexistent_section = self._filter_nonexistent_section(control_section)

        sort = [
            {field_updated_at: {"order": SortDirections.ASCENDING}},
            {MAIN_ID_FIELD: {"order": SortDirections.ASCENDING}},
        ]

        records = self.search(query=query, filters=filter_nonexistent_section, sort=sort, limit=limit)
        return records

    def search_nonexistent_section_records_by_channel_id(self, channel_id=None, limit=10000):
        query = self.by_channel_ids_query(channel_id) if channel_id is not None else None
        records = self._search_nonexistent_section_records(query=query, limit=limit)
        return records

    def search_nonexistent_section_records_by_content_owner_id(self, content_owner_id=None, limit=10000):
        query = self.by_content_owner_ids_query(content_owner_id) if content_owner_id is not None else None
        records = self._search_nonexistent_section_records(query=query, limit=limit)
        return records
