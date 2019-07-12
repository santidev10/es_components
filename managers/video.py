from es_components.constants import Sections
from es_components.constants import VIDEO_CHANNEL_ID_FIELD
from es_components.constants import FilterOperators
from es_components.constants import MAIN_ID_FIELD
from es_components.constants import SortDirections
from es_components.constants import TimestampFields
from es_components.constants import CONTENT_OWNER_ID_FIELD
from es_components.managers.base import BaseManager
from es_components.models.video import Video


class VideoManager(BaseManager):
    allowed_sections = BaseManager.allowed_sections\
                       + (Sections.GENERAL_DATA, Sections.STATS, Sections.ANALYTICS, Sections.CHANNEL,
                          Sections.TRANSCRIPTS, Sections.MONETIZATION, Sections.ADS_STATS, Sections.CMS,
                          Sections.ANALYTICS_SCHEDULE, Sections.TRANSCRIPTS_SCHEDULE)
    model = Video

    def get_all_video_ids(self, channel_id):
        _query = self.by_channel_ids_query(channel_id)
        videos = self.model.search().source(Sections.MAIN).query(_query).scan()
        return [video.main.id for video in videos]

    def by_channel_not_equal_ids_query(self, channels_ids):
        return self.filter_term(VIDEO_CHANNEL_ID_FIELD, channels_ids, not_equal=True)

    def by_channel_ids_query(self, channels_ids):
        return self.filter_term(VIDEO_CHANNEL_ID_FIELD, channels_ids)

    def by_content_owner_ids_query(self, content_owner_ids):
        return self.filter_term(CONTENT_OWNER_ID_FIELD, content_owner_ids)

    def forced_filters(self, updated_at=None):
        return super(VideoManager, self).forced_filters(updated_at) &\
               self.filter_existent_section(Sections.GENERAL_DATA)

    def get_never_updated(self, outdated_at, never_updated_section, ids=None, limit=10000):
        control_section = self._get_control_section()
        field_updated_at = f"{control_section}.{TimestampFields.UPDATED_AT}"

        _filter_outdated = self.filter_range(field_updated_at, FilterOperators.LESS_THAN, outdated_at)
        _filter_nonexistent_section = self.filter_nonexistent_section(control_section)
        _filter_never_updated_section = self.filter_nonexistent_section(never_updated_section)

        _filter = _filter_never_updated_section & _filter_outdated | _filter_nonexistent_section

        _query = self.ids_query(ids)

        _sort = [
            {field_updated_at: {"order": SortDirections.ASCENDING}},
            {MAIN_ID_FIELD: {"order": SortDirections.ASCENDING}},
        ]

        return self.search(queries=_query, filters=_filter, sort=_sort, limit=limit).execute().hits

    def aggregation_avg_videos_per_channel(self, search=None):

        if not search:
            search = self._search()

        search.aggs.bucket("video_per_channel", "terms", field=VIDEO_CHANNEL_ID_FIELD)
        search.aggs.pipeline("avg", "avg_bucket", buckets_path="video_per_channel>_count")

        aggregations_result = search.execute().aggregations
        video_per_channel = aggregations_result.video_per_channel.buckets

        channels = [_video_per_channel.key for _video_per_channel in video_per_channel]
        avg_video_per_channel = aggregations_result.avg.value

        return channels, avg_video_per_channel

