from collections import OrderedDict
import os
import re
import statistics
from typing import Type

from elasticsearch import NotFoundError
from elasticsearch.helpers import bulk
from elasticsearch_dsl import MultiSearch
from elasticsearch_dsl import connections

from es_components.config import ES_BULK_REFRESH_OPTION
from es_components.config import ES_CHUNK_SIZE
from es_components.config import ES_REQUEST_LIMIT
from es_components.config import ES_MAX_CHUNK_BYTES
from es_components.connections import init_es_connection
from es_components.constants import EsDictFields
from es_components.constants import FORCED_FILTER_OUDATED_DAYS
from es_components.constants import MAIN_ID_FIELD
from es_components.constants import SEGMENTS_UUID_FIELD
from es_components.constants import Sections
from es_components.constants import SortDirections
from es_components.constants import TimestampFields
from es_components.datetime_service import datetime_service
from es_components.exceptions import DataModelNotSpecified
from es_components.exceptions import SectionsNotAllowed
from es_components.iab_categories import TOP_LEVEL_CATEGORIES
from es_components.models.base import BaseDocument
from es_components.monitor import Monitor
from es_components.monitor import Warnings
from es_components.query_builder import QueryBuilder
from es_components.utils import chunks
from es_components.countries import COUNTRIES

AGGREGATION_COUNT_SIZE = 100000
AGGREGATION_PERCENTS = tuple(range(10, 100, 10))

# pylint: disable=too-many-public-methods
class BaseManager:
    """
    allowed_sections - a tuple of allowed sections name
    model - class of ES data model
    """
    allowed_sections = (Sections.MAIN, Sections.DELETED, Sections.SEGMENTS)
    model: Type[BaseDocument] = None
    forced_filter_oudated_days = FORCED_FILTER_OUDATED_DAYS
    forced_filter_section_oudated = Sections.MAIN
    range_aggregation_fields = ()
    count_aggregation_fields = ()
    percentiles_aggregation_fields = ()
    count_exists_aggregation_fields = ()
    count_missing_aggregation_fields = ()

    def __init__(self, sections=None, upsert_sections=None):
        """ Initialize manager.

        :param sections: tuple of sections name. If sections is not specified,
        manager will work only with MAIN section.

        The first section in the *sections* list is the control section.
        The control section is used to search. For example, in .get_outdated() method.
        """
        if not self.model:
            raise DataModelNotSpecified("Data Model is not specified")

        self.sections = self._init_sections(sections)
        self.upsert_sections = self._init_sections(upsert_sections or sections)

        try:
            connections.connections.get_connection()
        except KeyError:
            init_es_connection()

    def _init_sections(self, sections):
        if sections is None:
            sections = ()

        elif isinstance(sections, str):
            sections = (sections,)

        if sections and not set(sections).issubset(set(self.allowed_sections)):
            raise SectionsNotAllowed("Cannot find such section in Data Model sections")

        if Sections.MAIN not in sections:
            sections += (Sections.MAIN,)

        return sections

    def get(self, ids, skip_none=False):
        """ Retrieve model entities.

        :param ids: a list of ids
        :param skip_none: determine if None value should be skipped
        :return: list of entities
        """

        entities = []

        for _ids in chunks(ids, ES_REQUEST_LIMIT):
            entities += self.model.mget(list(_ids), _source=self.sections)

        if skip_none and None in entities:
            entities = [entity for entity in entities if entity is not None]

        return entities

    def get_or_create(self, ids, only_new=False):
        """ Retrieve or create(if is not exists) model entities.

        :param ids: a list of ids
        :param only_created: return new entries only
        :return: list of entities
        """
        if not ids:
            return []

        entries = self.get(ids)
        new_ids = []
        for i, entry in enumerate(entries):
            if entry is None:
                new_ids.append(i)
                # false positive pylint error
                # pylint: disable=not-callable
                entries[i] = self.model(id=ids[i])
                # pylint: enable=not-callable

        if only_new:
            entries = [entries[i] for i in new_ids]

        return entries

    def truncate(self):
        """ Recreate index with deleting all documents. """
        # pylint: disable=protected-access
        try:
            self.model._index.delete()
        except NotFoundError:
            pass
        # pylint: enable=protected-access
        self.model.init()

    def delete(self, ids):
        """ Delete entities.

        :param ids: a list of ids
        """

        for _ids in chunks(ids, ES_REQUEST_LIMIT):
            self.model.search().query("ids", values=list(_ids)).delete()


    def upsert(self, entries):
        """ Upsert a list of entries.

        :param entries: a list of model objects
        """

        for _entries in chunks(entries, ES_REQUEST_LIMIT):
            bulk(
                connections.get_connection(),
                self._upsert_generator(_entries),
                chunk_size=ES_CHUNK_SIZE,
                refresh=ES_BULK_REFRESH_OPTION,
                max_chunk_bytes=ES_MAX_CHUNK_BYTES
            )

    def _search(self):
        return self.model.search().source(self.sections)

    def search(self, query=None, filters=None, sort=None, limit=10000, offset=None):
        search = self._search()
        if query:
            search = search.query(query)
        if filters and isinstance(filters, list):
            for es_filter in filters:
                search = search.query(es_filter)
        elif filters:
            search = search.filter(filters)
        if sort:
            search = search.sort(*sort)
        return search[offset:limit]

    def scan(self, filters):
        yield from self.search(filters=filters).scan()

    def multi_search(self, searches):
        # pylint: disable=protected-access
        multi_search = MultiSearch(index=self.model._index._name)
        multi_search._searches = searches
        # pylint: enable=protected-access
        return multi_search.execute()

    def _upsert_generator(self, entries):
        """ Generator to create a dict from entity for upsertion.

        Controls that only sections field will be upserted.

        :param entries: a list of model objects
        """

        def update_timestamp(_entry_dict, timestamp):
            """ Update datetime created_at(if it is None) and updated_at to passed timestamp. """
            if not _entry_dict:
                _entry_dict = {}

            timestamp_created_at = _entry_dict.get(TimestampFields.CREATED_AT)
            _entry_dict[TimestampFields.CREATED_AT] = timestamp if timestamp_created_at is None \
                else datetime_service.localize(timestamp_created_at)

            _entry_dict[TimestampFields.UPDATED_AT] = timestamp

            return _entry_dict

        now = datetime_service.now()

        for entry in entries:
            entry_dict = entry.to_dict(include_meta=True, skip_empty=False)
            entry_dict[EsDictFields.DOC] = {}

            for section in self.upsert_sections:
                entry_dict[EsDictFields.DOC][section] = update_timestamp(
                    entry_dict[EsDictFields.SOURCE].get(section),
                    now
                )

                self._drop_invalid_field_from_section_dict(entry, entry_dict, section)

            entry_dict[EsDictFields.OP_TYPE] = "update"
            entry_dict[EsDictFields.DOC_AS_UPSERT] = True
            del entry_dict[EsDictFields.SOURCE]
            try:
                del entry_dict[EsDictFields.VERSION]
            except KeyError:
                pass

            yield entry_dict

    def _drop_invalid_field_from_section_dict(self, entry, entry_dict, section):
        # pylint: disable=protected-access
        doc_mapping = entry._doc_type.mapping.to_dict()[EsDictFields.PROPERTIES]
        # pylint: enable=protected-access

        section_mapping = doc_mapping[section][EsDictFields.PROPERTIES].keys()

        invalid_fields = entry_dict[EsDictFields.DOC][section].keys() - section_mapping

        for invalid_field in invalid_fields:
            entry_dict[EsDictFields.DOC][section][invalid_field] = None

    def _get_control_section(self):
        return self.sections[0]

    def _filter_nonexistent_section(self, section):
        return QueryBuilder().build().must_not().exists().field(section).get()

    def _filter_existent_section(self, section):
        return QueryBuilder().build().must().exists().field(section).get()

    def ids_query(self, ids, id_field=MAIN_ID_FIELD, exclude_ids=None, exclude_id_field=None):
        query = QueryBuilder().build().must().terms().field(id_field).value(ids).get()
        if exclude_ids is not None:
            query &= QueryBuilder().build().must_not().terms().field(exclude_id_field).value(exclude_ids).get()
        return query

    def ids_not_equal_query(self, ids, id_field=MAIN_ID_FIELD):
        return QueryBuilder().build().must_not().terms().field(id_field).value(ids).get()

    def filter_alive(self):
        return self._filter_nonexistent_section(Sections.DELETED)

    def forced_filters(self):
        # "now-1d/d" time format is used
        # it avoids being tied to the current point in time and makes it possible to cache request/response
        outdated_seconds = self.forced_filter_oudated_days * 86400
        updated_at = f"now-{outdated_seconds}s/s"
        field_updated_at = f"{self.forced_filter_section_oudated}.{TimestampFields.UPDATED_AT}"
        filter_range = QueryBuilder().build().must().range().field(field_updated_at) \
            .gt(updated_at).get()
        return self.filter_alive() & filter_range

    def search_nonexistent_section_records(self, ids=None, id_field=MAIN_ID_FIELD,
                                           exclude_ids=None, exclude_id_field=None, ignore_deleted=None, limit=10000):
        control_section = self._get_control_section()
        field_updated_at = f"{control_section}.{TimestampFields.UPDATED_AT}"

        _filters = [self._filter_nonexistent_section(control_section)]

        # to ignore items without main id field
        _filters.append(self._filter_existent_section(MAIN_ID_FIELD))

        if ignore_deleted is True:
            _filters.append(self.filter_alive())

        _query = None
        if ids or exclude_ids:
            _query = self.ids_query(
                ids=ids,
                id_field=id_field,
                exclude_ids=exclude_ids,
                exclude_id_field=exclude_id_field,
            )

        _sort = [
            {field_updated_at: {"order": SortDirections.ASCENDING}},
            {MAIN_ID_FIELD: {"order": SortDirections.ASCENDING}},
        ]
        return self.search(query=_query, filters=_filters, sort=_sort, limit=limit)

    def search_outdated_records(self, outdated_at, ids=None, id_field=MAIN_ID_FIELD,
                                exclude_ids=None, exclude_id_field=None, ignore_deleted=None, limit=10000):
        control_section = self._get_control_section()
        field_updated_at = f"{control_section}.{TimestampFields.UPDATED_AT}"

        _filters = [QueryBuilder().build().must().range().field(field_updated_at) \
            .lt(outdated_at).get()]

        # to ignore items without main id field
        _filters.append(self._filter_existent_section(MAIN_ID_FIELD))

        if ignore_deleted is True:
            _filters.append(self.filter_alive())

        _query = None
        if ids or exclude_ids:
            _query = self.ids_query(
                ids=ids,
                id_field=id_field,
                exclude_ids=exclude_ids,
                exclude_id_field=exclude_id_field,
            )

        _sort = [
            {field_updated_at: {"order": SortDirections.ASCENDING}},
            {MAIN_ID_FIELD: {"order": SortDirections.ASCENDING}},
        ]
        return self.search(query=_query, filters=_filters, sort=_sort, limit=limit)

    def get_never_updated(self, ids=None, id_field=MAIN_ID_FIELD, exclude_ids=None, exclude_id_field=None,
                          limit=10000, extract_hits=True, ignore_deleted=True):
        search = self.search_nonexistent_section_records(
            ids=ids,
            id_field=id_field,
            exclude_ids=exclude_ids,
            exclude_id_field=exclude_id_field,
            ignore_deleted=ignore_deleted,
            limit=limit,
        )
        if not extract_hits:
            return search
        entries = search.execute().hits
        return entries

    def get_outdated(self, outdated_at, ids=None, id_field=MAIN_ID_FIELD, exclude_ids=None, exclude_id_field=None,
                     limit=10000, extract_hits=True, ignore_deleted=True):
        search = self.search_outdated_records(
            outdated_at,
            ids=ids,
            id_field=id_field,
            exclude_ids=exclude_ids,
            exclude_id_field=exclude_id_field,
            ignore_deleted=ignore_deleted,
            limit=limit,
        )
        if not extract_hits:
            return search
        entries = search.execute().hits
        return entries

    def get_by_forced_filter(self):
        forced_filter = self.forced_filters()

        return self.search(filters=forced_filter).execute().hits

    def _get_range_aggs(self):
        range_aggs = {}

        for field in self.range_aggregation_fields:
            range_aggs["{}:min".format(field)] = {
                "min": {"field": field}
            }
            range_aggs["{}:max".format(field)] = {
                "max": {"field": field}
            }
        return range_aggs

    def _get_count_aggs(self):
        count_aggs = {}

        for field in self.count_aggregation_fields:
            count_aggs[field] = {
                "terms": {
                    "size": AGGREGATION_COUNT_SIZE,
                    "field": field,
                    "min_doc_count": 1,
                }
            }

        count_aggs["brand_safety"] = {
            "range": {
                "field": "brand_safety.overall_score",
                "ranges": [
                    {"from": 0, "to": 69.1},
                    {"from": 70, "to": 79.1},
                    {"from": 80, "to": 89.1},
                    {"from": 90, "to": 100.1},
                ]
            }
        }
        return count_aggs

    def _get_count_exists_aggs_result(self, search, properties=None):
        properties = properties or self.count_exists_aggregation_fields + self.count_missing_aggregation_fields
        filters = {
            **{
                f"{field}:exists": self._filter_existent_section(field)
                for field in self.count_exists_aggregation_fields
            },
            **{
                f"{field}:missing": self._filter_nonexistent_section(field)
                for field in self.count_missing_aggregation_fields
            }
        }

        result = {
            key: search.filter(value).count()
            for key, value in filters.items()
            if key in properties
        }

        return result

    def get_aggregation(self, search, size=0, properties=None):
        if not properties:
            return None

        aggregation_dict = {
            **self._get_range_aggs(),
            **self._get_count_aggs(),
        }

        aggregation_dict = {
            key: value
            for key, value in aggregation_dict.items()
            if key in properties
        }

        aggregations_search = self._search().update_from_dict({
            "size": size,
            "aggs": aggregation_dict
        })
        aggregations_search.update_from_dict(search.to_dict())
        aggregations_result = aggregations_search.execute().aggregations.to_dict()
        aggregations_result = self.adapt_is_viral_aggregation(aggregations_result)
        return aggregations_result

    def adapt_country_code_aggregation(self, aggregations):
        if "general_data.country_code" in aggregations:
            for bucket in aggregations["general_data.country_code"]["buckets"]:
                try:
                    bucket["title"] = COUNTRIES[bucket["key"]][0]
                # pylint: disable=invalid-name
                # pylint: disable=broad-except
                except Exception:
                    bucket["title"] = bucket["key"]
                # pylint: enable=invalid-name
                # pylint: enable=broad-except
        return aggregations

    def adapt_is_viral_aggregation(self, aggregations):
        if "stats.is_viral" in aggregations:
            aggregations["stats.is_viral"]["buckets"][1]["key"] = "Viral"
            aggregations["stats.is_viral"]["buckets"][1].pop("key_as_string")
            del aggregations["stats.is_viral"]["buckets"][0]
        return aggregations

    def adapt_iab_categories_aggregation(self, aggregations):
        if "general_data.iab_categories" in aggregations:
            top_level_buckets = []
            buckets = aggregations["general_data.iab_categories"]["buckets"]
            for bucket in buckets:
                if bucket['key'].lower().replace(" and ", " & ") in TOP_LEVEL_CATEGORIES:
                    top_level_buckets.append(bucket)
            aggregations["general_data.iab_categories"]["buckets"] = top_level_buckets
        return aggregations

    def generate_distinct_values(self, field, pagesize=10000):
        composite = {
            "size": pagesize,
            "sources": [{
                field: {
                    "terms": {
                        "field": field
                    }
                }
            }]
        }
        while True:
            aggregations_search = self._search().update_from_dict({
                "aggs": {
                    "values": {
                        "composite": composite
                    }
                }
            })
            result = aggregations_search.execute()
            for aggregation in result["aggregations"]["values"]["buckets"]:
                yield aggregation.key[field]
            if "after_key" in result["aggregations"]["values"]:
                composite["after"] = result["aggregations"]["values"]["after_key"]
            else:
                break

    def update(self, filter_query):
        # pylint: disable=protected-access
        return self.model._index.updateByQuery().filter(filter_query)
        # pylint: enable=protected-access

    def filter_items_related_to_segments(self, segment_ids):
        """
        :param segment_ids: List[<UUID>] - list of segments uuids
        :return: query to filter items related to given kist of segments
        """
        return QueryBuilder().build() \
            .must() \
            .terms().field(SEGMENTS_UUID_FIELD) \
            .value(segment_ids).get()

    def update_monetization(self, filter_query, is_monetizable):
        if Sections.MONETIZATION not in self.upsert_sections:
            raise BrokenPipeError(f"This manager can't update {Sections.MONETIZATION} section")

        script = dict(
            source=CachedScriptsReader.get_script("update_monetization.painless"),
            params=dict(
                now=datetime_service.now().isoformat(),
                is_monetizable=is_monetizable
            )
        )
        return self.update(filter_query) \
            .script(**script) \
            .execute()

    def remove_sections(self, filter_query, sections, proceed_conflict=False):
        if not set(sections).issubset(set(self.allowed_sections)):
            raise SectionsNotAllowed("Cannot find such section in Data Model sections")

        script = dict(
            source=CachedScriptsReader.get_script("remove_sections.painless"),
            params=dict(
                sections=sections
            )
        )
        update = self.update(filter_query)\
            .script(**script)
        if proceed_conflict is True:
            update = update.params(conflicts="proceed")
        return update.execute()

    def add_to_segment(self, filter_query, segment_uuid):
        if Sections.SEGMENTS not in self.upsert_sections:
            raise BrokenPipeError(f"This manager can't update {Sections.SEGMENTS} section")
        script = dict(
            source=CachedScriptsReader.get_script("add_to_segment.painless"),
            params=dict(
                uuid=segment_uuid,
                now=datetime_service.now().isoformat(),
            )
        )
        return self.update(filter_query) \
            .script(**script) \
            .execute()

    def add_to_segment_by_ids(self, ids, segment_uuid):
        if Sections.SEGMENTS not in self.upsert_sections:
            raise BrokenPipeError(f"This manager can't update {Sections.SEGMENTS} section")
        # pylint: disable=not-callable
        items = [self.model(id=item_id) for item_id in ids]
        # pylint: enable=not-callable
        self.upsert(items)
        query = QueryBuilder().build() \
            .must() \
            .terms().field(MAIN_ID_FIELD) \
            .value(ids) \
            .get()
        return self.add_to_segment(filter_query=query, segment_uuid=segment_uuid)

    def remove_from_segment(self, filter_query, segment_uuid):
        if Sections.SEGMENTS not in self.upsert_sections:
            raise BrokenPipeError(f"This manager can't update {Sections.SEGMENTS} section")
        script = dict(
            source=CachedScriptsReader.get_script("remove_from_segment.painless"),
            params=dict(
                uuid=segment_uuid,
                now=datetime_service.now().isoformat(),
            )
        )
        return self.update(filter_query) \
            .script(**script) \
            .execute()

    @classmethod
    def fetch_percentiles(cls, field):
        number_of_shards = cls.get_number_of_shards()
        aggregations = {
            "aggs": {
                "percentiles": {
                    "field": field,
                    "percents": AGGREGATION_PERCENTS,
                }
            }
        }
        sharded_percentiles = []
        for shard in range(number_of_shards):
            result = cls.model.search() \
                .params(preference=f"_shards:{shard}") \
                .query() \
                .update_from_dict({"aggs": aggregations, "size": 0}) \
                .execute().aggregations.aggs["values"].to_dict()
            sharded_percentiles.append(result)

        def aggregate(func, shards, key):
            value = func([_shard[key] for _shard in shards])
            return value

        result_keys = [str(float(key)) for key in AGGREGATION_PERCENTS]
        percentiles = OrderedDict([
            (key, aggregate(statistics.mean, sharded_percentiles, key))
            for key in result_keys
        ])

        return percentiles

    @classmethod
    def get_number_of_shards(cls):
        # pylint: disable=protected-access
        settings = cls.model._index.get_settings()
        # pylint: enable=protected-access
        number_of_shards = None
        for _, index_settings in settings.items():
            number_of_shards = int(index_settings["settings"]["index"]["number_of_shards"])
            break
        return number_of_shards


    def _get_enabled_monitoring_warnings(self):
        return (Warnings.MainSectionNotFilled(),)

    def _get_enabled_monitoring_alerts(self):
        return ()

    def _get_enabled_monitoring_params_info(self):
        return self.sections, ()


    def get_monitoring_data(self):
        # pylint: disable=protected-access
        monitor = Monitor(self.model._index._name)
        # pylint: enable=protected-access
        data = dict(
            cluster_name=monitor.get_cluster_name(),
            warnings=monitor.get_warnings(self._get_enabled_monitoring_warnings()),
            info=monitor.get_info(*self._get_enabled_monitoring_params_info()),
            alerts=monitor.get_alerts(self._get_enabled_monitoring_alerts())
        )
        return data
# pylint: enable=too-many-public-methods


class CachedScriptsReader:
    _scripts_cache = {}
    _scripts_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "scripts")

    @classmethod
    def get_script(cls, script_name):
        if script_name not in cls._scripts_cache:
            with open(os.path.join(cls._scripts_dir, script_name), "r") as file:
                cls._scripts_cache[script_name] = re.sub(r"[\n\s]+", " ", file.read())
        return cls._scripts_cache[script_name]
