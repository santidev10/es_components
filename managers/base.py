from collections import OrderedDict
import statistics
from typing import Type

from elasticsearch.helpers import bulk
from elasticsearch_dsl import MultiSearch
from elasticsearch_dsl import Q
from elasticsearch_dsl import connections

from es_components.config import ES_BULK_REFRESH_OPTION
from es_components.config import ES_CHUNK_SIZE
from es_components.config import ES_REQUEST_LIMIT
from es_components.connections import init_es_connection
from es_components.constants import EsDictFields
from es_components.constants import FORCED_FILTER_OUDATED_DAYS
from es_components.constants import MAIN_ID_FIELD
from es_components.constants import Sections
from es_components.constants import SortDirections
from es_components.constants import TimestampFields
from es_components.datetime_service import datetime_service
from es_components.exceptions import DataModelNotSpecified
from es_components.exceptions import SectionsNotAllowed
from es_components.models.base import BaseDocument
from es_components.query_builder import QueryBuilder
from es_components.utils import chunks

AGGREGATION_COUNT_SIZE = 100000
AGGREGATION_PERCENTS = tuple(range(10, 100, 10))


class BaseManager:
    """
    allowed_sections - a tuple of allowed sections name
    model - class of ES data model
    """
    allowed_sections = (Sections.MAIN, Sections.DELETED,)
    model: Type[BaseDocument] = None
    forced_filter_oudated_days = FORCED_FILTER_OUDATED_DAYS
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
        self.model._index.delete()
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
                refresh=ES_BULK_REFRESH_OPTION
            )

    def _search(self):
        return self.model.search().source(self.sections)

    def search(self, query=None, filters=None, sort=None, limit=10000, offset=None):
        search = self._search()
        if query:
            search = search.query(query)
        if filters and isinstance(filters, list):
            search = search.query(Q("bool", filter=filters))
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
                entry_dict[EsDictFields.DOC][section] = \
                    update_timestamp(entry_dict[EsDictFields.SOURCE].get(section), now)

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
        doc_type = entry._doc_type.name
        doc_mapping = entry._doc_type.mapping.to_dict()[doc_type][EsDictFields.PROPERTIES]
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
        field_updated_at = f"{Sections.MAIN}.{TimestampFields.UPDATED_AT}"
        filter_range = QueryBuilder().build().must().range().field(field_updated_at) \
            .gt(updated_at).get()
        return self.filter_alive() & filter_range

    def search_nonexistent_section_records(self, ids=None, id_field=MAIN_ID_FIELD,
                                           exclude_ids=None, exclude_id_field=None, limit=10000):
        control_section = self._get_control_section()
        field_updated_at = f"{control_section}.{TimestampFields.UPDATED_AT}"

        _filter_nonexistent_section = self._filter_nonexistent_section(control_section)

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
        return self.search(query=_query, filters=_filter_nonexistent_section, sort=_sort, limit=limit)

    def search_outdated_records(self, outdated_at, ids=None, id_field=MAIN_ID_FIELD,
                                exclude_ids=None, exclude_id_field=None, limit=10000):
        control_section = self._get_control_section()
        field_updated_at = f"{control_section}.{TimestampFields.UPDATED_AT}"

        _filter_outdated = QueryBuilder().build().must().range().field(field_updated_at) \
            .lt(outdated_at).get()

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
        return self.search(query=_query, filters=_filter_outdated, sort=_sort, limit=limit)

    def get_never_updated(self, ids=None, id_field=MAIN_ID_FIELD, exclude_ids=None, exclude_id_field=None,
                          limit=10000, extract_hits=True):
        search = self.search_nonexistent_section_records(
            ids=ids,
            id_field=id_field,
            exclude_ids=exclude_ids,
            exclude_id_field=exclude_id_field,
            limit=limit,
        )
        if not extract_hits:
            return search
        entries = search.execute().hits
        return entries

    def get_outdated(self, outdated_at, ids=None, id_field=MAIN_ID_FIELD, exclude_ids=None, exclude_id_field=None,
                     limit=10000, extract_hits=True):
        search = self.search_outdated_records(
            outdated_at,
            ids=ids,
            id_field=id_field,
            exclude_ids=exclude_ids,
            exclude_id_field=exclude_id_field,
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
        return aggregations_result

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
