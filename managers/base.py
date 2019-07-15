from datetime import datetime

from elasticsearch.helpers import bulk
from elasticsearch_dsl import connections
from elasticsearch_dsl import MultiSearch
from elasticsearch_dsl import Q

from es_components.constants import EsDictFields
from es_components.constants import FilterIncludeEmpty
from es_components.constants import FilterOperators
from es_components.constants import MAIN_ID_FIELD
from es_components.constants import Sections
from es_components.constants import SortDirections
from es_components.constants import TimestampFields

from es_components.config import ES_BULK_REFRESH_OPTION
from es_components.config import ES_CHUNK_SIZE
from es_components.config import ES_REQUEST_LIMIT

from es_components.exceptions import DataModelNotSpecified
from es_components.exceptions import SectionsNotAllowed

from es_components.utils import chunks


class BaseManager:
    """
    allowed_sections - a tuple of allowed sections name
    model - class of ES data model
    """
    allowed_sections = (Sections.MAIN, Sections.DELETED,)
    model: Type[BaseDocument] = None

    def __init__(self, sections=None):
        """ Initialize manager.

        :param sections: tuple of sections name. If sections is not specified,
        manager will work only with MAIN section.

        The first section in the *sections* list is the control section.
        The control section is used to search. For example, in .get_outdated() method.
        """
        if not self.model:
            raise DataModelNotSpecified("Data Model is not specified")

        if sections is None:
            sections = ()
        elif type(sections) == str:
            sections = (sections,)

        if sections and not set(sections).issubset(set(self.allowed_sections)):
            raise SectionsNotAllowed("Cannot find such section in Data Model sections")

        if Sections.MAIN not in sections:
            sections += (Sections.MAIN,)

        self.sections = sections

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

    def get_or_create(self, ids):
        """ Retrieve or create(if is not exists) model entities.

        :param ids: a list of ids
        :return: list of entities
        """
        if not ids:
            return []

        entries = self.get(ids)

        for i, entry in enumerate(entries):
            if entry is None:
                # false positive pylint error
                # pylint: disable=not-callable
                entries[i] = self.model(id=ids[i])
                # pylint: enable=not-callable

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

            bulk(connections.get_connection(), self._upsert_generator(_entries),
                chunk_size=ES_CHUNK_SIZE, refresh=ES_BULK_REFRESH_OPTION)

    def _search(self):
        return self.model.search().source(self.sections)

    def search(self, query=None, filters=None, sort=None, limit=10000, offset=None):
        search = self._search()
        if query:
            search = search.query(query)
        if filters:
            search = search.filter(filters)
        if sort:
            search = search.sort(*sort)
        return search[offset:limit]

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

            if _entry_dict.get(TimestampFields.CREATED_AT) is None:
                _entry_dict[TimestampFields.CREATED_AT] = timestamp
            _entry_dict[TimestampFields.UPDATED_AT] = timestamp

            return _entry_dict

        now = datetime.utcnow()

        for entry in entries:
            entry_dict = entry.to_dict(include_meta=True, skip_empty=False)
            entry_dict[EsDictFields.DOC] = {}

            for section in self.sections:
                entry_dict[EsDictFields.DOC][section] = \
                    update_timestamp(entry_dict[EsDictFields.SOURCE].get(section), now)

            entry_dict[EsDictFields.OP_TYPE] = "update"
            entry_dict[EsDictFields.DOC_AS_UPSERT] = True
            del entry_dict[EsDictFields.SOURCE]
            try:
                del entry_dict[EsDictFields.VERSION]
            except KeyError:
                pass

            yield entry_dict

    def _get_control_section(self):
        return self.sections[0]

    @staticmethod
    def filter_range(field, gte=None, gt=None, lte=None, lt=None):
        range_value = {}

        if gte:
            range_value[FilterOperators.GREATER_EQUAL_THAN] = gte
        if gt:
            range_value[FilterOperators.GREATER_THAN] = gt
        if lte:
            range_value[FilterOperators.LESS_EQUAL_THAN] = lte
        if lt:
            range_value[FilterOperators.LESS_THAN] = lt

        _filter = {
            "range": {
                field: range_value
            }
        }
        return Q(_filter)

    @staticmethod
    def filter_nonexistent_section(section):
        _filter = {
            "bool": {
                "must_not": {
                    "exists": {
                        "field": section
                    }
                }
            }
        }
        return Q(_filter)

    @staticmethod
    def filter_existent_section(section):
        _filter = {
            "bool": {
                "must": {
                    "exists": {
                        "field": section
                    }
                }
            }
        }
        return Q(_filter)

    @staticmethod
    def filter_term(field, values, not_equal=False):
        condition = "must_not" if not_equal else "must"
        term = "terms" if type(values) == list else "term"

        _filter = {
            "bool": {
                condition: {
                    term: {field: values}
                }
            }
        }
        return Q(_filter)

    @staticmethod
    def query_regexp(field, value):
        _query = {
            "regexp": {
                field: value
            }
        }
        return Q(_query)

    @classmethod
    def ids_query(cls, ids):
        return cls.filter_term(MAIN_ID_FIELD, ids)

    @classmethod
    def ids_not_equal_query(cls, ids):
        return cls.filter_term(MAIN_ID_FIELD, ids, not_equal=True)

    @classmethod
    def filter_alive(cls):
        return cls.filter_nonexistent_section(Sections.DELETED)

    def forced_filters(self, updated_at=None):
        if not updated_at:
            updated_at = datetime.utcnow().date()

        field_updated_at = f"{Sections.MAIN}.{TimestampFields.UPDATED_AT}"
        return self.filter_alive() & self.filter_range(field_updated_at, gt=updated_at)

    def search_nonexistent_section_records(self, ids=None, limit=10000):
        control_section = self._get_control_section()
        field_updated_at = f"{control_section}.{TimestampFields.UPDATED_AT}"

        _filter_nonexistent_section = self.filter_nonexistent_section(control_section)

        _query = self.ids_query(ids) if ids is not None else None

        _sort = [
            {field_updated_at: {"order": SortDirections.ASCENDING}},
            {MAIN_ID_FIELD: {"order": SortDirections.ASCENDING}},
        ]
        return self.search(query=_query, filters=_filter_nonexistent_section, sort=_sort, limit=limit)

    def search_outdated_records(self, outdated_at, ids=None, limit=10000):
        control_section = self._get_control_section()
        field_updated_at = f"{control_section}.{TimestampFields.UPDATED_AT}"

        _filter_outdated = self.filter_range(field_updated_at, lt=outdated_at)

        _query = self.ids_query(ids) if ids is not None else None

        _sort = [
            {field_updated_at: {"order": SortDirections.ASCENDING}},
            {MAIN_ID_FIELD: {"order": SortDirections.ASCENDING}},
        ]
        return self.search(query=_query, filters=_filter_outdated, sort=_sort, limit=limit)

    def get_outdated(self, outdated_at, include_empty=FilterIncludeEmpty.NO, ids=None, limit=10000):
        if include_empty not in FilterIncludeEmpty.ALL:
            raise ValueError

        entries = []
        if include_empty == FilterIncludeEmpty.FIRST:
            entries += self.search_nonexistent_section_records(ids=ids, limit=limit).execute().hits

        limit_remaining = limit - len(entries)
        if limit_remaining > 0:
            entries +=  self.search_outdated_records(outdated_at, ids=ids, limit=limit_remaining).execute().hits

        limit_remaining = limit - len(entries)
        if limit_remaining > 0 and include_empty == FilterIncludeEmpty.LAST:
            entries += self.search_nonexistent_section_records(ids=ids, limit=limit_remaining).execute().hits

        return entries

    def get_by_forced_filter(self):
        updated_at = datetime.utcnow().date()

        forced_filter = self.forced_filters(updated_at)

        return self.search(filters=forced_filter).execute().hits

    def aggs_from_dict(self, aggregations, search=None, size=0):

        if not aggregations:
            return None

        if not search:
            search = self.model._search()

        search.update_from_dict({
            "size": size,
            "aggs": aggregations
        })
        aggregations_result = search.execute().aggregations

        return aggregations_result
