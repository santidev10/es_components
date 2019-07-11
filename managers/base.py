from datetime import datetime

from elasticsearch.helpers import bulk
from elasticsearch_dsl import connections
from elasticsearch_dsl import MultiSearch
from elasticsearch_dsl import Q

from es_components.constants import ES_DICT_FIELDS
from es_components.constants import FILTER_INCLUDE_EMPTY
from es_components.constants import FILTER_OPERATORS
from es_components.constants import MAIN_ID_FIELD
from es_components.constants import SECTIONS
from es_components.constants import SORT_DIRECTIONS
from es_components.constants import TIMESTAMP_FIELDS

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
    allowed_sections = (SECTIONS.MAIN, SECTIONS.DELETED,)
    model = None

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

        if SECTIONS.MAIN not in sections:
            sections += (SECTIONS.MAIN,)

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
                entries[i] = self.model(id=ids[i])

        return entries

    def truncate(self):
        """ Recreate index with deleting all documents. """
        self.model._index.delete()
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

    def search(self, query=None, filter=None, sort=None, limit=10000, offset=None):
        search = self._search()
        if query:
            search = search.query(query)
        if filter:
            search = search.filter(filter)
        if sort:
            search = search.sort(*sort)
        return search[offset:limit]

    def multi_search(self, searches):
        ms = MultiSearch(index=self.model._index._name)
        ms._searches = searches
        return ms.execute()


    def _upsert_generator(self, entries):
        """ Generator to create a dict from entity for upsertion.

        Controls that only sections field will be upserted.

        :param entries: a list of model objects
        """
        def update_timestamp(_entry_dict, timestamp):
            """ Update datetime created_at(if it is None) and updated_at to passed timestamp. """
            if not _entry_dict:
                _entry_dict = {}

            if _entry_dict.get(TIMESTAMP_FIELDS.CREATED_AT) is None:
                _entry_dict[TIMESTAMP_FIELDS.CREATED_AT] = timestamp
            _entry_dict[TIMESTAMP_FIELDS.UPDATED_AT] = timestamp

            return _entry_dict

        now = datetime.utcnow()

        for entry in entries:
            entry_dict = entry.to_dict(include_meta=True, skip_empty=False)
            entry_dict[ES_DICT_FIELDS.DOC] = {}

            for section in self.sections:
                entry_dict[ES_DICT_FIELDS.DOC][section] = \
                    update_timestamp(entry_dict[ES_DICT_FIELDS.SOURCE].get(section), now)

            entry_dict[ES_DICT_FIELDS.OP_TYPE] = "update"
            entry_dict[ES_DICT_FIELDS.DOC_AS_UPSERT] = True
            del entry_dict[ES_DICT_FIELDS.SOURCE]
            try:
                del entry_dict[ES_DICT_FIELDS.VERSION]
            except KeyError:
                pass

            yield entry_dict

    def _get_control_section(self):
        return self.sections[0]

    def _filter_range(self, field, operator, value):
        _filter = {
            "range": {
                field: {
                    operator: value
                }
            }
        }
        return Q(_filter)

    def _filter_nonexistent_section(self, section):
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

    def _filter_existent_section(self, section):
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

    def _filter_term(self, field, values, not_equal=False):
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

    def ids_query(self, ids):
        return self._filter_term(MAIN_ID_FIELD, ids)

    def ids_not_equal_query(self, ids):
        return self._filter_term(MAIN_ID_FIELD, ids, not_equal=True)

    def filter_alive(self):
        return self._filter_nonexistent_section(SECTIONS.DELETED)

    def forced_filters(self, updated_at):
        field_updated_at = f"{SECTIONS.MAIN}.{TIMESTAMP_FIELDS.UPDATED_AT}"
        return self.filter_alive() & \
               self._filter_range(field_updated_at, FILTER_OPERATORS.GREATER_THAN, updated_at)

    def search_nonexistent_section_records(self, ids=None, limit=10000):
        control_section = self._get_control_section()
        field_updated_at = f"{control_section}.{TIMESTAMP_FIELDS.UPDATED_AT}"

        _filter_nonexistent_section = self._filter_nonexistent_section(control_section)

        _query = self.ids_query(ids) if ids is not None else None

        _sort = [
            {field_updated_at: {"order": SORT_DIRECTIONS.ASCENDING}},
            {MAIN_ID_FIELD: {"order": SORT_DIRECTIONS.ASCENDING}},
        ]
        return self.search(query=_query, filter=_filter_nonexistent_section, sort=_sort, limit=limit)

    def search_outdated_records(self, outdated_at, ids=None, limit=10000):
        control_section = self._get_control_section()
        field_updated_at = f"{control_section}.{TIMESTAMP_FIELDS.UPDATED_AT}"

        _filter_outdated = self._filter_range(field_updated_at, FILTER_OPERATORS.LESS_THAN, outdated_at)

        _query = self.ids_query(ids) if ids is not None else None

        _sort = [
            {field_updated_at: {"order": SORT_DIRECTIONS.ASCENDING}},
            {MAIN_ID_FIELD: {"order": SORT_DIRECTIONS.ASCENDING}},
        ]
        return self.search(query=_query, filter=_filter_outdated, sort=_sort, limit=limit)

    def get_outdated(self, outdated_at, include_empty=FILTER_INCLUDE_EMPTY.NO, ids=None, limit=10000):
        if include_empty not in FILTER_INCLUDE_EMPTY.ALL:
            raise ValueError

        entries = []
        if include_empty == FILTER_INCLUDE_EMPTY.FIRST:
            entries += self.search_nonexistent_section_records(ids=ids, limit=limit).execute().hits

        limit_remaining = limit - len(entries)
        if limit_remaining > 0:
            entries +=  self.search_outdated_records(outdated_at, ids=ids, limit=limit_remaining).execute().hits

        limit_remaining = limit - len(entries)
        if limit_remaining > 0 and include_empty == FILTER_INCLUDE_EMPTY.LAST:
            entries += self.search_nonexistent_section_records(ids=ids, limit=limit_remaining).execute().hits

        return entries

    def get_by_forced_filter(self):
        updated_at = datetime.utcnow().date()

        forced_filter = self.forced_filters(updated_at)

        return self.search(filter=forced_filter).execute().hits
