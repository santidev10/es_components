import itertools
from contextlib import contextmanager
from datetime import datetime
from datetime import timedelta
from unittest.mock import patch

from elasticsearch_dsl import Keyword
from elasticsearch_dsl import Object

from es_components.connections import init_es_connection
from es_components.constants import MAIN_ID_FIELD
from es_components.constants import Sections
from es_components.datetime_service import datetime_service
from es_components.managers.base import BaseManager
from es_components.models.base import BaseDocument
from es_components.models.base import BaseInnerDoc
from es_components.query_builder import QueryBuilder
from es_components.tests.utils import ESTestCase


class ESManagerSegmentsBaseTestCase(ESTestCase):
    @classmethod
    def setUpClass(cls):
        init_es_connection()

    def setUp(self):
        try:
            TestSegmentManager().truncate()
        except:
            pass
        TestSegmentDoc.init()
        self.manager_main = TestSegmentManager(sections=(Sections.MAIN,))
        self.manager_segments = TestSegmentManager(sections=(Sections.SEGMENTS,))

    @contextmanager
    def patch_now(self, dt):
        with patch.object(datetime_service, "now", return_value=dt):
            yield


class ESManagerSegmentsAddTestCase(ESManagerSegmentsBaseTestCase):

    def test_add_segment_to_existing_without_section(self):
        test_datetime = datetime(2020, 1, 2, 12, 2, 3)
        item_id = generate_item_id()
        segment_id = generate_segment_id()
        items = self.manager_main.get_or_create([item_id])
        self.manager_main.upsert(items)

        query = QueryBuilder().build().must().terms().field(MAIN_ID_FIELD).value([item_id]).get()
        with self.patch_now(test_datetime):
            self.manager_segments.add_to_segment(filter_query=query, segment_uuid=segment_id)

        item = self.manager_segments.get([item_id])[0]
        self.assertEqual([segment_id], item.segments.uuid)
        self.assertEqual(test_datetime, item.segments.created_at)
        self.assertEqual(test_datetime, item.segments.updated_at)

    def test_add_segment_to_existing_with_sections(self):
        test_created_at = datetime(2020, 1, 2, 12, 2, 3)
        test_updated_at = test_created_at + timedelta(days=2, minutes=3)
        item_id = generate_item_id()
        segment_id = generate_segment_id()
        items = self.manager_segments.get_or_create([item_id])
        with self.patch_now(test_created_at):
            self.manager_segments.upsert(items)

        query = QueryBuilder().build().must().terms().field(MAIN_ID_FIELD).value([item_id]).get()
        with self.patch_now(test_updated_at):
            self.manager_segments.add_to_segment(filter_query=query, segment_uuid=segment_id)

        item = self.manager_segments.get([item_id])[0]
        self.assertEqual([segment_id], item.segments.uuid)
        self.assertEqual(test_created_at, item.segments.created_at)
        self.assertEqual(test_updated_at, item.segments.updated_at)

    def test_add_segment_to_missing(self):
        item_id = generate_item_id()
        segment_id = generate_segment_id()

        query = QueryBuilder().build().must().terms().field(MAIN_ID_FIELD).value([item_id]).get()
        self.manager_segments.add_to_segment(filter_query=query, segment_uuid=segment_id)

        self.assertEqual([], self.manager_main.get([item_id], skip_none=True))

    def test_add_segment_only_filtered(self):
        item_id_1 = generate_item_id()
        item_id_2 = generate_item_id()
        segment_id = generate_segment_id()
        item = self.manager_main.get_or_create([item_id_1])[0]
        self.manager_main.upsert([item])

        query = QueryBuilder().build().must().terms().field(MAIN_ID_FIELD).value([item_id_2]).get()
        self.manager_segments.add_to_segment(filter_query=query, segment_uuid=segment_id)

        self.assertEqual({}, self.manager_segments.get([item_id_1], skip_none=True)[0].segments)

    def test_add_segment_duplicate(self):
        item_id = generate_item_id()
        segment_id = generate_segment_id()
        item = self.manager_segments.get_or_create([item_id])[0]
        item.populate_segments(uuid=[segment_id])
        self.manager_segments.upsert([item])

        query = QueryBuilder().build().must().terms().field(MAIN_ID_FIELD).value([item_id]).get()
        self.manager_segments.add_to_segment(filter_query=query, segment_uuid=segment_id)

        item = self.manager_segments.get([item_id])[0]
        self.assertEqual([segment_id], item.segments.uuid)

    def test_add_segment_keep_multiple_segments(self):
        item_id = generate_item_id()
        segment_id_1 = generate_segment_id()
        segment_id_2 = generate_segment_id()
        item = self.manager_segments.get_or_create([item_id])[0]
        item.populate_segments(uuid=[segment_id_1])
        self.manager_segments.upsert([item])

        query = QueryBuilder().build().must().terms().field(MAIN_ID_FIELD).value([item_id]).get()
        self.manager_segments.add_to_segment(filter_query=query, segment_uuid=segment_id_2)

        item = self.manager_segments.get([item_id])[0]
        self.assertEqual(
            sorted([segment_id_1, segment_id_2]),
            sorted(item.segments.uuid)
        )

    def test_add_segment_update_multiple_items(self):
        item_id_1 = generate_item_id()
        item_id_2 = generate_item_id()
        segment_id = generate_segment_id()
        items = self.manager_segments.get_or_create([item_id_1, item_id_2])
        for item in items:
            item.populate_segments(uuid=[])
        self.manager_segments.upsert(items)

        query = QueryBuilder().build().must().exists().field(MAIN_ID_FIELD).get()
        self.manager_segments.add_to_segment(filter_query=query, segment_uuid=segment_id)

        self.assertEqual([segment_id], self.manager_segments.get([item_id_1])[0].segments.uuid)
        self.assertEqual([segment_id], self.manager_segments.get([item_id_2])[0].segments.uuid)


class ESManagerSegmentsRemoveTestCase(ESManagerSegmentsBaseTestCase):
    def test_remove_segment_missing_entity(self):
        item_id = generate_item_id()
        segment_id = generate_segment_id()

        query = QueryBuilder().build().must().terms().field(MAIN_ID_FIELD).value([item_id]).get()
        self.manager_segments.remove_from_segment(filter_query=query, segment_uuid=segment_id)

        self.assertEqual([], self.manager_main.get([item_id], skip_none=True))

    def test_remove_segment_from_existing_without_section(self):
        test_datetime = datetime(2020, 1, 2, 12, 2, 3)
        item_id = generate_item_id()
        segment_id = generate_segment_id()
        items = self.manager_main.get_or_create([item_id])
        self.manager_main.upsert(items)

        query = QueryBuilder().build().must().terms().field(MAIN_ID_FIELD).value([item_id]).get()
        with self.patch_now(test_datetime):
            self.manager_segments.remove_from_segment(filter_query=query, segment_uuid=segment_id)

        item = self.manager_segments.get([item_id])[0]
        self.assertEqual([], item.segments.uuid)
        self.assertEqual(test_datetime, item.segments.created_at)
        self.assertEqual(test_datetime, item.segments.updated_at)

    def test_remove_segment_from_existing_with_section(self):
        test_created_at = datetime(2020, 1, 2, 12, 2, 3)
        test_updated_at = test_created_at + timedelta(days=2, minutes=3)
        item_id = generate_item_id()
        segment_id = generate_segment_id()
        item = self.manager_segments.get_or_create([item_id])[0]
        item.populate_segments(uuid=[segment_id])
        with self.patch_now(test_created_at):
            self.manager_segments.upsert([item])

        query = QueryBuilder().build().must().terms().field(MAIN_ID_FIELD).value([item_id]).get()
        with self.patch_now(test_updated_at):
            self.manager_segments.remove_from_segment(filter_query=query, segment_uuid=segment_id)

        item = self.manager_segments.get([item_id])[0]
        self.assertEqual([], item.segments.uuid)
        self.assertEqual(test_created_at, item.segments.created_at)
        self.assertEqual(test_updated_at, item.segments.updated_at)

    def test_remove_duplicates(self):
        item_id = generate_item_id()
        segment_id = generate_segment_id()
        item = self.manager_segments.get_or_create([item_id])[0]
        item.populate_segments(uuid=[segment_id, segment_id])
        self.manager_segments.upsert([item])

        query = QueryBuilder().build().must().terms().field(MAIN_ID_FIELD).value([item_id]).get()
        self.manager_segments.remove_from_segment(filter_query=query, segment_uuid=segment_id)

        item = self.manager_segments.get([item_id])[0]
        self.assertEqual([], item.segments.uuid)

    def test_remove_only_filtered(self):
        item_id_1 = generate_item_id()
        item_id_2 = generate_item_id()
        segment_id = generate_segment_id()
        items = self.manager_segments.get_or_create([item_id_1, item_id_2])
        items[0].populate_segments(uuid=[segment_id])
        items[1].populate_segments(uuid=[segment_id])
        self.manager_segments.upsert(items)

        query = QueryBuilder().build().must().terms().field(MAIN_ID_FIELD).value([item_id_1]).get()
        self.manager_segments.remove_from_segment(filter_query=query, segment_uuid=segment_id)

        self.assertEqual([], self.manager_segments.get([item_id_1])[0].segments.uuid)
        self.assertEqual([segment_id], self.manager_segments.get([item_id_2])[0].segments.uuid)

    def test_remove_from_multiple(self):
        item_id = generate_item_id()
        segment_id_1 = generate_segment_id()
        segment_id_2 = generate_segment_id()
        item = self.manager_segments.get_or_create([item_id])[0]
        item.populate_segments(uuid=[segment_id_1, segment_id_2])
        self.manager_segments.upsert([item])

        query = QueryBuilder().build().must().terms().field(MAIN_ID_FIELD).value([item_id]).get()
        self.manager_segments.remove_from_segment(filter_query=query, segment_uuid=segment_id_1)

        self.assertEqual([segment_id_2], self.manager_segments.get([item_id])[0].segments.uuid)


class ESManagerSegmentsAddByIdsTestCase(ESManagerSegmentsBaseTestCase):
    def test_add_to_segment_by_ids_creates_multiple(self):
        item_id_1 = generate_item_id()
        item_id_2 = generate_item_id()
        segment_id = generate_segment_id()

        self.manager_segments.add_to_segment_by_ids(ids=[item_id_1, item_id_2], segment_uuid=segment_id)

        self.assertEqual([segment_id], self.manager_segments.get([item_id_1])[0].segments.uuid)
        self.assertEqual([segment_id], self.manager_segments.get([item_id_2])[0].segments.uuid)

    def test_add_to_segment_by_ids_does_not_replace_existing_sections(self):
        test_datetime = datetime(2020, 1, 2, 12, 2, 3)
        item_id = generate_item_id()
        custom_manager = TestSegmentManager(("custom_section",))
        test_property = "test_custom_property_value"
        item = custom_manager.get_or_create([item_id])[0]
        item.populate_custom_section(
            test_property=test_property
        )
        with self.patch_now(test_datetime):
            custom_manager.upsert([item])
        segment_id = generate_segment_id()

        self.manager_segments.add_to_segment_by_ids(ids=[item_id], segment_uuid=segment_id)

        item = custom_manager.get([item_id])[0]
        self.assertEqual(test_property, item.custom_section.test_property)
        self.assertEqual(test_datetime, item.custom_section.created_at)

    def test_add_to_segment_by_ids_does_not_perform_get(self):
        item_id_1 = generate_item_id()
        item_id_2 = generate_item_id()
        segment_id = generate_segment_id()

        with patch.object(BaseManager, "get") as get_mock:
            self.manager_segments.add_to_segment_by_ids(ids=[item_id_1, item_id_2], segment_uuid=segment_id)

            get_mock.assert_not_called()


class CustomSection(BaseInnerDoc):
    test_property = Keyword()


class TestSegmentDoc(BaseDocument):
    custom_section = Object(CustomSection)

    def populate_custom_section(self, **kwargs):
        return self._populate_section("custom_section", **kwargs)

    class Index:
        name = "test_segment_documents"
        prefix = "test_segment_documents_"

    class Meta:
        doc_type = "test_segment_document"


class TestSegmentManager(BaseManager):
    allowed_sections = BaseManager.allowed_sections + ("custom_section",)
    model = TestSegmentDoc


int_iterator = itertools.count(1, 1)


def generate_segment_id():
    return generate_id("segment")


def generate_item_id():
    return generate_id("document")


def generate_id(entity_name):
    return f"{entity_name}:{next(int_iterator)}"
