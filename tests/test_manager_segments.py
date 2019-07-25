import itertools
from unittest import TestCase

from es_components.connections import init_es_connection
from es_components.constants import MAIN_ID_FIELD
from es_components.constants import Sections
from es_components.managers.base import BaseManager
from es_components.models.base import BaseDocument
from es_components.query_builder import QueryBuilder


class ESManagerSegmentsBaseTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        init_es_connection()

    def setUp(self):
        TestSegmentManager().truncate()
        TestSegmentDoc.init()
        self.manager_main = TestSegmentManager(sections=(Sections.MAIN,))
        self.manager_segments = TestSegmentManager(sections=(Sections.SEGMENTS,))


class ESManagerSegmentsAddTestCase(ESManagerSegmentsBaseTestCase):

    def test_add_segment_to_existing_without_section(self):
        item_id = generate_item_id()
        segment_id = generate_segment_id()
        items = self.manager_main.get_or_create([item_id])
        self.manager_main.upsert(items)

        query = QueryBuilder().build().must().terms().field(MAIN_ID_FIELD).value([item_id]).get()
        self.manager_segments.add_to_segment(filter_query=query, segment_uuid=segment_id)

        item = self.manager_segments.get([item_id])[0]
        self.assertEqual([segment_id], item.segments.uuid)

    def test_add_segment_to_existing_with_sections(self):
        item_id = generate_item_id()
        segment_id = generate_segment_id()
        items = self.manager_segments.get_or_create([item_id])
        self.manager_segments.upsert(items)

        query = QueryBuilder().build().must().terms().field(MAIN_ID_FIELD).value([item_id]).get()
        self.manager_segments.add_to_segment(filter_query=query, segment_uuid=segment_id)

        item = self.manager_segments.get([item_id])[0]
        self.assertEqual([segment_id], item.segments.uuid)

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
    def test_remove_segment_missing_segment(self):
        item_id = generate_item_id()
        segment_id = generate_segment_id()
        items = self.manager_main.get_or_create([item_id])
        self.manager_main.upsert(items)

        query = QueryBuilder().build().must().terms().field(MAIN_ID_FIELD).value([item_id]).get()
        self.manager_segments.remove_from_segment(filter_query=query, segment_uuid=segment_id)

        self.assertEqual([], self.manager_segments.get([item_id])[0].segments.uuid)

    def test_remove_segment_missing_entity(self):
        item_id = generate_item_id()
        segment_id = generate_segment_id()

        query = QueryBuilder().build().must().terms().field(MAIN_ID_FIELD).value([item_id]).get()
        self.manager_segments.remove_from_segment(filter_query=query, segment_uuid=segment_id)

        self.assertEqual([], self.manager_main.get([item_id], skip_none=True))

    def test_remove_segment_existing(self):
        item_id = generate_item_id()
        segment_id = generate_segment_id()
        item = self.manager_segments.get_or_create([item_id])[0]
        item.populate_segments(uuid=[segment_id])
        self.manager_segments.upsert([item])

        query = QueryBuilder().build().must().terms().field(MAIN_ID_FIELD).value([item_id]).get()
        self.manager_segments.remove_from_segment(filter_query=query, segment_uuid=segment_id)

        item = self.manager_segments.get([item_id])[0]
        self.assertEqual([], item.segments.uuid)

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
    def test_add_to_segment_by_ids(self):
        item_id_1 = generate_item_id()
        item_id_2 = generate_item_id()
        segment_id = generate_segment_id()

        self.manager_segments.add_to_segment_by_ids(ids=[item_id_1, item_id_2], segment_uuid=segment_id)

        self.assertEqual([segment_id], self.manager_segments.get([item_id_1])[0].segments.uuid)
        self.assertEqual([segment_id], self.manager_segments.get([item_id_2])[0].segments.uuid)


class TestSegmentDoc(BaseDocument):
    class Index:
        name = "test_segment_documents"

    class Meta:
        doc_type = "test_segment_document"


class TestSegmentManager(BaseManager):
    allowed_sections = BaseManager.allowed_sections
    model = TestSegmentDoc


int_iterator = itertools.count(1, 1)


def generate_segment_id():
    return generate_id("segment")


def generate_item_id():
    return generate_id("document")


def generate_id(entity_name):
    return f"{entity_name}:{next(int_iterator)}"
