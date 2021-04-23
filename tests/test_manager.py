import itertools

from elasticsearch_dsl import Keyword
from elasticsearch_dsl import Object

from es_components.managers.base import BaseManager
from es_components.models.base import BaseDocument
from es_components.models.base import BaseInnerDoc
from es_components.tests.utils import ESTestCase


int_iterator = itertools.count(1, 1)


class ESManagerTestCase(ESTestCase):
    def test_dont_populate_field(self):
        test_id = "123"
        item = TestManager().get_or_create([test_id])[0]

        self.assertRaises(ValueError, item.populate_section, "section_1", a=1)

    def test_dont_update_extra_fields(self):
        item = TestDoc("123")

        extra_field = "a"

        self.assertRaises(ValueError, item.section_1.update, **{extra_field: None})
        self.assertFalse(hasattr(item.section_1, extra_field))

    def test_upsert_ignore_update_time_sections(self):
        manager = TestManager()
        ignore_sections = ["main"]
        with self.subTest("Adds updated_at timestamp if newly creating even if ignoring section"):
            item = TestDoc(f"id_{next(int_iterator)}")
            manager.upsert([item], ignore_update_time_sections=ignore_sections)
            updated = manager.get([item.main.id])[0]
            self.assertEqual(updated.main.created_at, updated.main.updated_at)

        with self.subTest("Does not update updated_at with ignore sections for existing documents"):
            item = manager.get_or_create(f"id_{next(int_iterator)}")[0]
            manager.upsert([item], ignore_update_time_sections=ignore_sections)
            updated = manager.get([item.main.id])[0]
            self.assertNotEqual(item.main.created_at, updated.main.updated_at)

        with self.subTest("Updates updated_at with no ignore update sections"):
            item = manager.get_or_create(f"id_{next(int_iterator)}")[0]
            manager.upsert([item], ignore_update_time_sections=None)
            updated = manager.get([item.main.id])[0]
            self.assertTrue(item.main.created_at < updated.main.updated_at)


class TestSection1(BaseInnerDoc):
    dfe = Keyword()


class TestDoc(BaseDocument):
    section_1 = Object(TestSection1)

    def populate_section(self, *args, **kwargs):
        return self._populate_section(*args, **kwargs)

    class Index:
        name = "test_documents"
        prefix = "test_documents_"

    class Meta:
        doc_type = "test_document"


class TestManager(BaseManager):
    allowed_sections = BaseManager.allowed_sections + ("section_1",)
    model = TestDoc
