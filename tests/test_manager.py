from unittest import TestCase

from elasticsearch_dsl import Object

from es_components.connections import init_es_connection
from es_components.managers.base import BaseManager
from elasticsearch_dsl import Keyword
from es_components.models.base import BaseDocument
from es_components.models.base import BaseInnerDoc


class ESManagerTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        init_es_connection()
        TestDoc.init()

    def test_dont_populate_field(self):
        item = TestManager().get_or_create(["123"])[0]

        self.assertRaises(ValueError, item.populate_section, "section_1", a=1)


class TestSection1(BaseInnerDoc):
    dfe = Keyword()


class TestDoc(BaseDocument):
    section_1 = Object(TestSection1)

    def populate_section(self, *args, **kwargs):
        return self._populate_section(*args, **kwargs)

    class Index:
        name = "test_documents"

    class Meta:
        doc_type = "test_document"


class TestManager(BaseManager):
    allowed_sections = BaseManager.allowed_sections + ("section_1",)
    model = TestDoc
