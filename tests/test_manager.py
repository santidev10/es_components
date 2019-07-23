from unittest import TestCase
from unittest.mock import patch

from elasticsearch_dsl import Object

from elasticsearch_dsl import Keyword
from es_components.managers.base import BaseManager
from es_components.models.base import BaseDocument
from es_components.models.base import BaseInnerDoc


class ESManagerTestCase(TestCase):
    def test_dont_populate_field(self):
        test_id = "123"
        with patch("es_components.tests.test_manager.TestDoc.mget", return_value=[TestDoc(id=test_id)]):
            item = TestManager().get_or_create([test_id])[0]

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
