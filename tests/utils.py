from unittest import TestCase
from unittest.mock import PropertyMock
from unittest.mock import patch

from elasticsearch import NotFoundError
from elasticsearch_dsl.connections import connections

from es_components.connections import init_es_connection
from es_components.managers.base import BaseManager
from es_components.models.base import BaseDocument


class ESTestCase(TestCase):
    _patches = []

    @classmethod
    def setUpClass(cls):
        init_es_connection()
        hosts = [item["host"] for item in connections.get_connection("default").transport.hosts]
        if any(["prod" in host for host in hosts]):
            raise ConnectionError("Testing on prod env detected")
        for model_cls in BaseDocument.__subclasses__():
            # pylint: disable=protected-access
            index_patch = patch.object(model_cls._index, "_name",
                                       new_callable=PropertyMock(return_value="test_" + model_cls.Index.name))
            # pylint: enable=protected-access
            index_patch.__enter__()
            cls._patches.append(index_patch)

    @classmethod
    def tearDownClass(cls):
        for patch_item in cls._patches:
            patch_item.__exit__()
        del cls._patches[:]

    def setUp(self):
        for manager_cls in BaseManager.__subclasses__():
            try:
                manager_cls().truncate()
            except NotFoundError:
                pass
        for model_cls in BaseDocument.__subclasses__():
            model_cls.init()
