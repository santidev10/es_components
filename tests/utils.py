import os
import threading
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
            index_mock = PropertyMock(return_value="test_" + model_cls.Index.name + "_" + str(threading.get_ident()))
            prefix_mock = PropertyMock(return_value="test_" + model_cls.Index.prefix)

            index_patch = patch.object(model_cls._index, "_name", new_callable=index_mock)
            prefix_patch = patch.object(model_cls.Index, "prefix", new_callable=prefix_mock)
            # pylint: enable=protected-access

            index_patch.__enter__()
            prefix_patch.__enter__()

            cls._patches.append(index_patch)
            cls._patches.append(prefix_patch)

    @classmethod
    def tearDownClass(cls):
        if os.getenv("KEEP_TEST_ES_INDEX", "0") != "1":
            cls.__remove_indexes()
        for patch_item in cls._patches:
            patch_item.__exit__()
        del cls._patches[:]

    @classmethod
    def __remove_indexes(cls):
        for model_cls in BaseDocument.__subclasses__():
            model_cls._index.delete()

    def setUp(self):
        for manager_cls in BaseManager.__subclasses__():
            try:
                manager_cls().truncate()
            except NotFoundError:
                pass
        for model_cls in BaseDocument.__subclasses__():
            model_cls.init()
