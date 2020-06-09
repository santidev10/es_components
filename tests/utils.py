import os
import re
import uuid
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

    def __init__(self, *args, **kwargs):
        super(ESTestCase, self).__init__(*args, **kwargs)
        init_es_connection()
        self.__dangerous_environment_check()

    def __dangerous_environment_check(self):
        hosts = [item["host"] for item in connections.get_connection("default").transport.hosts]
        dangerous_hosts = (
            r"prod",
            r"rc",
        )
        if any([any([re.search(host_mask, host) for host_mask in dangerous_hosts]) for host in hosts]):
            raise ConnectionError("Testing on dangerous ElasticSearch host detected")

    @classmethod
    def setUpClass(cls):
        for model_cls in BaseDocument.__subclasses__():
            # pylint: disable=protected-access
            test_index_name = f"test_{model_cls.Index.name}_{uuid.uuid4()}"
            index_mock = PropertyMock(return_value=test_index_name)
            prefix_mock = PropertyMock(return_value=test_index_name)

            index_patch = patch.object(model_cls._index, "_name", new_callable=index_mock)
            prefix_patch = patch.object(model_cls.Index, "prefix", new_callable=prefix_mock)
            # pylint: enable=protected-access

            index_patch.__enter__()
            prefix_patch.__enter__()

            cls._patches.append(index_patch)
            cls._patches.append(prefix_patch)

            model_cls.init()

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
            # pylint: disable=protected-access
            model_cls._index.delete()
            # pylint: enable=protected-access

    def setUp(self):
        for manager_cls in BaseManager.__subclasses__():
            try:
                manager_cls().truncate(refresh=True)
            except NotFoundError:
                pass
