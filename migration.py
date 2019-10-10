from elasticsearch_dsl import connections
from es_components.models.base import BaseDocument
from .connections import init_es_connection
from .datetime_service import datetime_service

date = datetime_service.now().strftime("%Y%m%d")

ALL_MODELS = BaseDocument.__subclasses__()

# pylint: enable=redefined-outer-name
for _model in ALL_MODELS:
    _model.Index.name = f"{_model.Index.prefix}{date}"
# pylint: enable=redefined-outer-name

init_es_connection()
connection = connections.get_connection()


def init_mapping():
    for _model in ALL_MODELS:
        _model.init()

def reindex():
    for model in ALL_MODELS:
        src_index = model.Index.prefix.strip("_")
        body = {
            "source": {"index": src_index},
            "dest": {"index": model.Index.name},
        }
        connection.reindex(
            body=body,
            wait_for_completion=False,
        )

def get_reindex_tasks():
    tasks = connection.tasks.list(group_by="parents", actions="*reindex").get("tasks")
    return tasks


def update_alias():
    reindex_tasks_count = len(get_reindex_tasks())
    if reindex_tasks_count > 0:
        raise RuntimeError(f"There are still active {reindex_tasks_count} reindex task(s).")

    aliases = {}
    for key, value in connection.indices.get_alias().items():
        _aliases = value.get("aliases", {})
        if _aliases:
            for alias in _aliases.keys():
                aliases[alias] = key
    actions = []

    for model in ALL_MODELS:
        alias = model.Index.prefix.strip("_")

        old_index = aliases.get(alias)
        if old_index:
            actions.append({"remove": {"index": old_index, "alias": alias}})
        actions.append({"add": {"index": model.Index.name, "alias": alias, "is_write_index": True}})

    connection.indices.update_aliases({"actions": actions})
