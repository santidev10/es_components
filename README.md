# es-components

It contains data models such as Channel, Video, Keyword and corresponding models managers for them.

# Usage
To migrate ES mapping:

```python
from es_components.migration import init_mapping

init_mapping()
```

To reindex ES:

```python
from es_components.migration import reindex

reindex()
```

To update ES alias:

```python
from es_components.migration import update_alias

update_alias()
```