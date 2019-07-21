from elasticsearch_dsl import Boolean
from elasticsearch_dsl import Date
from elasticsearch_dsl import Document
from elasticsearch_dsl import Double
from elasticsearch_dsl import InnerDoc
from elasticsearch_dsl import Keyword
from elasticsearch_dsl import Object
from elasticsearch_dsl import Text
from elasticsearch_dsl.utils import AttrList

from es_components.stats import History


class BaseInnerDoc(InnerDoc):
    created_at = Date(index=False)
    updated_at = Date()

    def update(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class MainSection(BaseInnerDoc):
    # pylint: disable=invalid-name
    id = Keyword()
    # pylint: enable=invalid-name


class BaseInnerDocWithHistory(BaseInnerDoc):
    __history = None

    def prepare_history(self):
        self.__history = History(self, self.History.all)

    def update_history(self):
        if self.__history:
            self.__history.update()

    def __del__(self):
        self.__history = None


class Schedule(BaseInnerDoc):
    pass


class SectionAnalytics(BaseInnerDoc):
    """ Nested analytics section """
    fetched_at = Date(index=False)
    direct_auth = Boolean()
    auth_timestamp = Date()
    content_owner_id = Keyword(multi=True)
    cms_title = Keyword()

    # general info
    comments = Object(enabled=False)
    views = Object(enabled=False)
    likes = Object(enabled=False)
    dislikes = Object(enabled=False)
    minutes_watched = Object(enabled=False)
    subscribers_gained = Object(enabled=False)
    subscribers_lost = Object(enabled=False)

    # audience
    audience = Object(enabled=False)
    age = Object(enabled=False)
    age_13_17 = Double()
    age_18_24 = Double()
    age_25_34 = Double()
    age_35_44 = Double()
    age_45_54 = Double()
    age_55_64 = Double()
    age_65_ = Double()
    gender = Object(enabled=False)
    gender_male = Double()
    gender_female = Double()

    # country
    country = Object(enabled=False)
    
    # traffic_source
    traffic_source = Object(enabled=False)


class Deleted(BaseInnerDoc):
    initiator = Text(index=False)
    reason = Text(index=False)


class BaseDocument(Document):
    main = Object(MainSection)
    deleted = Object(Deleted)

    # pylint: disable=redefined-builtin
    def __init__(self, id=None, **kwargs):
        """ Initialize Base Document.

        :param id: id to initialize main section
        :param kwargs: params that will be passed to super(BaseDocument, self).__init__
        """
        super(BaseDocument, self).__init__(**kwargs)

        if id:
            self.init_main(id=id)

        if "id" not in self.meta:
            self.meta["id"] = self.main.id

    # pylint: enable=redefined-builtin

    def init_main(self, **kwargs):
        if self.main:
            raise Exception("Main section already exists. Cannot reinitialize it.")

        self.main = MainSection(**kwargs)

    def _populate_section(self, section_name, append_uniq_kwargs=None, **init_kwargs):
        section = getattr(self, section_name)

        # pylint: disable=protected-access
        section_class = self._doc_type.mapping[section_name]._doc_class
        section_class_properties = set(section_class._doc_type.mapping)
        # pylint: enable=protected-access
        properties_missed_in_mapping = init_kwargs.keys() - section_class_properties
        if properties_missed_in_mapping:
            raise ValueError(f"Extra fields: {properties_missed_in_mapping}")

        if not section:
            section = section_class()
            setattr(self, section_name, section)

        section.update(**init_kwargs)
        if append_uniq_kwargs:
            for name, extra_values in append_uniq_kwargs.items():
                if not isinstance(extra_values, list):
                    raise TypeError
                values = getattr(section, name)
                values = AttrList(set(list(values) + extra_values))
                setattr(section, name, values)

