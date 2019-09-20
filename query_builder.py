from elasticsearch_dsl import Q


class QueryBuilder:
    def __init__(self):
        self.__condition = None
        self.__field = None
        self.__value = None
        self.__rule = None

    def set_condition(self, value):
        self.__condition = value

    def set_field(self, value):
        self.__field = value

    def set_value(self, value):
        self.__value = value

    def set_rule(self, value):
        self.__rule = value

    def build(self):
        return Condition(self)

    def get(self):
        query_filter = {
            "bool": {
                self.__condition: {
                    self.__rule: {
                        self.__field: self.__value
                    }
                }
            }
        }
        return Q(query_filter)


class Condition:
    def __init__(self, query):
        self.__query = query

    def must(self):
        self.__query.set_condition("must")
        return Rule(self.__query)

    def must_not(self):
        self.__query.set_condition("must_not")
        return Rule(self.__query)

    def should(self):
        self.__query.set_condition("should")
        return Rule(self.__query)


class Rule:
    def __init__(self, query):
        self.__query = query

    def exists(self):
        self.__query.set_rule("exists")
        return ExistsField(self.__query)

    def term(self):
        self.__query.set_rule("term")
        return Field(self.__query, Value)

    def terms(self):
        self.__query.set_rule("terms")
        return Field(self.__query, Value)

    def match_phrase(self):
        self.__query.set_rule("match_phrase")
        return Field(self.__query, Value)

    def range(self):
        self.__query.set_rule("range")
        return Field(self.__query, RangeValue)


class ExistsField:
    def __init__(self, query):
        self.__query = query

    def field(self, value):
        self.__query.set_field("field")
        self.__query.set_value(value)
        return self.__query


class Field:
    def __init__(self, query, value_cls):
        self.__query = query
        self.__value_cls = value_cls

    def field(self, value):
        self.__query.set_field(value)
        return self.__value_cls(self.__query)


class Value:
    def __init__(self, query):
        self.__query = query

    def value(self, value):
        self.__query.set_value(value)
        return self.__query


class RangeValue:
    def __init__(self, query):
        self.__query = query
        self.values = {}

    def lt(self, value):
        self.values["lt"] = value
        return self

    def lte(self, value):
        self.values["lte"] = value
        return self

    def gt(self, value):
        self.values["gt"] = value
        return self

    def gte(self, value):
        self.values["gte"] = value
        return self

    def get(self):
        self.__query.set_value(self.values)
        return self.__query.get()
