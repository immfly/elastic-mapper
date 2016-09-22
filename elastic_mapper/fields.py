from __future__ import unicode_literals
import collections
import six
import datetime

from elastic_mapper import repr_utils


def get_attribute(obj, attr):
    if isinstance(obj, collections.Mapping):
        return obj[attr]
    else:
        return getattr(obj, attr)


NOT_SOURCE_AND_METHOD_MESSAGE = 'May not set both `source` and `method` attributes simultaneously'
WRONG_OPTION_TYPE_MESSAGE = ('Parameter `{param}` for field of type `{field_class}` '
                             'should be a `{type}`.')
WRONG_OPTION_CHOICE_MESSAGE = ('Parameter `{param}` for field of type `{field_class}` '
                               'should be one of {choices}.')


class Field(object):
    __counter = 0
    options = {}

    def __new__(cls, *args, **kwargs):
        """
        """
        instance = super(Field, cls).__new__(cls)
        instance._args = args
        instance._kwargs = kwargs
        return instance

    def __init__(self, source=None, method=None, **kwargs):
        self.__class__.__counter += 1

        assert not (source and method), NOT_SOURCE_AND_METHOD_MESSAGE

        self.source = source
        self.method = method
        # field_name and mapper will be set from the mapper
        # by calling the `bind` method
        self.field_name = None
        self.mapper = None

        self._validate_field_params(**self._kwargs)

    def _validate_field_params(self, **params):
        self.params = {}
        for kw, value in params.items():
            if kw in self.options:
                _type = self.options[kw][0]
                assert isinstance(value, _type), (
                    WRONG_OPTION_TYPE_MESSAGE.format(param=kw,
                                                     field_class=self.__class__.__name__,
                                                     type=_type)
                )
                if len(self.options[kw]) == 2:
                    _type, choices = self.options[kw]
                    assert value in choices, (
                        WRONG_OPTION_CHOICE_MESSAGE.format(param=kw,
                                                           field_class=self.__class__.__name__,
                                                           type=_type,
                                                           choices=choices)
                    )
                self.params[kw] = value

    def bind(self, field_name, mapper):
        assert self.method or self.source != field_name, (
            "Remove the redundant `source='%s'` keyword argument "
            "in mapper '%s' as it matches the field name '%s'." %
            (self.source, mapper.__class__.__name__, field_name)
        )

        self.field_name = field_name
        self.mapper = mapper

        if not self.source and not self.method:
            self.source = self.field_name

    def get_attribute(self, instance):
        if self.method:
            # obtain attribute from a mapper method
            method = getattr(self.mapper, self.method)
            return method(instance)

        # obtain attribute from instance using the given source
        return get_attribute(instance, self.source)

    def to_representation(self, value):
        raise NotImplementedError("A Field must implement the to_representation method")

    @property
    def mapping_data(self):
        raise NotImplementedError("A Field must implement the mapping_data method")

    def __repr__(self):
        return repr_utils.field_repr(self)


class StringField(Field):
    options = {
        'index': (str, ('analyzed', 'not_analyzed', 'no')),
    }

    def to_representation(self, value):
        return six.text_type(value)

    @property
    def mapping_data(self):
        mapping = {
            'type': 'string',
        }
        mapping.update(self.params)
        return mapping


class NumericField(Field):
    options = {
        'index': (str, ('not_analyzed', 'no')),
        'precision_step': (int, ),
    }

    @property
    def mapping_data(self):
        mapping = {
            'type': self.mapping_type,  # must be overridden by subclasses
        }
        mapping.update(self.params)
        return mapping


class IntegerField(NumericField):
    mapping_type = 'integer'

    def to_representation(self, value):
        return int(value)


class DateField(Field):
    options = {
        'format': (str, ),
        'index': (str, ('not_analyzed', 'no')),
    }

    def __init__(self, **kwargs):
        self.auto_now = kwargs.pop('auto_now', False)
        self.strftime = kwargs.pop('strftime', '%Y-%m-%dT%H:%M:%S%z')
        super(DateField, self).__init__(**kwargs)

    def get_attribute(self, instance):
        if self.auto_now:
            # user current datetime as default
            now = datetime.datetime.now()
            return now

        # obtain datetime from source
        return super(DateField, self).get_attribute(instance)

    def to_representation(self, value):
        return value.strftime(self.strftime)

    @property
    def mapping_data(self):
        mapping = {
            'type': 'date',
        }
        mapping.update(self.params)
        return mapping
