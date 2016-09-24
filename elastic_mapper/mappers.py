from __future__ import unicode_literals
import copy

import six

from elastic_mapper import repr_utils
from elastic_mapper.loggers import global_logger

from elastic_mapper.fields import (  # flake8: noqa # isort:skip
    get_attribute,  
    Field,
    StringField,
    IntegerField,
    DateField,
)


class MapperMetaclass(type):

    @classmethod
    def _get_fields(cls, bases, attrs):
        fields = [(field_name, attrs.pop(field_name))
                  for field_name, obj in list(attrs.items())
                  if isinstance(obj, Field)]

        for base in reversed(bases):
            if hasattr(base, '_declared_fields'):
                fields = list(base._declared_fields.items()) + fields

        return dict(fields)

    def __new__(cls, name, bases, attrs):
        attrs['_declared_fields'] = cls._get_fields(bases, attrs)
        return super(MapperMetaclass, cls).__new__(cls, name, bases, attrs)


@six.add_metaclass(MapperMetaclass)
class Mapper(Field):

    def __init__(self, instance=None, *args, **kwargs):
        self.instance = instance
        super(Mapper, self).__init__(*args, **kwargs)

    @property
    def fields(self):
        """
        """
        if not hasattr(self, '_fields'):
            self._fields = {}
            for field_name, field in self.copied_fields().items():
                field.bind(field_name, self)
                self._fields[field_name] = field

        return self._fields.values()

    def copied_fields(self):
        return copy.deepcopy(self._declared_fields)

    def to_representation(self, instance):
        ret = dict()

        # mapped data
        for field in self.fields:
            attribute = field.get_attribute(instance)
            val = field.to_representation(attribute)
            ret[field.field_name] = val

        # dynamic data
        attrs = getattr(instance, '__dict__', instance)
        if not attrs:
            return ret
        for attr, value in attrs.iteritems():
            # TODO: recursively assert type correctness
            skip_attrs = (f.field_name
                          if (not f.source or f.source.startswith('mapper__'))
                          else f.source
                          for f in self.fields)
            if attr not in skip_attrs:
                ret[attr] = get_attribute(instance, attr)

        return ret

    def export(self):
        "Sends the mapped data to the configured export backends"
        global_logger.info('export', self)

    @property
    def index(self):
        return self.template.parse_index(self)

    @property
    def mapped_data(self):
        """
        Object instance -> Dict of primitive datatypes.
        """
        return self.to_representation(self.instance)

    @property
    def mapping_data(self):
        mapping = {
            'type': 'object',
            'properties': {},
        }
        for field in self.fields:
            mapping['properties'][field.field_name] = field.mapping_data
        return mapping

    @classmethod
    def generate_mapping(cls):
        properties = {}
        for attr_name, field in cls._declared_fields.iteritems():
            properties[attr_name] = field.mapping_data

        mapping = {
            cls.typename: {
                'properties': properties,
            }
        }
        return mapping

    def __repr__(self):
        return repr_utils.mapper_repr(self)
