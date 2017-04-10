from __future__ import unicode_literals
import copy

import six

from elastic_mapper import repr_utils
from elastic_mapper.loggers import global_logger

from elastic_mapper.fields import (  # flake8: noqa # isort:skip
    get_attribute,
    Field,
    StringField,
    BooleanField,
    IntegerField,
    FloatField,
    DateField,
)


class MapperOptions(object):

    def __init__(self, dynamic_fields):
        self.dynamic_fields = dynamic_fields


class MapperMetaclass(type):

    @classmethod
    def _get_fields(cls, bases, attrs):
        # store fields
        fields = [(field_name, attrs.pop(field_name))
                  for field_name, obj in list(attrs.items())
                  if isinstance(obj, Field)]

        for base in reversed(bases):
            if hasattr(base, '_declared_fields'):
                fields = list(base._declared_fields.items()) + fields

        return dict(fields)

    def __new__(cls, name, bases, attrs):
        # add declared fields to class
        attrs['_declared_fields'] = cls._get_fields(bases, attrs)
        return super(MapperMetaclass, cls).__new__(cls, name, bases, attrs)

    def __init__(cls, name, bases, attrs):
        # set _meta options
        dynamic_fields = ()
        meta = getattr(cls, 'Meta', None)
        if meta:
            # override defaults from SyncController's Meta
            dynamic_fields = getattr(meta, 'dynamic_fields', dynamic_fields)

        # create _meta attribute containing the Mapper's options
        options = MapperOptions(dynamic_fields=dynamic_fields)
        setattr(cls, '_meta', options)


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
            value = field.get_attribute(instance)
            if value is not None:
                value = field.to_representation(value)
            ret[field.field_name] = value

        # dynamic data
        attrs = getattr(instance, '__dict__', instance)
        if not attrs:
            return ret

        for attr, value in six.iteritems(attrs):
            if not(self._meta.dynamic_fields == '__all__' or attr in self._meta.dynamic_fields):
                # skip when this attribute is not allowed to be dynamic
                continue

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
        for attr_name, field in six.iteritems(cls._declared_fields):
            properties[attr_name] = field.mapping_data

        mapping = {
            cls.typename: {
                'properties': properties,
            }
        }
        return mapping

    def __repr__(self):
        return repr_utils.mapper_repr(self)
