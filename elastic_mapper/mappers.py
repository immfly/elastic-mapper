import six
import collections
import copy
import inspect

from elastic_mapper.fields import *  # noqa # isort:skip


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
        A dictionary of {field_name: field_instance}.
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
        for attr, value in attrs.iteritems():
            # TODO: recursively assert type correctness
            skip_attrs = (f.field_name if (not f.source or f.source.startswith('mapper__')) else f.source
            	          for f in self.fields)
            if attr not in skip_attrs:
            	ret[attr] = get_attribute(instance, attr)

        return ret

    @property
    def data(self):
        """
        Object instance -> Dict of primitive datatypes.
        """
        return self.to_representation(self.instance)

    def store(self):
    	"Sends the mapped data to the configured backends"
    	# TODO: send data to backends
    	pass

    def mapping_data(self):
    	# TODO: parse options
    	mapping = {}
    	return mapping

    @classmethod
    def mapping(cls):
    	mapping = {}
    	for attr_name, field in cls._declared_fields.iteritems():
    	    mapping[attr_name] = field.mapping_data()

    	return mapping