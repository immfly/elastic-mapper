import six
import collections
import copy
import inspect



def get_attribute(obj, attr):
    if isinstance(obj, collections.Mapping):
        return obj[attr]
    else:
        return getattr(obj, attr)


class Field(object):
    __counter = 0

    def __init__(self, source=None):
        self.__class__.__counter += 1
        self.source = source
        # field_name and mapper will be set from the mapper
        # by calling the `bind` method
        self.field_name = None
        self.mapper = None

    def bind(self, field_name, mapper):
        assert self.source != field_name, (
            "Remove the redundant `source='%s'` keyword argument "
            "in mapper '%s' as it matches the field name '%s'." %
            (self.source, mapper.__class__.__name__, field_name)
        )
    	self.field_name = field_name
    	self.mapper = mapper

    	if not self.source:
    	    self.source = self.field_name

    def get_attribute(self, instance):
    	if self.source.startswith('mapper__'):
    	    # obtain attribute from a mapper method
    	    method_name = self.source.replace('mapper__', '')
    	    method = getattr(self.mapper, method_name)
    	    return method(instance)

    	# obtain attribute from instance using the given source
        return get_attribute(instance, self.source)

    def to_representation(self, value):
        raise NotImplementedError("A Field must implement the to_representation method")

    def mapping_data(self):
	raise NotImplementedError("A Field must implement the mapping_data method")    	


class StringField(Field):

    def to_representation(self, value):
        return six.text_type(value)

    def mapping_data(self):
    	# TODO: parse options
    	mapping = {
    	    'type': 'string',
    	}
    	return mapping



class IntegerField(Field):

    def to_representation(self, value):
        return int(value)

    def mapping_data(self):
    	# TODO: parse options
    	mapping = {
    	    'type': 'integer',
    	}
    	return mapping