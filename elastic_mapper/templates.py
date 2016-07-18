import six
import collections
import copy
import inspect




class Template(object):
    types = set()

    def _get_meta_settings(self):
    	meta = getattr(self, 'Meta')
    	meta_attrs = inspect.getmembers(meta, lambda a: not(inspect.isroutine(a)))
    	attrs = (a for a in meta_attrs if not (a[0].startswith('__') and a[0].endswith('__')))
    	return attrs

    def template_data(self):
    	data = collections.OrderedDict()
    	data['template'] = self.template
    	data['settings'] = collections.OrderedDict()
	for attr, value in self._get_meta_settings():
    	    data['settings'][attr] = value

    	data['mappings'] = collections.OrderedDict()
    	for type_name, mapper_cls in self.types:
    	    data['mappings'][type_name] = mapper_cls.mapping()
    	data['aliases'] = collections.OrderedDict()

    	return data


def register(type_name, template_cls):
    
    def wrapped(cls):
    	template_cls.types.add((type_name, cls))
    	cls.template = template_cls
    	return cls

    return wrapped
