import collections
import inspect
import warnings

from elastic_mapper import repr_utils

INVALID_TYPENAME_MSG = (
    'Type name `{typename}` for `{template}` contains invalid characters. '
    'Automatically converting typename `{typename}` into `{sanitized}`.'
)


class Template(object):
    # types = dict()

    @classmethod
    def _get_meta_settings(self):
        meta = getattr(self, 'Meta', None)
        if not meta:
            return ()
        meta_attrs = inspect.getmembers(meta, lambda a: not(inspect.isroutine(a)))
        attrs = (a for a in meta_attrs if not (a[0].startswith('__') and a[0].endswith('__')))
        return attrs

    @classmethod
    def generate_template(cls):
        data = collections.OrderedDict()
        data['template'] = cls.parse_index_template()
        settings = cls._get_meta_settings()
        if settings:
            data['settings'] = collections.OrderedDict()
            for attr, value in settings:
                data['settings'][attr] = value

        if cls.types:
            data['mappings'] = collections.OrderedDict()
            for typename, mapper_cls in cls.types.items():
                data['mappings'][typename] = mapper_cls.generate_mapping()[typename]
        # TODO: aliases
        # data['aliases'] = collections.OrderedDict()

        return data

    @classmethod
    def parse_index(cls, mapper):
        if mapper:
            return cls.parser.parse(cls.index, mapper)

    @classmethod
    def parse_index_template(cls):
        """
        Format the index template pattern to be sent to ES.

        For example, it turns:
            test-string-{time}
        into:
            test-string-*
        """
        return cls.index.format(time='*')


def register(typename, template_cls):

    def wrapped(mapper_cls):
        # sanitize typename to make sure it's a valid python identifier
        sanitized = repr_utils.sanitize_identifier(typename)
        if sanitized != typename:
            msg = INVALID_TYPENAME_MSG.format(typename=typename,
                                              template=template_cls.__name__,
                                              sanitized=sanitized)
            warnings.warn(msg, SyntaxWarning)
        # add the mapper to the template types dict using the sanitized name as the key
        types = getattr(template_cls, 'types', dict())
        types[sanitized] = mapper_cls
        setattr(template_cls, 'types', types)
        # add the sanitized typename and template properties to the mapper
        mapper_cls.typename = sanitized
        mapper_cls.template = template_cls
        return mapper_cls

    return wrapped
