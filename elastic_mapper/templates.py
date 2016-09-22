import collections
import inspect
import warnings

from elastic_mapper import repr_utils

INVALID_TYPENAME_MSG = (
    'Type name `{typename}` for `{template}` contains invalid characters. '
    'Automatically converting typename `{typename}` into `{sanitized}`.'
)


class Template(object):
    types = dict()

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
        data['template'] = cls.index
        settings = cls._get_meta_settings()
        if settings:
            data['settings'] = collections.OrderedDict()
            for attr, value in settings:
                data['settings'][attr] = value

        if cls.types:
            data['mappings'] = collections.OrderedDict()
            for typename, mapper_cls in cls.types.items():
                data['mappings'] = mapper_cls.generate_mapping()
        # TODO: aliases
        # data['aliases'] = collections.OrderedDict()

        return data

    @classmethod
    def parse_index(cls, mapper):
        if mapper:
            return cls.parser.parse(cls.index, mapper)


def register(typename, template_cls):

    def wrapped(cls):
        sanitized = repr_utils.sanitize_identifier(typename)
        if sanitized != typename:
            msg = INVALID_TYPENAME_MSG.format(typename=typename,
                                              template=template_cls.__name__,
                                              sanitized=sanitized)
            warnings.warn(msg, SyntaxWarning)
        template_cls.types[sanitized] = cls
        cls.typename = sanitized
        cls.template = template_cls
        return cls

    return wrapped
