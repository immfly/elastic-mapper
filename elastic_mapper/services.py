import six

from elastic_mapper import templates

MISSING_TYPENAME_MESSAGE = (
    'There is no registered typename `{typename}`.\n'
    'Please register a new typename or use one of the existing: [{typename_list}].'
)
INVALID_SERVICE_METHOD_MESSAGE = (
    'Export method `{method}` is invalid. Valid export method are formed by appending '
    'a valid registered typename to `{prefix}` '
    '(e.g. TrackingService.export_typename(*args, **kwargs)). '
    'Please use one of the valid registered typenames: [{typename_list}].'
)

EXPORT_PREFIX = 'export_'


class ServiceMetaclass(type):

    def __getattr__(cls, key):
        if key.startswith(EXPORT_PREFIX):
            typename = key.replace(EXPORT_PREFIX, '')
            mapper_cls = templates.Template.types.get(typename, None)
            if not mapper_cls:
                typename_list = ','.join(templates.Template.types.keys())
                msg = MISSING_TYPENAME_MESSAGE.format(typename=typename,
                                                      typename_list=typename_list)
                raise AttributeError(msg)

            def wrapper(*args, **kwargs):
                mapper = mapper_cls(*args, **kwargs)
                mapper.export()
            return wrapper
        elif not hasattr(cls, key):
            typename_list = ','.join(templates.Template.types.keys())
            msg = INVALID_SERVICE_METHOD_MESSAGE.format(method=key,
                                                        prefix=EXPORT_PREFIX,
                                                        typename_list=typename_list)
            raise AttributeError(msg)


@six.add_metaclass(ServiceMetaclass)
class TrackingService(object):
    pass
