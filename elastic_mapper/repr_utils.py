import re

import six


def unicode_repr(instance):
    # Get the repr of an instance, but ensure it is a unicode string
    # on both python 3 (already the case) and 2 (not the case).
    if six.PY2:
        return repr(instance).decode('utf-8')
    return repr(instance)


def field_repr(field):
    kwargs = field._kwargs

    # arg_string = ', '.join([val for val in field._args])
    kwarg_string = ', '.join([
        '%s=%s' % (key, unicode_repr(val))
        for key, val in sorted(kwargs.items())
    ])
    # if arg_string and kwarg_string:
    #     arg_string += ', '

    class_name = field.__class__.__name__

    return "%s(%s)" % (class_name, kwarg_string)


def mapper_repr(mapper, indent=1):
    ret = field_repr(mapper) + ':'
    indent_str = '    ' * indent

    fields = mapper.fields

    for field in fields:
        field_name = field.field_name
        ret += '\n' + indent_str + field_name + ' = '
        if hasattr(field, 'fields'):
            ret += mapper_repr(field, indent + 1)
        else:
            ret += field_repr(field)

    return ret


def sanitize_identifier(identifier):
    sanitized = identifier.replace('-', '_')
    sanitized = filter(lambda ch: bool(re.match('[a-zA-Z0-9_]', ch)), sanitized)
    sanitized = sanitized.strip('_ ')
    return sanitized
