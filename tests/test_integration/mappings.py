from elastic_mapper import mappers
from elastic_mapper import templates as elastic_templates

from templates import TestStringTemplate, TestIntTemplate


@elastic_templates.register('test_type_string', TestStringTemplate)
class TestStringMapper(mappers.Mapper):
    string_field = mappers.StringField(index='not_analyzed')


@elastic_templates.register('test_type_int', TestIntTemplate)
class TestIntMapper(mappers.Mapper):
    int_field = mappers.IntegerField(precision_step=16,
                                     boost=0.5)
