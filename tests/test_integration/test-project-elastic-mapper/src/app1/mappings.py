from elastic_mapper import mappers
from elastic_mapper import templates as elastic_templates

from templates import TestTemplateApp1


class NestedMapper(mappers.Mapper):
    nested_string = mappers.StringField()
    nested_int_with_source = mappers.IntegerField(source='nested_int_field')


@elastic_templates.register('test_type_app1', TestTemplateApp1)
class TestMapperApp1(mappers.Mapper):
    date_field = mappers.DateField(auto_now=True)
    string_field = mappers.StringField(index='not_analyzed')
    int_field = mappers.IntegerField(precision_step=16)
    string_field_with_source = mappers.StringField(source='another_attr')
    method_field = mappers.StringField(method='get_test_method_field')
    nested_field = NestedMapper()

    def get_test_method_field(self, obj):
        return "test method value"
