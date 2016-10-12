from elastic_mapper import mappers
from elastic_mapper import templates as elastic_templates

from app1.mappings import NestedMapper
from templates import TestTemplateApp2


@elastic_templates.register('test_type_app2', TestTemplateApp2)
class TestMapperApp2(mappers.Mapper):
    date_field = mappers.DateField(auto_now=True)
    string_field = mappers.StringField(index='analyzed')
    int_field = mappers.IntegerField()
    method_field = mappers.StringField(method='get_test_method_field')
    nested_field = NestedMapper()

    def get_test_method_field(self, obj):
        return "test method value"


@elastic_templates.register('test_type_app3', TestTemplateApp2)
class TestMapperApp3(mappers.Mapper):
    date_field_third = mappers.DateField(auto_now=True)
    string_field_third = mappers.StringField(index='not_analyzed')
