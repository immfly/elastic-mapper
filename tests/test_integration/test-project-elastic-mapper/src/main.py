from elastic_mapper import config

from app1.mappings import TestMapperApp1
from app1.templates import TestTemplateApp1
from app2.mappings import TestMapperApp2
from app2.templates import TestTemplateApp2
from export_backends import TestElasticSearchBackend


def test(mapper_cls):
    test_args = {
        "string_field": "value",
        "int_field": 1,
        "another_attr": "another value",
        "nested_field": {
            "nested_string": "nested value",
            "nested_int_field": 42,
        },
        "dynamic_attr": "not mapped value",
    }
    mapper_cls(test_args).export()


if __name__ == '__main__':
    conf = config.Config()
    conf.add_export_backend(TestElasticSearchBackend)

    test(TestMapperApp1)
    test(TestMapperApp2)
