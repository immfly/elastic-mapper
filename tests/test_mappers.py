import json

import pytest
from six import string_types

from elastic_mapper import config, exporters, mappers, templates
from elastic_mapper.services import TrackingService


class TestMapper(object):

    def test_default_source_field(self):
        class TestMapper(mappers.Mapper):
            test_attr = mappers.StringField()

        test_args = {
            'test_attr': 'test value',
        }
        tm = TestMapper(test_args)
        assert tm.mapped_data == test_args

    def test_non_default_source_field(self):
        class TestMapper(mappers.Mapper):
            test_attr_with_source = mappers.StringField(source='test_attr')

        test_args = {
            'test_attr': 'test value',
        }
        tm = TestMapper(test_args)
        assert tm.mapped_data == {'test_attr_with_source': 'test value'}

    def test_mapper_repr(self):
        class TestMapper(mappers.Mapper):
            test_attr_with_source = mappers.StringField(source='test_attr')

        representation = repr(TestMapper())
        result = ("TestMapper():\n"
                  "    test_attr_with_source = StringField(source='test_attr')")
        assert representation == result

    def test_method_source_field(self):
        RESULT = "test method string"

        class TestMapper(mappers.Mapper):
            method_field = mappers.StringField(method='get_test_method_field')

            def get_test_method_field(self, obj):
                return RESULT

        tm = TestMapper()
        assert tm.mapped_data == {'method_field': RESULT}

    def test_nested_mapper(self):
        class NestedMapper(mappers.Mapper):
            test_attr = mappers.StringField()

        class TestMapper(mappers.Mapper):
            nested_attr = NestedMapper()

        test_args = {
            'nested_attr': {
                'test_attr': 'nested test value'
            }
        }
        tm = TestMapper(test_args)
        assert tm.mapped_data == test_args

    def test_dynamic_attribute(self):
        class TestMapper(mappers.Mapper):
            test_attr = mappers.StringField()
            # no such field `dynamic_attr`

        test_args = {
            'test_attr': 'test value',
            'dynamic_attr': 3,
        }
        tm = TestMapper(test_args)
        assert 'dynamic_attr' in tm.mapped_data
        assert tm.mapped_data == test_args


class TestField(object):

    def test_valid_field_options(self):
        class TestMapper(mappers.Mapper):
            test_attr = mappers.StringField(index='not_analyzed')

    def test_invalid_field_options_type(self):
        with pytest.raises(AssertionError) as excinfo:
            class TestMapper(mappers.Mapper):
                test_attr = mappers.StringField(index=25)

        assert 'should be a' in str(excinfo.value)
        assert "'str'" in str(excinfo.value)

    def test_invalid_field_options_choice(self):
        with pytest.raises(AssertionError) as excinfo:
            class TestMapper(mappers.Mapper):
                test_attr = mappers.StringField(index='invalid_choice')

        assert 'should be one of' in str(excinfo.value)
        assert "analyzed" in str(excinfo.value)


class TestStringField(object):

    def test_string_field_data(self):
        class TestMapper(mappers.Mapper):
            test_attr = mappers.StringField()

        test_args = {
            'test_attr': 'test value',
        }
        data = TestMapper(test_args).mapped_data
        assert isinstance(data['test_attr'], string_types)

    def test_string_field_mapping(self):
        test_attr_1 = mappers.StringField(index='not_analyzed')
        assert test_attr_1.mapping_data == {'type': 'string', 'index': 'not_analyzed'}
        test_attr_2 = mappers.StringField(index='analyzed')
        assert test_attr_2.mapping_data == {'type': 'string', 'index': 'analyzed'}
        test_attr_3 = mappers.StringField(index='no')
        assert test_attr_3.mapping_data == {'type': 'string', 'index': 'no'}


class TestIntegerField(object):

    @pytest.fixture
    def integer_mapper_cls(self):
        class TestMapper(mappers.Mapper):
            test_attr = mappers.IntegerField()

        return TestMapper

    @pytest.fixture
    def integer_data(self):
        test_args = {
            'test_attr': 3,
        }
        return test_args

    def test_integer_field_data(self, integer_mapper_cls, integer_data):
        data = integer_mapper_cls(integer_data).mapped_data
        assert isinstance(data['test_attr'], int)

    def test_integer_field_data_conversion(self, integer_mapper_cls, integer_data):
        integer_data['test_attr'] = 3.5  # convert to float
        data = integer_mapper_cls(integer_data).mapped_data
        assert isinstance(data['test_attr'], int)
        assert data['test_attr'], 3

    def test_int_field_mapping(self):
        test_attr_1 = mappers.IntegerField(index='no')
        assert test_attr_1.mapping_data == {'type': 'integer', 'index': 'no'}
        test_attr_2 = mappers.IntegerField(precision_step=16)
        assert test_attr_2.mapping_data == {'type': 'integer', 'precision_step': 16}


class TestTemplate(object):

    def test_template_types(self):
        class TestTemplate(templates.Template):
            name = "test_template"
            index = "test-*"

        @templates.register('test_type', TestTemplate)
        class TestMapper(mappers.Mapper):
            test_attr = mappers.StringField()

        data = TestTemplate.generate_template()
        assert data['template'] == 'test-*'
        assert 'settings' not in data
        assert 'test_type' in data['mappings']
        assert data['mappings']['test_type'] == {'test_attr': {'type': 'string'}}

    def test_template_settings(self):
        class TestTemplate(templates.Template):
            name = "test_template"
            index = "test-*"

            class Meta:
                number_of_shards = 1

        @templates.register('test_type', TestTemplate)
        class TestMapper(mappers.Mapper):
            test_attr = mappers.StringField()

        data = TestTemplate.generate_template()
        assert 'settings' in data
        assert data['settings']['number_of_shards'] == 1


class TestExporter(object):

    @pytest.fixture
    def cached_backend_cls(self):
        class CachedExportBackend(exporters.ExportBackend):
            cached_tracks = []

            def export(self, mapper):
                data = json.dumps(mapper.mapped_data)
                self.__class__.cached_tracks.append(data)

        conf = config.Config()
        conf.add_export_backend(CachedExportBackend)
        return CachedExportBackend

    def test_exporter(self, cached_backend_cls):
        class TestMapper(mappers.Mapper):
            test_attr = mappers.StringField()

        test_args = {
            'test_attr': 'test value',
        }
        TestMapper(test_args).export()
        exported = json.loads(cached_backend_cls.cached_tracks[0])
        assert exported == test_args


class TestService(object):

    @pytest.fixture
    def mapper_args(self):
        class TestTemplate(templates.Template):
            name = "test_template"
            index = "test-*"

            class Meta:
                number_of_shards = 1

        @templates.register('test_type', TestTemplate)
        class TestMapper(mappers.Mapper):
            test_attr = mappers.StringField()

        test_args = {
            'test_attr': 'test value',
        }
        return test_args

    def test_service_registered_typename(self, mapper_args):
        TrackingService.export_test_type(mapper_args)

    def test_service_not_registered_typename(self, mapper_args):
        with pytest.raises(AttributeError) as excinfo:
            TrackingService.export_unknown_type(mapper_args)
        assert 'no registered typename' in str(excinfo.value)
        assert '`unknown_type`' in str(excinfo.value)

    def test_service_call_invalid_classmethod(self, mapper_args):
        with pytest.raises(AttributeError) as excinfo:
            TrackingService.invalid_method()
        assert 'use one of the valid registered typenames' in str(excinfo.value)
        assert '`invalid_method`' in str(excinfo.value)

    def test_service_invalid_typename(self):
        class TestTemplate(templates.Template):
            name = "test_template"
            index = "test-*"

            class Meta:
                number_of_shards = 1

        with pytest.warns(SyntaxWarning) as record:
            @templates.register('9-invalid-typename-*\\/?', TestTemplate)
            class TestMapper(mappers.Mapper):
                test_attr = mappers.StringField()

        test_args = {
            'test_attr': 'test value',
        }
        warning_msg = record[0].message.args[0]
        assert 'contains invalid characters' in warning_msg
        assert '`9_invalid_typename`' in warning_msg
        # check that the sanitized identifier is valid (no exception risen)
        TrackingService.export_9_invalid_typename(test_args)
