import pytest
import elasticsearch
import datetime

import six

from elastic_mapper import mappers
from elastic_mapper import parsers
from elastic_mapper import templates as elastic_templates
from elastic_mapper.cli import differs
from elastic_mapper.cli.differs import State

from mappings import TestIntMapper, TestStringMapper, TestDateMapper
from templates import TestStringTemplate, TestDateTemplate

from . import BaseESTest, es_host


def _diff(mapper):
    es_mapping = es_host.indices.get_mapping(index=mapper.index)[mapper.index]
    differ = differs.MappingDiffer(mapper.typename,
                                   mapper.generate_mapping()[mapper.typename]['properties'],
                                   es_mapping['mappings'][mapper.typename]['properties'])
    states = differ.diff()
    return states


def _diff_template(template_cls):
    es_template = es_host.indices.get_template(template_cls.name)
    differ = differs.TemplateDiffer(template_cls.name,
                                    template_cls.generate_template()['mappings'],
                                    es_template[template_cls.name]['mappings'])
    states = differ.diff()
    return states


def _export_and_diff(mapper_cls, data):
    """
    Export `data` to ES using the mapper `mapper_cls`.

    It then performs a diff between the ES mapping and the expected mapping given by
    `mapper_cls` and returns the used mapper and the resulting diff.
    """
    mapper = mapper_cls(data)
    mapper.export()

    states = _diff(mapper)
    return mapper, states


def _create_index(mapper_cls, properties=None):
    """
    Create an index in ES.

    If `properties` is None, it will create a mapping according to the given `mapper_cls`.
    Otherwise, it will set the mapping data according to `properties`.
    """
    mapper = mapper_cls()
    if properties:
        body = {
            "mappings": {
                mapper.typename: {
                    "properties": properties,
                }
            }
        }
        es_host.indices.create(index=mapper.index, body=body)
    else:
        es_host.indices.create(index=mapper.index,
                               body={"mappings": mapper_cls.generate_mapping()})


def _create_template(template_cls):
    """Put a template into ES."""
    es_host.indices.put_template(template_cls.name, template_cls.generate_template())


def _delete_template(template_cls):
    """Put a template into ES."""
    try:
        es_host.indices.delete_template(template_cls.name)
    except elasticsearch.exceptions.NotFoundError:
        # ignore not found error: faster than checking for existence first
        pass


class TestMappingDiff(BaseESTest):

    def test_ok(self):
        # export the mapping for TestStringMapper
        _create_index(TestStringMapper)

        test_args = {
            "string_field": "string_value",
        }
        mapper = TestStringMapper(test_args)
        states = _diff(mapper)
        assert len(states['string_field']) == 1
        ok = states['string_field'][0]
        assert ok.state == State.ok
        assert ok.description == ''

    def test_missing_param(self):
        test_args = {
            "string_field": "value_missing_param",
        }
        _, states = _export_and_diff(TestStringMapper, test_args)
        assert len(states) == 1
        assert ('string_field') in states.keys()
        assert states['string_field'][0].state == State.missing_param
        assert states['string_field'][0].param_name == 'index'
        assert states['string_field'][0].param_value == 'not_analyzed'
        assert 'index' in states['string_field'][0].description
        assert 'not_analyzed' in states['string_field'][0].description

    def test_double_missing_param(self):
        # Post index with no parameters
        _create_index(TestIntMapper,
                      properties={"int_field": {"type": "integer"}})
        # Export some data using a mapper with some defined parameters
        test_args = {
            "int_field": 666,
        }
        _, states = _export_and_diff(TestIntMapper, test_args)
        assert len(states) == 1
        assert ('int_field') in states.keys()
        assert len(states['int_field']) == 2  # 2 missing parameter conflicts
        # all conflicts are due to a missing parameter
        assert all([s.state == State.missing_param for s in states['int_field']])
        # one conflict is due to a missing `index` parameter
        assert any([(s.param_name == 'boost' and s.param_value == 0.5)
                    for s in states['int_field']])
        # the other conflict is due to a missing `precision_step` parameter
        assert any([(s.param_name == 'precision_step' and s.param_value == 16)
                    for s in states['int_field']])

    def test_dynamic_field(self):
        _create_index(TestIntMapper)
        test_args = {
            "int_field": 666,
            "dynamic_field": "dynamic_string",
        }
        _, states = _export_and_diff(TestIntMapper, test_args)
        assert any([s.state == State.extra_field for s in states['dynamic_field']])

    def test_extra_param(self):
        # NOTE: this relies on the fact that adding a boost parameter to an integer field
        # automatically adds a norms parameter to the index. This is possibly going to break when
        # using different versions of ES.
        _create_index(TestIntMapper)
        test_args = {
            "int_field": 666,
        }
        _, states = _export_and_diff(TestIntMapper, test_args)
        conflict = states['int_field'][0]
        assert conflict.state == State.extra_param
        assert conflict.param_name == 'norms'

    def test_missing_field(self):
        # export the mapping for TestStringMapper
        _create_index(TestStringMapper)

        # define the same mapper with a new field (as if the mapper had been updated)
        @elastic_templates.register('test_type_string', TestStringTemplate)
        class TestNewStringMapper(mappers.Mapper):
            string_field = mappers.StringField(index='not_analyzed')
            new_string_field = mappers.StringField(index='not_analyzed')

        test_args = {
            "string_field": "string_value_old",
            "new_string_field": "string_value_new",
        }
        mapper = TestNewStringMapper(test_args)
        states = _diff(mapper)
        assert any([s.state == State.missing_field for s in states['new_string_field']])

    def test_type_conflict(self):
        # export the mapping for TestStringMapper
        _create_index(TestStringMapper)
        # define the same mapper with a new field (as if the mapper had been updated)

        @elastic_templates.register('test_type_string', TestStringTemplate)
        class TestNewStringMapper(mappers.Mapper):
            string_field = mappers.IntegerField(precision_step=16)  # updated field (string -> int)

        test_args = {
            "string_field": 16,
        }
        mapper = TestNewStringMapper(test_args)
        states = _diff(mapper)
        assert len(states['string_field']) == 1
        conflict = states['string_field'][0]
        assert conflict.state == State.type_conflict
        assert conflict.source_type == 'string'
        assert conflict.dest_type == 'integer'

    def test_param_conflict(self):
        # export the mapping for TestStringMapper
        _create_index(TestStringMapper)

        # define the same mapper with a new field (as if the mapper had been updated)
        @elastic_templates.register('test_type_string', TestStringTemplate)
        class TestNewStringMapper(mappers.Mapper):
            string_field = mappers.StringField(index='analyzed')  # updated param (not_analyzed -> analyzed) # noqa

        test_args = {
            "string_field": "string_value",
        }
        mapper = TestNewStringMapper(test_args)
        states = _diff(mapper)
        assert len(states['string_field']) == 1
        conflict = states['string_field'][0]
        assert conflict.state == State.param_conflict
        assert conflict.param_name == 'index'
        assert conflict.source_param == 'not_analyzed'
        assert conflict.dest_param == 'analyzed'


class TestTimelyMappingDiff(BaseESTest):
    NUM_TEST_INDICES = 3  # larger numbers make the tests slower

    def test_ok_timely_index(self):
        # export the mapping for TestStringMapper
        _create_template(TestDateTemplate)

        base_time = datetime.datetime(2000, 1, 1, 0, 0, 0)
        for i in range(self.NUM_TEST_INDICES):
            time = base_time + datetime.timedelta(days=i)
            test_args = {
                "timestamp": time,
                "integer_field": 666,
            }
            mapper = TestDateMapper(test_args)
            mapper.export()

        all_mappings = es_host.indices.get_mapping(index="*")

        matches = parsers.get_matching_indexes(TestDateTemplate.index, all_mappings)

        assert len(matches) == self.NUM_TEST_INDICES
        assert all([match.startswith('test-date-') for match in matches])

        differ = differs.TimelyIndexDiffer(TestDateTemplate.name,
                                           TestDateMapper.generate_mapping(),
                                           TestDateTemplate.index,
                                           all_mappings)
        states = differ.diff()
        for fieldname, states in six.iteritems(states):
            assert all([state.state == State.ok for state in states])

    def test_index_type_conflict_timely_index(self):
        # delete template to generate conflicts
        _delete_template(TestDateTemplate)

        # define the same mapper with a conflicting type (string_field)
        @elastic_templates.register('test_type_date', TestDateTemplate)
        class TestNewDateMapper(mappers.Mapper):
            timestamp = mappers.DateField(format='strict_date_optional_time||epoch_millis')
            integer_field = mappers.StringField(index='not_analyzed')

        time_1 = datetime.datetime(2000, 1, 1, 0, 0, 0)
        time_2 = datetime.datetime(2000, 1, 2, 0, 0, 0)
        test_args_1 = {
            "timestamp": time_1,
            "integer_field": 666,
        }
        test_args_2 = {
            "timestamp": time_2,
            "integer_field": "wrong value",
        }
        mapper_1 = TestDateMapper(test_args_1)
        mapper_1.export()
        mapper_2 = TestNewDateMapper(test_args_2)
        mapper_2.export()

        all_mappings = es_host.indices.get_mapping(index="*")

        matches = parsers.get_matching_indexes(TestDateTemplate.index, all_mappings)

        assert len(matches) == 2
        assert 'test-date-20000101' in matches
        assert 'test-date-20000102' in matches

        differ = differs.TimelyIndexDiffer(TestDateTemplate.name,
                                           TestDateMapper.generate_mapping(),
                                           TestDateTemplate.index,
                                           all_mappings)
        states = differ.diff()
        assert len(states['integer_field']) == 1
        assert states['integer_field'][0].state == State.index_type_conflict
        assert states['integer_field'][0].fieldname == 'integer_field'
        assert 'long' in states['integer_field'][0].defined_types
        assert 'string' in states['integer_field'][0].defined_types


class TestTemplateDiff(BaseESTest):

    def test_ok(self):
        _create_template(TestStringTemplate)

        result = _diff_template(TestStringTemplate)
        assert result.type_states['test_type_string']['string_field'][0].state == State.ok

    def test_template_missing_type(self):
        # store initial template register
        original_types = TestStringTemplate.types.copy()

        # (temporary) add a new type into the register
        @elastic_templates.register('test_missing_type', TestStringTemplate)
        class TestMissingTypeMapper(mappers.Mapper):
            new_field = mappers.IntegerField()

        # put the template with the new type into ES
        _create_template(TestStringTemplate)

        # restore original type register for comparison
        TestStringTemplate.types = original_types

        # perform the diff to check the missing type
        result = _diff_template(TestStringTemplate)
        assert len(result.template_states) == 1
        missing_state = result.template_states[0]
        assert missing_state.state == State.template_missing_type
        assert missing_state.typename == 'test_missing_type'
        assert 'test_missing_type' in missing_state.description

    def test_template_extra_type(self):
        _create_template(TestStringTemplate)

        # define the same mapper with a new field (as if the mapper had been updated)
        @elastic_templates.register('test_extra_type', TestStringTemplate)
        class TestExtraTypeMapper(mappers.Mapper):
            new_field = mappers.IntegerField()

        result = _diff_template(TestStringTemplate)
        assert len(result.template_states) == 1
        extra_state = result.template_states[0]
        assert extra_state.state == State.template_extra_type
        assert extra_state.typename == 'test_extra_type'
        assert 'test_extra_type' in extra_state.description

    def test_template_inconsistent_types(self):
        # define a new mapper with a field with same name but different types
        @elastic_templates.register('test_type_inconsistent', TestStringTemplate)
        class TestInconsistentTypeMapper(mappers.Mapper):
            string_field = mappers.IntegerField()  # inconsistent: same name but different type

        result = _diff_template(TestStringTemplate)
        # check that the inconsistend field conflict is detected for the first mapper
        assert 'test_type_string' in result.type_states
        assert 'string_field' in result.type_states['test_type_string']
        assert len(result.type_states['test_type_string']['string_field']) == 1
        assert result.type_states['test_type_string']['string_field'][0].state == State.inconsistent_field  # noqa

        # check that the inconsistend field conflict is detected for the second mapper
        assert 'test_type_inconsistent' in result.type_states
        assert 'string_field' in result.type_states['test_type_inconsistent']
        assert len(result.type_states['test_type_inconsistent']['string_field']) == 1
        assert result.type_states['test_type_inconsistent']['string_field'][0].state == State.inconsistent_field  # noqa

        # check that putting the template into ES returns an error due to the inconsistency
        with pytest.raises(elasticsearch.exceptions.RequestError) as excinfo:
            _create_template(TestStringTemplate)
        assert 'string_field' in str(excinfo.value)
