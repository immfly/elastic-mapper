from elastic_mapper import mappers
from elastic_mapper import templates as elastic_templates
from elastic_mapper.cli import differs
from elastic_mapper.cli.differs import State

from mappings import TestIntMapper, TestStringMapper
from templates import TestStringTemplate

from . import BaseESTest, es_host


class TestDiff(BaseESTest):

    def _diff(self, mapper):
        es_mapping = es_host.indices.get_mapping(index=mapper.index)[mapper.index]
        differ = differs.MappingDiffer(mapper.typename,
                                       mapper.generate_mapping()[mapper.typename]['properties'],
                                       es_mapping['mappings'][mapper.typename]['properties'])
        states = differ.diff()
        return states

    def _export_and_diff(self, mapper_cls, data):
        """
        Exports `data` to ES using the mapper `mapper_cls`.
        It then performs a diff between the ES mapping and the expected mapping given by
        `mapper_cls` and returns the used mapper and the resulting diff.
        """
        mapper = mapper_cls(data)
        mapper.export()

        states = self._diff(mapper)
        return mapper, states

    def _create_index(self, mapper_cls, properties=None):
        """
        Creates an index in ES.
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
            print "MAPPING"
            print mapper_cls.generate_mapping()
            es_host.indices.create(index=mapper.index,
                                   body={"mappings": mapper_cls.generate_mapping()})

    def test_ok(self):
        # export the mapping for TestStringMapper
        self._create_index(TestStringMapper)

        test_args = {
            "string_field": "string_value",
        }
        mapper = TestStringMapper(test_args)
        states = self._diff(mapper)
        assert len(states['string_field']) == 1
        ok = states['string_field'][0]
        assert ok.state == State.ok
        assert ok.description == ''

    def test_missing_param(self):
        test_args = {
            "string_field": "value_missing_param",
        }
        _, states = self._export_and_diff(TestStringMapper, test_args)
        assert len(states) == 1
        assert ('string_field') in states.keys()
        assert states['string_field'][0].state == State.missing_param
        assert states['string_field'][0].param_name == 'index'
        assert states['string_field'][0].param_value == 'not_analyzed'
        assert 'index' in states['string_field'][0].description
        assert 'not_analyzed' in states['string_field'][0].description

    def test_double_missing_param(self):
        # Post index with no parameters
        self._create_index(TestIntMapper,
                           properties={"int_field": {"type": "integer"}})
        # Export some data using a mapper with some defined parameters
        test_args = {
            "int_field": 666,
        }
        _, states = self._export_and_diff(TestIntMapper, test_args)
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
        self._create_index(TestIntMapper)
        test_args = {
            "int_field": 666,
            "dynamic_field": "dynamic_string",
        }
        _, states = self._export_and_diff(TestIntMapper, test_args)
        assert any([s.state == State.extra_field for s in states['dynamic_field']])

    def test_extra_param(self):
        # NOTE: this relies on the fact that adding a boost parameter to an integer field
        # automatically adds a norms parameter to the index. This is possibly going to break when
        # using different versions of ES.
        self._create_index(TestIntMapper)
        test_args = {
            "int_field": 666,
        }
        _, states = self._export_and_diff(TestIntMapper, test_args)
        conflict = states['int_field'][0]
        assert conflict.state == State.extra_param
        assert conflict.param_name == 'norms'

    def test_missing_field(self):
        # export the mapping for TestStringMapper
        self._create_index(TestStringMapper)

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
        states = self._diff(mapper)
        assert any([s.state == State.missing_field for s in states['new_string_field']])

    def test_type_conflict(self):
        # export the mapping for TestStringMapper
        self._create_index(TestStringMapper)
        # define the same mapper with a new field (as if the mapper had been updated)

        @elastic_templates.register('test_type_string', TestStringTemplate)
        class TestNewStringMapper(mappers.Mapper):
            string_field = mappers.IntegerField(precision_step=16)  # updated field (string -> int)

        test_args = {
            "string_field": 16,
        }
        mapper = TestNewStringMapper(test_args)
        states = self._diff(mapper)
        assert len(states['string_field']) == 1
        conflict = states['string_field'][0]
        assert conflict.state == State.type_conflict
        assert conflict.source_type == 'string'
        assert conflict.dest_type == 'integer'

    def test_param_conflict(self):
        # export the mapping for TestStringMapper
        self._create_index(TestStringMapper)

        # define the same mapper with a new field (as if the mapper had been updated)
        @elastic_templates.register('test_type_string', TestStringTemplate)
        class TestNewStringMapper(mappers.Mapper):
            string_field = mappers.StringField(index='analyzed')  # updated param (not_analyzed -> analyzed) # noqa

        test_args = {
            "string_field": "string_value",
        }
        mapper = TestNewStringMapper(test_args)
        states = self._diff(mapper)
        assert len(states['string_field']) == 1
        conflict = states['string_field'][0]
        assert conflict.state == State.param_conflict
        assert conflict.param_name == 'index'
        assert conflict.source_param == 'not_analyzed'
        assert conflict.dest_param == 'analyzed'
