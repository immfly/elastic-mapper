from elastic_mapper import mappers, templates


class TestMapper(object):

    def test_default_source_field(self):
    	class TestMapper(mappers.Mapper):
            test_attr = mappers.StringField()

        test_args = {
            'test_attr': 'test value',
        }
        tm = TestMapper(test_args)
        assert tm.data == test_args

    def test_non_default_source_field(self):
    	class TestMapper(mappers.Mapper):
    	    test_attr_with_source = mappers.StringField(source='test_attr')

	test_args = {
            'test_attr': 'test value',
        }
        tm = TestMapper(test_args)
        assert tm.data == {'test_attr_with_source': 'test value'}

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
        assert tm.data == test_args

    def test_dynamic_attribute(self):
    	class TestMapper(mappers.Mapper):
    	    test_attr = mappers.StringField()
    	    # no such field `dynamic_attr`

    	test_args = {
    	    'test_attr': 'test value',
    	    'dynamic_attr': 3,
    	}
	tm = TestMapper(test_args)
	assert 'dynamic_attr' in tm.data
        assert tm.data == test_args