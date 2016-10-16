from . import BaseESTest, es_host


class TestExport(BaseESTest):

    def test_export(self):
        # 1. export data
        from mappings import TestStringMapper
        test_args = {
            "string_field": "value",
        }
        mapper = TestStringMapper(test_args)
        mapper.export()

        # 2. assert mappings
        es_mappings = es_host.indices.get_mapping(index=mapper.index)
        assert mapper.index in es_mappings.keys()
        mappings = es_mappings[mapper.index]['mappings']
        assert 'test_type_string' in mappings.keys()
        assert 'string_field' in mappings['test_type_string']['properties'].keys()

        # 3. assert data is actually exported
        es_host.indices.refresh(index=mapper.index)
        data = es_host.search(index=mapper.index)
        assert data['hits']['total'] == 1
        assert data['hits']['hits'][0]['_source']['string_field'] == 'value'
