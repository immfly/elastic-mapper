from mappings import TestStringMapper
from elastic_mapper import mappers
from elastic_mapper import templates as elastic_templates
from templates import TestStringTemplate

from . import BaseESTest, es_host


class TestDiff(BaseESTest):

    def test_versioning(self):
        # mapper = TestStringMapper()
        es_host.indices.create(index="test-v1-201601",
                               body={"mappings": TestStringMapper.generate_mapping()})
        mappings_v1_1 = es_host.indices.get_mapping(index="test-v1-201601")
        print mappings_v1_1
        es_host.indices.create(index="test-v1-201602",
                               body={"mappings": TestStringMapper.generate_mapping()})
        mappings_v1_2 = es_host.indices.get_mapping(index="test-v1-201602")
        print mappings_v1_2

        # create alias
        es_host.indices.put_alias(index="test*", name="test")
        aliases = es_host.indices.get_alias(name="test")
        print "aliases:"
        import pprint
        pprint.pprint(aliases)

        es_host.indices.create(index="test-v1-201603",
                               body={"mappings": TestStringMapper.generate_mapping()})
        mappings_v1_3 = es_host.indices.get_mapping(index="test-v1-201603")
        print mappings_v1_3

        # test_args = {
        #     "string_field": "value",
        # }
        # mapper = TestStringMapper(test_args)
        # es_host.create(index="test", doc_type=mapper.typename, body=test_args)

        aliases = es_host.indices.get_alias(name="test")
        print "aliases:"
        import pprint
        pprint.pprint(aliases)

        @elastic_templates.register('test_type_string', TestStringTemplate)
        class TestStringMapperV2(mappers.Mapper):
            string_field = mappers.IntegerField(precision_step=16)  # updated field (string -> int)

        es_host.indices.create(index="test-v2-201601",
                               body={"mappings": TestStringMapperV2.generate_mapping()})
        mappings_v2_1 = es_host.indices.get_mapping(index="test-v2-201601")
        print mappings_v2_1
        es_host.indices.create(index="test-v2-201602",
                               body={"mappings": TestStringMapperV2.generate_mapping()})
        mappings_v2_2 = es_host.indices.get_mapping(index="test-v2-201602")
        print mappings_v2_2

        assert False
