import pytest
from elasticsearch import Elasticsearch

# TODO: factor this out
ES_HOST = {
    "host": 'localhost',
    "port": 9200,
}
es = Elasticsearch(hosts=[ES_HOST])


class TestExport(object):

    def setup_method(self, method):
        es.indices.delete('test*')
        pass

    def test_export(self):
        es_mappings = es.indices.get_mapping(index='*')
        import pprint
        pprint.pprint(es_mappings)
        assert False