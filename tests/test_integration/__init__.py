from elastic_mapper import config
from elasticsearch import Elasticsearch
from export_backends import TestElasticSearchBackend

# TODO: factor this out
ES_HOST = {
    "host": 'localhost',
    "port": 9200,
}
es_host = Elasticsearch(hosts=[ES_HOST])


class BaseESTest(object):

    def setup_method(self, method):
        conf = config.Config()
        conf.add_export_backend(TestElasticSearchBackend)
        es_host.indices.delete('test*')
