from elastic_mapper import exporters
from elasticsearch import Elasticsearch
import json


class TestElasticSearchBackend(exporters.ExportBackend):
    """
    Exporter backend to send tracks directly to Elasticsearch for development.
    """

    def __init__(self, host='localhost', port='9200'):
        self.host = host
        self.port = port

    def export(self, mapper):
        es_host = {
            "host": self.host,
            "port": self.port,
        }
        es = Elasticsearch(hosts=[es_host])
        data = json.dumps(mapper.mapped_data)
        es.index(index=mapper.index, doc_type=mapper.typename, body=data)
