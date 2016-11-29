import logging

from elastic_mapper import config


class ElasticMapperHandler(logging.Handler, object):
    """
    """

    def emit(self, record):
        print "emit"
        print record
        conf = config.Config()
        mapper = record.args[0]
        for backend in conf.export_backends:
            print backend
            backend.export(mapper)


global_logger = logging.getLogger('elastic_mapper.global_logger')
global_logger.setLevel(logging.INFO)

handler = ElasticMapperHandler()
global_logger.addHandler(handler)
