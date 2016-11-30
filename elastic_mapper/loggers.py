import logging

from elastic_mapper import config


logger = logging.getLogger(__name__)


class ElasticMapperHandler(logging.Handler, object):
    """
    """

    def emit(self, record):
        conf = config.Config()
        mapper = record.args[0]
        for backend in conf.export_backends:
            try:
                backend.export(mapper)
            except Exception as e:
                logger.exception(e)


global_logger = logging.getLogger('elastic_mapper.global_logger')
global_logger.setLevel(logging.INFO)

handler = ElasticMapperHandler()
global_logger.addHandler(handler)
