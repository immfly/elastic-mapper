import json
import logging
from abc import ABCMeta, abstractmethod

import six

# set logging config for log based backends
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
handler.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(message)s')
handler.setFormatter(formatter)

logger.addHandler(handler)


@six.add_metaclass(ABCMeta)
class ExportBackend(object):

    @abstractmethod
    def export(self, mapper):
        pass


class LoggingExportBackend(ExportBackend):

    def __init__(self, *args, **kwargs):
        self.indent = kwargs.pop('indent', None)

    def export(self, mapper):
        data = json.dumps(mapper.mapped_data, indent=self.indent)
        logger.info("%s: %s" % (mapper.__class__.__name__, data))
