import os
from abc import ABCMeta, abstractmethod

import six

import importutils
from elastic_mapper.templates import Template


@six.add_metaclass(ABCMeta)
class AbstractMappingLoader(object):

    @abstractmethod
    def get_mappings(self):
        pass

    @abstractmethod
    def get_templates(self):
        pass


class ProjectMappingLoader(AbstractMappingLoader):

    def __init__(self, path):
        self.path = path
        self.mappings = {}
        self.templates = {}

        self.load()

    def pyfiles(self):
        for root, dirs, files in os.walk(self.path, topdown=False):
            pyfiles = (f for f in files if f.split(os.extsep, 1)[-1] == 'py')
            for name in pyfiles:
                path = os.path.join(root, name)
                yield path

    def load(self):
        templates = importutils.search_subclasses(self.path, Template)
        for template in templates:
            self.templates[template.name] = template
            self.mappings.update(template.types)

    def get_mappings(self):
        ret = {}
        for name, mapping in self.mappings.items():
            # TODO: duplicate names?
            ret[name] = mapping.generate_mapping()
        return ret

    def get_templates(self):
        ret = {}
        for name, template in self.templates.items():
            ret[name] = template.generate_template()
        return ret
