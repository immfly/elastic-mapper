from abc import ABCMeta, abstractmethod

import six
import re

import arrow

from elastic_mapper.fields import get_attribute


def get_matching_indexes(key, mappings):
    """Get the list of Elasticsearch indices that match the timely pattern given by `key`."""
    # TODO: add unit test for this
    # TODO: consider matching using versioning (e.g. index-prefix-{version}-{time})
    pattern = re.compile(r"(?<=\{)(.*?)(?=\})")
    matches = pattern.findall(key)
    for match in matches:
        key = key.replace('{' + match + '}', "\\w")

    repl = re.compile(key)
    matches = []
    for key in mappings.keys():
        if repl.match(key):
            matches.append(key)

    return matches


@six.add_metaclass(ABCMeta)
class IndexParser(object):

    @abstractmethod
    def parse(self, index, mapper):
        """
        """
        pass


class TimeParser(IndexParser):

    def __init__(self, time_field=None, format='YYYYMMDD'):
        self.format = format
        self.time_field = time_field

    def parse(self, index, mapper):
        if self.time_field:
            time = get_attribute(mapper.instance, 'timestamp')
        else:
            time = arrow.now()
        return index.format(time=self.get_time_string(time))

    def get_time_string(self, timestamp):
        v = arrow.get(timestamp)
        return v.format(self.format)


class YearlyParser(TimeParser):

    def __init__(self, time_field=None):
        super(YearlyParser, self).__init__(format='YYYY')


class MonthlyParser(TimeParser):

    def __init__(self, time_field=None):
        super(MonthlyParser, self).__init__(format='YYYYMM')


class DailyParser(TimeParser):
    pass
