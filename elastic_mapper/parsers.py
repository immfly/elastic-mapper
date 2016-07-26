from abc import ABCMeta, abstractmethod

import six
import datetime


@six.add_metaclass(ABCMeta)
class IndexParser(object):

    @abstractmethod
    def parse(self, index, mapper):
        """
        """
        pass


class TimeParser(IndexParser):

    def __init__(self, format='%Y%m%d'):
        self.format = format

    def parse(self, index, mapper):
        # TODO: use configurable timestamp
        now = datetime.datetime.now()
        return index.format(time=self.get_time_string(now))

    def get_time_string(self, timestamp):
        return timestamp.strftime(self.format)


class YearlyParser(TimeParser):

    def __init__(self):
        super(YearlyParser, self).__init__(format='%Y')


class MonthlyParser(TimeParser):

    def __init__(self):
        super(MonthlyParser, self).__init__(format='%Y%m')


class DailyParser(TimeParser):
    pass
