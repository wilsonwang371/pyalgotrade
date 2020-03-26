import abc

import six


@six.add_metaclass(abc.ABCMeta)
class MuxPlugin:

    @abc.abstractmethod
    def process(self, data):
        raise NotImplementedError('Need to implement MuxPlugin process method')
