import abc

import six


@six.add_metaclass(abc.ABCMeta)
class MuxPlugin:

    @abc.abstractmethod
    def process(self, key, data):
        raise NotImplementedError('Need to implement MuxPlugin process method')
