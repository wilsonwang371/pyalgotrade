import abc

import six


@six.add_metaclass(abc.ABCMeta)
class Plugin:

    @abc.abstractmethod
    def __init__(self, *args):
        raise NotImplementedError('Need to implement __init__')

    @abc.abstractmethod
    def process(self, key, data):
        # Input: if no input queue declared, process() will be called
        # in a loop with key and data as None
        # Return: a tuple with a key and data
        raise NotImplementedError('Need to implement plugin process method')

    @property
    def keys_in(self):
        return getattr(self, '__keys_in', [])
    
    @keys_in.setter
    def keys_in(self, keys_in):
        self.__keys_in = keys_in
    
    @property
    def keys_out(self):
        return getattr(self, '__keys_out', [])
    
    @keys_out.setter
    def keys_out(self, keys_out):
        self.__keys_out = keys_out
