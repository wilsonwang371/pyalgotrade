import datetime
import enum
import json
import logging
import os
import os.path
import pickle
import sys
import threading
import time

import pika
import pyalgotrade.logger
import pyalgotrade.mq as mq
from pyalgotrade import bar, dispatchprio, feed
from pyalgotrade.barfeed import BaseBarFeed, MultiFrequencyBarFeed
from pyalgotrade.fsm import StateMachine, state
from pyalgotrade.utils.misc import protected_function, pyGo

# This is only for backward compatibility since Frequency used to be defined here and not in bar.py.
Frequency = bar.Frequency

logger = pyalgotrade.logger.getLogger('livebarfeed')

class LiveBarFeed(MultiFrequencyBarFeed):

    def __init__(self, instrument, frequencies,
                 maxLen=1000):
        if not isinstance(frequencies, list):
            frequencies = [frequencies]
        super(LiveBarFeed, self).__init__(frequencies, maxLen=maxLen)

        self.__instrument = instrument
        # proactivly register our instrument just incase not found if
        # someone tries to call getDataSeries()
        for i in frequencies:
            self.registerDataSeries(instrument, i)

    def getCurrentDateTime(self):
        raise NotImplementedError()

    def barsHaveAdjClose(self):
        return False

    '''    def getNextBars(self):
        bars = self.__getNextBars()
        return bars

    def __getNextBars(self):
        raise NotImplementedError()'''

    def join(self):
        pass

    def eof(self):
        return False

    def peekDateTime(self):
        return None

    def start(self):
        super(LiveBarFeed, self).start()

    def stop(self):
        pass


class RabbitMQLiveBarFeed(LiveBarFeed):

    def __init__(self, params, instrument, exchange_name, frequencies,
                 maxLen=1000):
        super(RabbitMQLiveBarFeed, self).__init__(instrument, frequencies,
            maxLen)
        self.__instrument = instrument
        self.__exchange_name = exchange_name
        self.__consumer = mq.MQConsumer(params, self.__exchange_name)
        self.__consumer.start()

    def getNextBars(self):
        row = self.__consumer.fetch_one()
        tmp = bar.BasicBar(row['timestamp'], row['open'], row['high'],
                            row['low'], row['close'], row['volume'], False,
                            row['freq'])
        return bar.Bars({self.__instrument: tmp}, frequecy=tmp.getFrequency())
