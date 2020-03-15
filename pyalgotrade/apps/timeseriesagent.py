import argparse
import datetime as dt
import enum
import errno
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, wait

from six.moves.queue import Queue

import coloredlogs
import pika
import pyalgotrade.bar as bar
import pyalgotrade.logger
from pyalgotrade.fsm import StateMachine, state
from pyalgotrade.mq import MQConsumer, MQProducer
from pyalgotrade.utils.misc import protected_function, pyGo

coloredlogs.install(level='INFO')
logger = pyalgotrade.logger.getLogger(__name__)


class OHLCData:

    def __init__(self, freq):
        self.__freq = freq
        self.reset()
        self.__buf = Queue()

    def reset(self):
        self.__open_val = self.__high_val = self.__low_val = self.__close_val = None
        self.__begin_ts = self.__end_ts = None
        self.__count = 0
        self.__volume_val = 0.0

    def add(self, timestamp_val,
        open_val, high_val, low_val, close_val,
        volume_val=0.0):
        if self.__begin_ts is None:
            self.__begin_ts = timestamp_val
        if self.__end_ts is not None and timestamp_val < self.__end_ts:
            logger.info('old data received')
            return
        
        # check if there is a large gap between begin timestamp and end timestamp
        tsdiff = timestamp_val - self.__begin_ts
        if tsdiff >= self.__freq and tsdiff < 2 * self__freq:
            #TODO: add more logic
            pass

        # initial timestamp checking done
        self.__end_ts = timestamp_val
        if self.__open_val is None:
            self.__open_val = open_val
        if self.__high_val is None or self.__high_val < high_val:
            self.__high_val = high_val
        if self.__low_val is None or self.__low_val > low_val:
            self.__low_val = low_val
        self.__close_val = close_val
        self.__volume_val += volume_val
        self.__count += 1

    @property
    def begints(self):
        return self.__begin_ts

    @property
    def endts(self):
        return self.__end_ts

    @property
    def open(self):
        return self.__open_val

    @property
    def high(self):
        return self.__high_val

    @property
    def low(self):
        return self.__low_val

    @property
    def close(self):
        return self.__close_val

    @property
    def volume(self):
        return self.__volume_val

    @property
    def count(self):
        return self.__count

    def __str__(self):
        begindate = dt.datetime.fromtimestamp(self.__begin_ts)
        enddate = dt.datetime.fromtimestamp(self.__end_ts)
        return ('<OHLCData BeginTs:[{},{}] EndTs:[{},{}] '
            'Open:{} High:{} Low:{} Close:{} Volume:{} Count:{}>').format(
            self.__begin_ts, begindate,
            self.__end_ts, enddate,
            self.__open_val,
            self.__high_val,
            self.__low_val,
            self.__close_val,
            self.__volume_val,
            self.__count)

    def generate_olhc(self):
        if (self.__open_val is None or self.__high_val is None or
            self.__low_val is None or self.__close_val is None):
            return None
        return {
            'open': self.__open_val,
            'high': self.__high_val,
            'low': self.__low_val,
            'close': self.__close_val,
            'volume': self.__volume_val,
            'ts_begin': self.__begin_ts,
            'ts_end': self.__end_ts,
            'freq': self.__freq
        }


class TimeSeriesAgentFSMState(enum.Enum):

    INIT = 1
    READY = 2
    RETRY = 3
    ERROR = -1


class TimeSeriesAgent(StateMachine):

    def __init__(self, url, inqueue, outqueue, freqs):
        super(TimeSeriesAgent, self).__init__()
        self.__url = url
        self.__inqueue = inqueue
        self.__outqueue = outqueue
        self.__freqs = []
        for i in freqs:
            if i == 'hour':
                self.__freqs.append(bar.Frequency.HOUR)
            elif i == 'day':
                self.__freqs.append(bar.Frequency.DAY)
        if len(freqs) == 0:
            logger.info('no extra data frequency dispatching')
        elif len(freqs) == 1:
            logger.info('generating frequency at {}'.format(self.__freqs[0]))
        else:
            logger.info('generating frequencies at {}'.format(self.__freqs))

    @state(TimeSeriesAgentFSMState.INIT, True)
    @protected_function(TimeSeriesAgentFSMState.ERROR)
    def state_init(self):
        self.__consumer = MQConsumer(self.__url, self.__inqueue)
        self.__producer = MQProducer(self.__url, self.__outqueue)
        self.__inbuf = Queue()
        self.__outbuf = Queue()
        def in_task():
            while True:
                tmp = self.__consumer.fetch_one()
                self.__inbuf.put(tmp)
        def out_task():
            while True:
                tmp = self.__outbuf.get()
                self.__producer.put_one(tmp)
        self.__consumer.start()
        self.__producer.start()
        pyGo(in_task)
        pyGo(out_task)
        return TimeSeriesAgentFSMState.READY

    @state(TimeSeriesAgentFSMState.READY, False)
    @protected_function(TimeSeriesAgentFSMState.ERROR)
    def state_ready(self):
        #TODO: fetch inbuf and put outbuf
        itm = self.__inbuf.get()
        logger.info(itm)
        return TimeSeriesAgentFSMState.READY
    
    @state(TimeSeriesAgentFSMState.RETRY, False)
    @protected_function(TimeSeriesAgentFSMState.ERROR)
    def state_retry(self):
        return TimeSeriesAgentFSMState.READY
    
    @state(TimeSeriesAgentFSMState.ERROR, False)
    def state_error(self):
        logger.error('Fatal error, terminating...')
        sys.exit(errno.EFAULT)


def parse_args():
    parser = argparse.ArgumentParser(prog=sys.argv[0],
        description='IB(Interactive Brokers) data agent.')
    parser.add_argument('-i', '--inqueue', dest='inqueue',
        required=True,
        help='input message queue name')
    parser.add_argument('-o', '--outqueue', dest='outqueue',
        required=True,
        help='output message queue name')
    parser.add_argument('-u', '--url', dest='url',
        required=True,
        help='amqp protocol url')
    parser.add_argument('-f','--frequency', action='append',
        dest='freq', choices=['hour', 'day'], default=[],
        help='time series frequencies we want to generate')
    return parser.parse_args()


def main():
    args = parse_args()
    agent = TimeSeriesAgent(args.url,
        args.inqueue, args.outqueue,
        args.freq)
    try:
        while True:
            agent.run()
    except KeyboardInterrupt:
        logger.info('Terminating...')
    except Exception:
        logger.error(traceback.format_exc())


# PYTHONPATH='./' python3 ./pyalgotrade/apps/timeseriesagent.py -i xauusd -q tsxauusd -u "amqp://guest:guest@localhost/%2f"
if __name__ == '__main__':
    main()
