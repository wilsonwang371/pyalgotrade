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
    
    def __init__(self):
        self.__open = self.__high = self.__low = self.__close = None
        self.begin_ts = self.end_ts = None

    def add(self, timestamp, value):
        if self.begin_ts is None:
            self.begin_ts = timestamp
        self.end_ts = timestamp
        if self.__open is None:
            self.__open = value
        if self.__high is None or self.__high < value:
            self.__high = value
        if self.__low is None or self.__low > value:
            self.__low = value
        self.__close = value

    @property
    def open(self):
        return self.__open
    
    @property
    def high(self):
        return self.__high
    
    @property
    def low(self):
        return self.__low

    @property
    def close(self):
        return self.__close


class TimeSeriesAgentFSMState(enum.Enum):

    INIT = 1
    READY = 2
    RETRY = 3
    ERROR = -1


class TimeSeriesAgent(StateMachine):

    def __init__(self, url, inqueue, outqueue):
        super(TimeSeriesAgent, self).__init__()
        self.__url = url
        self.__inqueue = inqueue
        self.__outqueue = outqueue

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
    return parser.parse_args()


def main():
    args = parse_args()
    agent = TimeSeriesAgent(args.url, args.inqueue, args.outqueue)
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
