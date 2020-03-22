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
import pytz
import pytz.tzinfo
from pyalgotrade.fsm import StateMachine, state
from pyalgotrade.mq import MQConsumer, MQProducer
from pyalgotrade.utils.misc import protected_function, pyGo

coloredlogs.install(level='INFO')
logger = pyalgotrade.logger.getLogger(__name__)


class OHLCData:

    def __init__(self, freq, timezone=pytz.timezone('Etc/GMT+2')):
        # Etc/GMT+2 is the ideal timezone for gold price tracking
        assert isinstance(timezone, pytz.tzinfo.BaseTzInfo)
        assert freq in [bar.Frequency.DAY, bar.Frequency.HOUR]
        self.__freq = freq
        self.reset()
        self.__buf = Queue()
        self.__tz = timezone

    def reset(self):
        self.__open_val = self.__high_val = self.__low_val = self.__close_val = None
        self.__begin_ts = self.__end_ts = None
        self.__count = 0
        self.__volume_val = 0.0

    def empty(self):
        return self.__buf.empty()

    def get(self, *args, **kwargs):
        return self.__buf.get(*args, **kwargs)

    def add(self, timestamp_val,
        open_val, high_val, low_val, close_val,
        volume_val=0.0):
        # check if there is a large gap between begin timestamp and end timestamp
        logger.info('ohlc: %.2f, %.3f, %.3f, %.3f, %.3f' % (timestamp_val,
            open_val, high_val, low_val, close_val))
        if self.__begin_ts is not None:
            if self.__freq == bar.Frequency.HOUR:
                cur_hour = dt.datetime.utcfromtimestamp(timestamp_val).replace(minute=0,
                    second=0, microsecond=0)
                begin_hour = dt.datetime.utcfromtimestamp(self.__begin_ts).replace(minute=0,
                    second=0, microsecond=0)
                if cur_hour > begin_hour:
                    # use begin hour to compute a new ohlc data
                    tmpdata = self.generate_olhc()
                    tmpdata['timestamp'] = begin_hour.timestamp()
                    self.__buf.put(tmpdata)
                    logger.info('hour ohlc: {}'.format(tmpdata))
                    self.reset()
            elif self.__freq == bar.Frequency.DAY:
                cur_day = self.__tz.localize(dt.datetime.utcfromtimestamp(timestamp_val)).replace(hour=0,
                    minute=0, second=0, microsecond=0)
                begin_day = self.__tz.localize(dt.datetime.utcfromtimestamp(self.__begin_ts)).replace(hour=0,
                    minute=0, second=0, microsecond=0)
                if cur_day > begin_day:
                    # use begin day to compute a new ohlc data
                    tmpdata = self.generate_olhc()
                    tmpdata['timestamp'] = begin_day.timestamp()
                    self.__buf.put(tmpdata)
                    logger.info('day ohlc: {}'.format(tmpdata))
                    self.reset()

        if self.__begin_ts is None:
            self.__begin_ts = timestamp_val
        if self.__end_ts is not None and timestamp_val < self.__end_ts:
            logger.info('old data received')
            return
        self.__end_ts = timestamp_val

        # initial timestamp checking done
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
        begindate = dt.datetime.utcfromtimestamp(self.__begin_ts)
        enddate = dt.datetime.utcfromtimestamp(self.__end_ts)
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
            'freq': self.__freq,
            'ticks': self.__count,
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
        self.__timeseries = {}
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
        for i in self.__freqs:
            self.__timeseries[i] = OHLCData(i)

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
        itm = self.__inbuf.get()
        self.__outbuf.put(itm)
        for i in sorted(self.__timeseries.keys()):
            ohlcdata = self.__timeseries[i]
            ohlcdata.add(itm['timestamp'], itm['open'], itm['high'],
                itm['low'], itm['close'], itm['volume'])
            while not ohlcdata.empty():
                newdata = ohlcdata.get()
                self.__outbuf.put(newdata)
                logger.info(newdata)
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
