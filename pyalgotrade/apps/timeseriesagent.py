import argparse
import datetime as dt
import enum
import errno
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, wait

import coloredlogs
import pika
import pyalgotrade.bar as bar
import pyalgotrade.logger
from pyalgotrade.barfeed.driver.ibdatadriver import IBDataDriver
from pyalgotrade.fsm import StateMachine, state
from pyalgotrade.mq import MQProducer
from pyalgotrade.utils.misc import protected_function, pyGo

coloredlogs.install(level='INFO')
logger = pyalgotrade.logger.getLogger(__name__)


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
    def state_init(self):
        return TimeSeriesAgentFSMState.READY

    @state(TimeSeriesAgentFSMState.READY, False)
    def state_ready(self):
        return TimeSeriesAgentFSMState.READY
    
    @state(TimeSeriesAgentFSMState.RETRY, False)
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