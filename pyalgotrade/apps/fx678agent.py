import argparse
import datetime as dt
import enum
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

class FX678DataAgentFSMStates(enum.Enum):
    pass

class FX678DataAgent(StateMachine):
    
    def __init__(self, url, queue, symbol):
        super(FX678DataAgent, self).__init__()
        self.__url = url
        self.__symbol = symbol
        self.__queue = queue


def parse_args():
    # for testing purpose, I use these options:
    #  -s XAUUSD -u "amqp://guest:guest@localhost/%2f"
    parser = argparse.ArgumentParser(prog=sys.argv[0],
        description='FX678(汇通财经) data agent.')
    parser.add_argument('-s', '--symbol', dest='symbol',
        required=True,
        help='strategy resource symbol name')
    parser.add_argument('-u', '--url', dest='url',
        required=True,
        help='amqp protocol url')
    return parser.parse_args()


def main():
    args = parse_args()
    agent = FX678DataAgent(args.url, args.symbol, args.symbol)
    try:
        while True:
            agent.run()
    except KeyboardInterrupt:
        logger.info('Terminating...')
    except Exception:
        logger.error(traceback.format_exc())

# PYTHONPATH='./' python3 ./pyalgotrade/apps/ibagent.py -s XAUUSD -u "amqp://guest:guest@localhost/%2f"

if __name__ == '__main__':
    main()
