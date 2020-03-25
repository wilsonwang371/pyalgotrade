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

#GOLD_SYMBOL = 'XAUUSD'
#QUEUE_NAME = GOLD_SYMBOL
#RABBITMQ_AMQP_URL_DEFAULT = 'amqp://guest:guest@localhost/%2f'
#XAUUSD_TUPLE = (GOLD_SYMBOL, 'CMDTY', 'SMART', 'USD', '', 0.0, '')
#GC_CONT_FUTURE_TUPLE = ('@GC', 'FUT', 'SMART', 'USD', '', 0.0, '')

SLEEP_TIME = 10
DATA_EXPIRE_SECONDS = 120

class IBDataAgentFSMState(enum.Enum):
    INIT = 1
    SUBSCRIBING = 2
    SUBSCRIBED = 3
    ERROR = 99

supported_symbols = {
    'XAUUSD': ('XAUUSD', 'CMDTY', 'SMART', 'USD', '', 0.0, ''),
}

class IBDataAgent(StateMachine):

    def __init__(self, params, symbol, outexchange):
        super(IBDataAgent, self).__init__()
        self.__symbol = symbol
        self.__outexchange = outexchange
        self.__params = params

    @state(IBDataAgentFSMState.INIT, True)
    def init(self):
        self.__producer = {}
        self.__contracts = [
            supported_symbols[self.__symbol],
        ]
        expire = 1000 * DATA_EXPIRE_SECONDS
        expiration_prop = pika.BasicProperties(expiration=str(expire))
        for i in self.__contracts:
            tmp = MQProducer(self.params, self.__outexchange)
            tmp.start()
            self.__producer[str(i)] = tmp
            tmp.properties = expiration_prop

        self.__driver = IBDataDriver()
        self.__driver.start()

        self.__tids = {}
        self.__executor = ThreadPoolExecutor(8)
        return IBDataAgentFSMState.SUBSCRIBING

    @state(IBDataAgentFSMState.SUBSCRIBING, False)
    @protected_function(IBDataAgentFSMState.ERROR)
    def subscribing(self):
        while not self.__driver.connected:
            time.sleep(1)
        for i in self.__contracts:
            tid = self.__driver.create_contract(i)
            if not tid:
                logger.error('Cannot create contract for tuple '
                    '%s. Do you have IB Gateway running?' % str(i))
                return IBDataAgentFSMState.ERROR
            self.__tids[str(i)] = tid
            self.__driver.request_market_data(i[0])
        return IBDataAgentFSMState.SUBSCRIBED

    @state(IBDataAgentFSMState.SUBSCRIBED, False)
    @protected_function(IBDataAgentFSMState.ERROR)
    def subscribed(self):
        tasks = []
        def dispatch_task(contract_tuple):
            row = self.__driver.quote(self.__tids[str(contract_tuple)])
            if not row:
                time.sleep(SLEEP_TIME)
                return IBDataAgentFSMState.SUBSCRIBING
            assert row['source'] == 'ib'
            data = {
                'symbol': contract_tuple[0],
                'timestamp': row['time'],
                'open': row['price'],
                'high': row['price'],
                'low': row['price'],
                'close': row['price'],
                'volume': row['price'],
                'source': row['source'],
                'freq': bar.Frequency.REALTIME
            }
            logger.debug('Task running for tuple %s' % str(contract_tuple))
            logger.debug('data %s' % str(data))
            self.__producer[str(contract_tuple)].put_one(data)
            time.sleep(SLEEP_TIME)

        for i in self.__contracts:
            tmp = self.__executor.submit(dispatch_task, (i))
            tasks.append(tmp)

        # wait for task done
        wait(tasks)

        return IBDataAgentFSMState.SUBSCRIBED

    @state(IBDataAgentFSMState.ERROR, False)
    def error(self):
        logger.error('Fatal error, terminating...')
        sys.exit(errno.EFAULT)


def parse_args():
    parser = argparse.ArgumentParser(prog=sys.argv[0],
        description='IB(Interactive Brokers) data agent.')
    parser.add_argument('-s', '--symbol', dest='symbol',
        required=True,
        help='strategy resource symbol name')
    parser.add_argument('-o', '--outexchange', dest='outexchange',
        required=True,
        help='out message exchange name')

    parser.add_argument('-u', '--url', dest='url',
        required=True,
        help='amqp protocol url')
    parser.add_argument('-U', '--user', dest='username',
        default='guest',
        help='RabbitMQ username')
    parser.add_argument('-P', '--pass', dest='password',
        default='guest',
        help='RabbitMQ password')
    parser.add_argument('-H', '--host', dest='host',
        default='localhost',
        help='RabbitMQ hostname')
    return parser.parse_args()


def main():
    args = parse_args()
    if args.symbol not in supported_symbols:
        logger.error('Unsupported symbol. Supported symbols are: {}'.format(list(supported_symbols.keys())))
        sys.exit(errno.EINVAL)
    params = pika.URLParameters(args.url)
    params.socket_timeout = 5
    agent = IBDataAgent(params, args.symbol, args.outexchange)
    try:
        while True:
            agent.run()
    except KeyboardInterrupt:
        logger.info('Terminating...')
    except Exception:
        logger.error(traceback.format_exc())


# PYTHONPATH='./' python3 ./pyalgotrade/apps/ibagent.py -s XAUUSD -i raw_xauusd -u "amqp://guest:guest@localhost/%2f"
if __name__ == '__main__':
    main()
