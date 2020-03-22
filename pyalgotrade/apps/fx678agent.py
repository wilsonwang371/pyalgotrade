import argparse
import datetime as dt
import enum
import errno
import json
import random
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, wait

import coloredlogs
import pandas as pd
import pika
import pyalgotrade.bar as bar
import pyalgotrade.logger
from pyalgotrade.apps.utils.net import WebRequest
from pyalgotrade.bar import Frequency
from pyalgotrade.barfeed.driver.ibdatadriver import IBDataDriver
from pyalgotrade.fsm import StateMachine, state
from pyalgotrade.mq import MQProducer
from pyalgotrade.utils.misc import protected_function, pyGo

coloredlogs.install(level='INFO')
logger = pyalgotrade.logger.getLogger(__name__)


SLEEP_TIME = 30
DATA_EXPIRE_SECONDS = 120
RETRY_SLEEP_TIME = 60
RETRY_COUNT_MAX = 5

supported_symbols = {
    'XAUUSD': ['XAU', 'WGJS'],
    'XAGUSD': ['XAG', 'WGJS'],
    'WTI': ['CONC', 'NYMEX'],
    'USDIDX': ['USD', 'WH'],
    'NASDAQ': ['NASDAQ', 'GJZS'],
    'DOWJOHNS': ['DJIA', 'GJZS'],
    '@GC': ['GLNC', 'COMEX'],
    '@SI': ['SLNC', 'COMEX'],
}

#QUOTE_BASE_URL = 'https://api.q.fx678.com/getQuote.php?'
QUOTE_BASE_URL = 'https://api-q.fx678img.com/getQuote.php?'
HEADERS = {
    'User-Agent':
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0',
    'Origin': 'http://quote.fx678.com',
    'Host': 'api.q.fx678.com',
}


def build_headers(symbol):
    tmp = HEADERS.copy()
    tmp.update({'Referer': 'http://quote.fx678.com/HQApi/%s' % symbol})
    return tmp

def build_quote(symbol, exchange_name):
    '''
    Build a quote string from symbol
    Example:
    exchName=WGJS&symbol=XAU&st=0.8725721536786181
    '''
    apidict = dict()
    apidict['exchName'] = exchange_name
    apidict['symbol'] = symbol
    apidict['st'] = random.random()
    return apidict

def process_rawdata(raw_data):
    """
    Convert raw json data to dataframe
    :return:
    """
    assert raw_data is not None
    data = {}
    if len(raw_data['t']) == 0:
        return None
    tmp = zip(raw_data['t'], raw_data['o'],
                raw_data['h'], raw_data['l'],
                raw_data['c'], raw_data['v'])
    for i in tmp:
        t, o, h, l, c, v = i
        data['open'] = float(o)
        data['high'] = float(h)
        data['low'] = float(l)
        data['close'] = float(c)
        data['volume'] = float(v)
        data['freq'] = Frequency.REALTIME
        data['timestamp'] = int(t) + 1
        data['source'] = 'fx678'
    return data


class FX678DataAgentFSMStates(enum.Enum):

    INIT = 1
    READY = 2
    RETRY = 3
    ERROR = -1


class FX678DataAgent(StateMachine):
    
    def __init__(self, url, symbol, queue):
        super(FX678DataAgent, self).__init__()
        self.__url = url
        self.__symbol = symbol
        self.__queue = queue
        self.failed_count = 0
        self.producer = MQProducer(self.__url, self.__queue)
        self.producer.start()

    @state(FX678DataAgentFSMStates.INIT, True)
    @protected_function(FX678DataAgentFSMStates.ERROR)
    def state_init(self):
        match = []
        for i in supported_symbols.keys():
            if i.lower().find(self.__symbol.lower()) != -1:
                match.append(i)
        assert len(match) == 1
        self.symbol = supported_symbols[match[0]][0]
        exchange_name = supported_symbols[match[0]][1]
        self.api_quote_dict = build_quote(self.symbol, exchange_name)
        if not self.api_quote_dict:
            raise Exception('Invalid quote')
        expire = 1000 * DATA_EXPIRE_SECONDS
        self.producer.properties = pika.BasicProperties(expiration=str(expire))
        return FX678DataAgentFSMStates.READY

    @state(FX678DataAgentFSMStates.READY, False)
    @protected_function(FX678DataAgentFSMStates.ERROR)
    def state_ready(self):
        req = WebRequest(QUOTE_BASE_URL, headers=build_headers(self.symbol),
            params=self.api_quote_dict)
        data = req.download_page()
        if data is None:
            logger.error('No data downloaded.')
            return FX678DataAgentFSMStates.RETRY
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        try:
            jsondata = json.loads(data)
        except Exception as e:
            logger.error('{0}: {1} invalid json data {2}'.format(e.__class__.__name__,
                                                                 e, data))
            return FX678DataAgentFSMStates.RETRY
        if not isinstance(jsondata, dict):
            logger.error('Invalid return data {0}'.format(data))
            return FX678DataAgentFSMStates.RETRY
        assert 's' in jsondata.keys()
        assert 't' in jsondata.keys()
        assert 'c' in jsondata.keys()
        assert 'o' in jsondata.keys()
        assert 'h' in jsondata.keys()
        assert 'l' in jsondata.keys()
        assert 'p' in jsondata.keys()
        assert 'v' in jsondata.keys()
        assert 'b' in jsondata.keys()
        assert 'se' in jsondata.keys()
        if jsondata['s'] != 'ok':
            logger.error('Download data failed')
            return FX678DataAgentFSMStates.RETRY
        raw_data = jsondata
        data = process_rawdata(raw_data)

        #Now we got the data, we need to dispatch it
        self.producer.put_one(data)

        self.failed_count = 0
        time.sleep(SLEEP_TIME)
        return FX678DataAgentFSMStates.READY

    @state(FX678DataAgentFSMStates.RETRY, False)
    @protected_function(FX678DataAgentFSMStates.ERROR)
    def state_retry(self):
        logger.info('Retrying in {} seconds...'.format(RETRY_SLEEP_TIME))
        self.failed_count += 1
        if self.failed_count >= RETRY_COUNT_MAX:
            return FX678DataAgentFSMStates.ERROR
        time.sleep(RETRY_SLEEP_TIME)
        return FX678DataAgentFSMStates.READY

    @state(FX678DataAgentFSMStates.ERROR, False)
    def state_error(self):
        logger.error('Fatal system error')
        sys.exit(errno.EFAULT)


def parse_args():
    parser = argparse.ArgumentParser(prog=sys.argv[0],
        description='FX678(汇通财经) data agent.')
    parser.add_argument('-s', '--symbol', dest='symbol',
        required=True,
        help='strategy resource symbol name')
    parser.add_argument('-q', '--queue', dest='queue',
        required=True,
        help='message queue name')
    parser.add_argument('-u', '--url', dest='url',
        required=True,
        help='amqp protocol url')
    return parser.parse_args()


def main():
    args = parse_args()
    if args.symbol not in supported_symbols:
        logger.error('Unsupported symbol. Supported symbols are: {}'.format(list(supported_symbols.keys())))
        sys.exit(errno.EINVAL)
    agent = FX678DataAgent(args.url, args.symbol, args.queue)
    try:
        while True:
            agent.run()
    except KeyboardInterrupt:
        logger.info('Terminating...')
    except Exception:
        logger.error(traceback.format_exc())


# PYTHONPATH='./' python3 ./pyalgotrade/apps/ibagent.py -s XAUUSD -q xauusd -u "amqp://guest:guest@localhost/%2f"
if __name__ == '__main__':
    main()
