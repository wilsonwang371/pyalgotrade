import argparse
import json
import random
import time

import six

import coloredlogs
import pyalgotrade.logger
from pyalgotrade.apps.utils.net import WebRequest
from pyalgotrade.apps.utils.plugin import Plugin
from pyalgotrade.bar import Frequency
from pyalgotrade.utils.misc import protected_function

coloredlogs.install(level='INFO')
logger = pyalgotrade.logger.getLogger(__name__)

# PYTHONPATH='./' python3 ./pyalgotrade/apps/task.py -o raw_xauusd -f ./plugins/fx678agent.py -a='-s XAUUSD'
# PYTHONPATH='./' python3 ./pyalgotrade/apps/task.py -o raw_gc -f ./plugins/fx678agent.py -a='-s @GC'


SLEEP_TIME = 20
DATA_EXPIRE_SECONDS = 120

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

def process_rawdata(raw_data, symbol):
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
        data['symbol'] = symbol
        data['timestamp'] = int(t) + 1
        data['open'] = float(o)
        data['high'] = float(h)
        data['low'] = float(l)
        data['close'] = float(c)
        data['volume'] = float(v)
        data['freq'] = Frequency.REALTIME
        data['source'] = 'fx678'
    return data


class Fx678DataPlugin(Plugin):

    def __init__(self, *inargs):
        parser = argparse.ArgumentParser(prog=self.__class__.__name__,
            description='FX678(汇通财经) data agent.')
        parser.add_argument('-s', '--symbol', dest='symbol',
            required=True,
            help='strategy resource symbol name')
        args = parser.parse_args(inargs)
        
        match = []
        self.__symbol = args.symbol
        for i in supported_symbols.keys():
            if i.lower().find(self.__symbol.lower()) != -1:
                match.append(i)
        assert len(match) == 1
        self.symbol = supported_symbols[match[0]][0]
        exchange_name = supported_symbols[match[0]][1]
        self.api_quote_dict = build_quote(self.symbol, exchange_name)
        if not self.api_quote_dict:
            raise Exception('Invalid quote')

    @protected_function(None)
    def process(self, key, data):
        time.sleep(SLEEP_TIME)
        req = WebRequest(QUOTE_BASE_URL, headers=build_headers(self.symbol),
            params=self.api_quote_dict)
        data = req.download_page()
        if data is None:
            logger.error('No data downloaded.')
            return None
        if isinstance(data, bytes):
            data = data.decode('utf-8')
        try:
            jsondata = json.loads(data)
        except Exception as e:
            logger.error('{0}: {1} invalid json data {2}'.format(e.__class__.__name__,
                                                                 e, data))
            return None
        if not isinstance(jsondata, dict):
            logger.error('Invalid return data {0}'.format(data))
            return None
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
            return None
        raw_data = jsondata
        data = process_rawdata(raw_data, self.symbol)
        return data
