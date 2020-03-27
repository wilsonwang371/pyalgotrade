import argparse

import six
import uuid
import coloredlogs
import pyalgotrade.logger
import pymongo as pm
from pyalgotrade.apps.utils.muxplugin import MuxPlugin

coloredlogs.install(level='INFO')
logger = pyalgotrade.logger.getLogger(__name__)

# PYTHONPATH='./' python3 ./pyalgotrade/apps/multiplexer.py -i raw_xauusd -i raw_gc -f ./muxplugins/mongodbstore.py -a='-h localhost'

class MongoDBMuxPlugin(MuxPlugin):

    def __init__(self, *inargs):
        parser = argparse.ArgumentParser(prog=self.__class__.__name__,
            description='Multiplexer for multiple input data processing.')
        parser.add_argument('-h', '--host', dest='host', default='localhost',
            help='MongoDB host')
        parser.add_argument('-p', '--port', dest='port', default=27017,
            help='MongoDB port')
        parser.add_argument('-d', '--dbname', dest='dbname',
            default='MongoDB_{}'.format(uuid.uuid4()),
            help='MongoDB database name')
        args = parser.parse_args(inargs)
        self.client = pm.MongoClient(args.host, args.port)
        self.db = self.client[args.dbname]

    def process(self, key, data):
        assert 'symbol' in data
        assert 'timestamp' in data
        assert 'open' in data
        assert 'high' in data
        assert 'low' in data
        assert 'close' in data
        assert 'volume' in data
        assert 'freq' in data
        assert 'source' in data
        collection = self.db[data['symbol']]
        one_record = {
            '_id': data['timestamp'],
            'open': data['open'],
            'high': data['high'],
            'low': data['low'],
            'close': data['close'],
            'volume': data['volume'],
            'freq': data['freq'],
            'source': data['source'],
        }
        result = collection.insert_one(one_record)

