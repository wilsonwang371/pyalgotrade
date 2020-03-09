#!/usr/bin/env python
import datetime as dt
import enum
import json
import logging
import os
import pickle
import sys
import time

import pika

import pyalgotrade.logger
from pyalgotrade import bar
from pyalgotrade.utils.misc import protected_function
from pyalgotrade.fsm import StateMachine, state

logger = pyalgotrade.logger.getLogger('producer')

RECOVER_TIME_DEFAULT = 10
RECOVER_TIME_WAIT_MAX = 60*60
QUEUE_NAME = 'xauusd'
RABBITMQ_AMQP_URL_DEFAULT = 'amqp://guest:guest@localhost/%2f'


class DataProducerStates(enum.Enum):
    INIT = 0
    CONNECTING = 1
    CONNECTED = 2
    CLOSING = 3
    DISCONNECTED = 4
    ERROR = 5
    TERMINATING = 6


class DataProducer(StateMachine):

    @state(DataProducerStates.INIT, True)
    @protected_function(DataProducerStates.ERROR)
    def init(self):
        logger.debug('INIT')
        self.recover_time = RECOVER_TIME_DEFAULT
        self.url = os.environ.get('RABBITMQ_AMQP_URL', RABBITMQ_AMQP_URL_DEFAULT)
        return DataProducerStates.CONNECTING
    
    @state(DataProducerStates.CONNECTING, False)
    @protected_function(DataProducerStates.DISCONNECTED)
    def connecting(self):
        logger.debug('CONNECTING')
        params = pika.URLParameters(self.url)
        params.socket_timeout = 5

        connection = pika.BlockingConnection(params) # Connect to CloudAMQP
        channel = connection.channel()

        channel.queue_declare(queue=QUEUE_NAME) # Declare a queue
        self.connection = connection
        self.channel = channel
        return DataProducerStates.CONNECTED

    @state(DataProducerStates.CONNECTED, False)
    @protected_function(DataProducerStates.DISCONNECTED)
    def connected(self):
        logger.debug('CONNECTED')
        self.recover_time = RECOVER_TIME_DEFAULT

        data = {
            'timestamp': dt.datetime.now().timestamp(),
            'open': 1,
            'high': 1,
            'low': 1,
            'close': 1,
            'freq': bar.Frequency.REALTIME}
        try:
            self.channel.basic_publish(exchange='', routing_key=QUEUE_NAME, body=pickle.dumps(data))
        except:
            logger.error('Cannot serialize data %s' % str(data))

        sys.stderr.write('.')
        sys.stderr.flush()
        time.sleep(10)
        return DataProducerStates.CONNECTED

    @state(DataProducerStates.DISCONNECTED, False)
    @protected_function(DataProducerStates.DISCONNECTED)
    def disconnected(self):
        logger.debug('DISCONNECTED')
        if hasattr(self, 'channel'):
            delattr(self, 'channel')
        if hasattr(self, 'connection'):
            delattr(self, 'connection')
        logger.info('Retry in %s seconds' % self.recover_time)
        time.sleep(self.recover_time)
        self.recover_time = min(self.recover_time * 2, RECOVER_TIME_WAIT_MAX)
        return DataProducerStates.CONNECTING

    @state(DataProducerStates.CLOSING, False)
    @protected_function(DataProducerStates.TERMINATING)
    def closing(self):
        logger.debug('CLOSING')
        self.connection.close()
        return DataProducerStates.TERMINATING

    @state(DataProducerStates.TERMINATING, False)
    def terminating(self):
        logger.info('Terminating...')
        sys.exit(-1)

if __name__ == '__main__':
    a = DataProducer()
    while True:
        a.run()
