#!/usr/bin/env python
import enum
import json
import logging
import os
import pickle
import sys
import threading
import time

import pika

import pyalgotrade.logger
from pyalgotrade.utils.misc import protected_function
from pyalgotrade.fsm import StateMachine, state

logger = pyalgotrade.logger.getLogger('consumer')

RECOVER_TIME_DEFAULT = 10
RECOVER_TIME_WAIT_MAX = 60*60
QUEUE_NAME = 'XAUUSD'
RABBITMQ_AMQP_URL_DEFAULT = 'amqp://guest:guest@localhost/%2f'

class DataConsumerStates(enum.Enum):
    INIT = 0
    CONNECTING = 1
    CONNECTED = 2
    CLOSING = 3
    DISCONNECTED = 4
    ERROR = 5
    TERMINATING = 6


class DataConsumer(StateMachine):

    def __init__(self, server_url, queue_name, *args, **kwargs):
        super(DataConsumer, self).__init__(*args, **kwargs)
        self.databuf = []
        self.databuf_cond = threading.Condition()
        self.url = server_url
        self.queue_name = queue_name

    def fetch_one(self):
        result = None
        self.databuf_cond.acquire()
        while not self.databuf:
            self.databuf_cond.wait()
        result = self.databuf.pop(0)
        self.databuf_cond.release()
        return result

    @state(DataConsumerStates.INIT, True)
    @protected_function(DataConsumerStates.ERROR)
    def init(self):
        logger.debug('INIT')
        self.recover_time = RECOVER_TIME_DEFAULT
        return DataConsumerStates.CONNECTING
    
    @state(DataConsumerStates.CONNECTING, False)
    @protected_function(DataConsumerStates.DISCONNECTED)
    def connecting(self):
        logger.debug('CONNECTING')
        params = pika.URLParameters(self.url)
        params.socket_timeout = 5

        connection = pika.BlockingConnection(params) # Connect to CloudAMQP
        channel = connection.channel()

        channel.queue_declare(queue=self.queue_name) # Declare a queue
        self.connection = connection
        self.channel = channel
        return DataConsumerStates.CONNECTED

    @state(DataConsumerStates.CONNECTED, False)
    @protected_function(DataConsumerStates.DISCONNECTED)
    def connected(self):
        logger.debug('CONNECTED')
        self.recover_time = RECOVER_TIME_DEFAULT

        # create a function which is called on incoming messages
        def callback(ch, method, properties, body):
            data_in = body
            try:
                data = pickle.loads(data_in)
                self.databuf_cond.acquire()
                self.databuf.append(data)
                self.databuf_cond.notify()
                self.databuf_cond.release()
            except:
                logger.error('Cannot deserialize data %s' % data_in)

        # set up subscription on the queue
        self.channel.basic_consume(self.queue_name, callback, auto_ack=True)

        # start consuming (blocks)
        self.channel.start_consuming()
        return DataConsumerStates.CONNECTED

    @state(DataConsumerStates.DISCONNECTED, False)
    @protected_function(DataConsumerStates.DISCONNECTED)
    def disconnected(self):
        logger.debug('DISCONNECTED')
        if hasattr(self, 'channel'):
            delattr(self, 'channel')
        if hasattr(self, 'connection'):
            delattr(self, 'connection')
        logger.info('Retry in %s seconds' % self.recover_time)
        time.sleep(self.recover_time)
        self.recover_time = min(self.recover_time * 2, RECOVER_TIME_WAIT_MAX)
        return DataConsumerStates.CONNECTING

    @state(DataConsumerStates.CLOSING, False)
    @protected_function(DataConsumerStates.TERMINATING)
    def closing(self):
        logger.debug('CLOSING')
        self.connection.close()
        return DataConsumerStates.TERMINATING

    @state(DataConsumerStates.TERMINATING, False)
    def terminating(self):
        logger.info('Terminating...')
        sys.exit(-1)

if __name__ == '__main__':
    a = DataConsumer(RABBITMQ_AMQP_URL_DEFAULT, QUEUE_NAME)
    from pyalgotrade.utils.misc import pyGo
    def func():
        while True:
            tmp = a.fetch_one()
            if tmp:
                print(tmp)
    pyGo(func)
    while True:
        a.run()
