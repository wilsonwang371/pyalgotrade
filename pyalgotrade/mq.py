import datetime
import enum
import json
import logging
import os
import os.path
import pickle
import sys
import threading
import time

import six

import pandas as pd
import pika
import pika.exceptions
import pyalgotrade.logger
from pyalgotrade import bar, dispatchprio, feed
from pyalgotrade.barfeed import BaseBarFeed, MultiFrequencyBarFeed
from pyalgotrade.fsm import StateMachine, state
from pyalgotrade.utils.misc import protected_function, pyGo

# This is only for backward compatibility since Frequency used to be defined here and not in bar.py.
Frequency = bar.Frequency

logger = pyalgotrade.logger.getLogger('msgqueue')

RECOVER_TIME_DEFAULT = 10
RECOVER_TIME_WAIT_MAX = 60*60


class MQConsumerStates(enum.Enum):
    INIT = 0
    CONNECTING = 1
    CONNECTED = 2
    CLOSING = 3
    DISCONNECTED = 4
    ERROR = 5
    TERMINATING = 6

class MQConsumer(StateMachine):

    def __init__(self, server_url, queue_name, *args, **kwargs):
        super(MQConsumer, self).__init__(*args, **kwargs)
        self.databuf = []
        self.databuf_cond = threading.Condition()
        self.url = server_url
        self.queue_name = queue_name

    def start(self):
        ''' execute the statemachine in a separtate thread
        '''
        def runp():
            while True:
                try:
                    self.run()
                except:
                    break
        pyGo(runp)

    def fetch_one(self):
        result = None
        self.databuf_cond.acquire()
        while not self.databuf:
            self.databuf_cond.wait()
        result = self.databuf.pop(0)
        self.databuf_cond.release()
        return result

    @state(MQConsumerStates.INIT, True)
    @protected_function(MQConsumerStates.ERROR)
    def init(self):
        logger.debug('INIT')
        self.recover_time = RECOVER_TIME_DEFAULT
        return MQConsumerStates.CONNECTING
    
    @state(MQConsumerStates.CONNECTING, False)
    @protected_function(MQConsumerStates.DISCONNECTED)
    def connecting(self):
        logger.debug('CONNECTING')
        params = pika.URLParameters(self.url)
        params.socket_timeout = 5

        try:
            connection = pika.BlockingConnection(params) # Connect to CloudAMQP
        except pika.exceptions.AMQPConnectionError:
            logger.error('Failed to connect to AMQP server.')
            return MQConsumerStates.DISCONNECTED
        channel = connection.channel()

        channel.queue_declare(queue=self.queue_name) # Declare a queue
        self.connection = connection
        self.channel = channel
        return MQConsumerStates.CONNECTED

    @state(MQConsumerStates.CONNECTED, False)
    @protected_function(MQConsumerStates.DISCONNECTED)
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
        return MQConsumerStates.CONNECTED

    @state(MQConsumerStates.DISCONNECTED, False)
    @protected_function(MQConsumerStates.DISCONNECTED)
    def disconnected(self):
        logger.debug('DISCONNECTED')
        if hasattr(self, 'channel'):
            delattr(self, 'channel')
        if hasattr(self, 'connection'):
            delattr(self, 'connection')
        logger.info('Retry in %s seconds' % self.recover_time)
        time.sleep(self.recover_time)
        self.recover_time = min(self.recover_time * 2, RECOVER_TIME_WAIT_MAX)
        return MQConsumerStates.CONNECTING

    @state(MQConsumerStates.CLOSING, False)
    @protected_function(MQConsumerStates.TERMINATING)
    def closing(self):
        logger.debug('CLOSING')
        self.connection.close()
        return MQConsumerStates.TERMINATING

    @state(MQConsumerStates.TERMINATING, False)
    def terminating(self):
        logger.info('Terminating...')
        raise Exception('exiting...')



class MQProducerStates(enum.Enum):
    INIT = 0
    CONNECTING = 1
    CONNECTED = 2
    CLOSING = 3
    DISCONNECTED = 4
    ERROR = 5
    TERMINATING = 6


class MQProducer(StateMachine):

    def __init__(self, server_url, queue_name, *args, **kwargs):
        super(MQProducer, self).__init__(*args, **kwargs)
        self.databuf = []
        self.databuf_cond = threading.Condition()
        self.url = server_url
        self.queue_name = queue_name

    def put_one(self, data):
        self.databuf_cond.acquire()
        if not self.databuf:
            self.databuf_cond.notify()
        self.databuf.append(data)
        self.databuf_cond.release()

    def start(self):
        ''' execute the statemachine in a separtate thread
        '''
        def runp():
            while True:
                try:
                    self.run()
                except:
                    break
        pyGo(runp)

    def __fetch_one(self):
        result = None
        self.databuf_cond.acquire()
        while not self.databuf:
            self.databuf_cond.wait()
        result = self.databuf.pop(0)
        self.databuf_cond.release()
        return result

    @state(MQProducerStates.INIT, True)
    @protected_function(MQProducerStates.ERROR)
    def init(self):
        logger.debug('INIT')
        self.recover_time = RECOVER_TIME_DEFAULT
        return MQProducerStates.CONNECTING
    
    @state(MQProducerStates.CONNECTING, False)
    @protected_function(MQProducerStates.DISCONNECTED)
    def connecting(self):
        logger.debug('CONNECTING')
        params = pika.URLParameters(self.url)
        params.socket_timeout = 5

        try:
            connection = pika.BlockingConnection(params) # Connect to CloudAMQP
        except pika.exceptions.AMQPConnectionError:
            logger.error('Failed to connect to AMQP server.')
            return MQProducerStates.DISCONNECTED
        channel = connection.channel()

        channel.queue_declare(queue=self.queue_name) # Declare a queue
        self.connection = connection
        self.channel = channel
        return MQProducerStates.CONNECTED

    @state(MQProducerStates.CONNECTED, False)
    @protected_function(MQProducerStates.DISCONNECTED)
    def connected(self):
        logger.debug('CONNECTED')
        self.recover_time = RECOVER_TIME_DEFAULT

        while True:
            data = self.__fetch_one()
            try:
                data = pickle.dumps(data)
            except:
                logger.error('Cannot serialize data %s' % str(data))
                continue
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=data)

        return MQProducerStates.CONNECTED

    @state(MQProducerStates.DISCONNECTED, False)
    @protected_function(MQProducerStates.DISCONNECTED)
    def disconnected(self):
        logger.debug('DISCONNECTED')
        if hasattr(self, 'channel'):
            delattr(self, 'channel')
        if hasattr(self, 'connection'):
            delattr(self, 'connection')
        logger.info('Retry in %s seconds' % self.recover_time)
        time.sleep(self.recover_time)
        self.recover_time = min(self.recover_time * 2, RECOVER_TIME_WAIT_MAX)
        return MQProducerStates.CONNECTING

    @state(MQProducerStates.CLOSING, False)
    @protected_function(MQProducerStates.TERMINATING)
    def closing(self):
        logger.debug('CLOSING')
        self.connection.close()
        return MQProducerStates.TERMINATING

    @state(MQProducerStates.TERMINATING, False)
    def terminating(self):
        logger.info('Terminating...')
        raise Exception('exiting...')


if __name__ == '__main__':
    import datetime as dt
    data = {
        'timestamp': dt.datetime.now().timestamp(),
        'open': 1,
        'high': 1,
        'low': 1,
        'close': 1,
        'volume': 0,
        'freq': bar.Frequency.REALTIME}
    QUEUE_NAME = 'xauusd'
    RABBITMQ_AMQP_URL_DEFAULT = 'amqp://guest:guest@localhost/%2f'
    p = MQProducer(RABBITMQ_AMQP_URL_DEFAULT, QUEUE_NAME)
    p.start()


    while True:
        time.sleep(10)
        p.put_one(data)
