import argparse
import datetime as dt
import enum
import errno
import importlib.machinery as machinery
import importlib.util as util
import inspect
import os
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, wait

import six
from six.moves.queue import Queue

import coloredlogs
import pika
import pyalgotrade.bar as bar
import pyalgotrade.logger
from pyalgotrade.apps.utils.muxplugin import MuxPlugin
from pyalgotrade.fsm import StateMachine, state
from pyalgotrade.mq import MQConsumer, MQProducer
from pyalgotrade.utils.misc import protected_function, pyGo

coloredlogs.install(level='INFO')
logger = pyalgotrade.logger.getLogger(__name__)

DATA_EXPIRE_SECONDS = 120


class MultiplexerFSMState(enum.Enum):

    INIT = 1
    READY = 2
    RETRY = 3
    ERROR = -1


def load_plugin(filename):
    if not os.path.isfile(filename):
        raise FileNotFoundError('no such file: ' + filename)
    loader = machinery.SourceFileLoader('StrategyFSM', filename)
    spec = util.spec_from_loader(loader.name, loader)
    mod = util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    candids = []
    for i in dir(mod):
        item = getattr(mod, i)
        if inspect.isclass(item) and MuxPlugin in item.__bases__:
            candids.append(i)
    if len(candids) > 1:
        raise ValueError('more than one MuxPlugin subclass. {}'.format(str(candids)))
    if len(candids) == 0:
        raise ValueError('no MuxPlugin subclass.')
    return (candids[0], getattr(mod, candids[0]))


class Multiplexer(StateMachine):

    def __init__(self, params, inexchange_list, outexchange, plugin):
        super(Multiplexer, self).__init__()
        assert len(inexchange_list) != 0
        self.__params = params
        self.__inexchange_list = inexchange_list
        self.__outexchange = outexchange
        self.__plugin = plugin
        #self.__update_condition = threading.Condition()

    @state(MultiplexerFSMState.INIT, True)
    @protected_function(MultiplexerFSMState.ERROR)
    def state_init(self):
        self.__consumer = {}
        self.last_values = {}
        self.__inbuf = Queue()
        for i in self.__inexchange_list:
            self.__consumer[i] = MQConsumer(self.__params, i,
                queue_name='{}_MultiplexerQueue'.format(i.upper()))
            self.last_values[i] = None
            #self.__inbuf[i] = Queue()
        self.__producer = MQProducer(self.__params, self.__outexchange)
        expire = 1000 * DATA_EXPIRE_SECONDS
        self.__producer.properties = pika.BasicProperties(expiration=str(expire))
        self.__outbuf = Queue()
        def in_task(key, itm):
            while True:
                tmp = itm.fetch_one()
                #self.__update_condition.acquire()
                self.__inbuf.put([key, tmp])
                #self.__update_condition.notify()
                #self.__update_condition.release()
        for key, val in six.iteritems(self.__consumer):
            self.__consumer[key].start()
            pyGo(in_task, key, val)
        def out_task():
            while True:
                tmp = self.__outbuf.get()
                self.__producer.put_one(tmp)
        self.__producer.start()
        pyGo(out_task)
        return MultiplexerFSMState.READY

    @state(MultiplexerFSMState.READY, False)
    @protected_function(MultiplexerFSMState.ERROR)
    def state_ready(self):
        res = None
        #updated = False
        #self.__update_condition.acquire()
        #while updated is False:
        #    for k, v in six.iteritems(self.__inbuf):
        #        while not v.empty():
        #            self.last_values[k] = v.get()
        #            updated = True
        #    if not updated:
        #        self.__update_condition.wait()
        #tmp = self.last_values.copy()
        #self.__update_condition.release()
        try:
            while not self.__inbuf.empty():
                key, itm = self.__inbuf.get()
                res = self.__plugin.process(key, itm)
        except Exception as e:
            logger.error('Mux plugin exception {}.'.format(str(e)))
        if res is not None:
            self.__outbuf.put(res)
        return MultiplexerFSMState.READY
    
    @state(MultiplexerFSMState.RETRY, False)
    @protected_function(MultiplexerFSMState.ERROR)
    def state_retry(self):
        return MultiplexerFSMState.READY
    
    @state(MultiplexerFSMState.ERROR, False)
    def state_error(self):
        logger.error('Fatal error, terminating...')
        sys.exit(errno.EFAULT)


def parse_args():
    parser = argparse.ArgumentParser(prog=sys.argv[0],
        description='Multiplexer for multiple input data processing.')
    parser.add_argument('-i', '--inexchange', dest='inexchange_list',
        action='append', default=[],
        help=('input message exchange names, you can specify multiple '
            'inputs by using this option multiple times.'))
    parser.add_argument('-o', '--outexchange', dest='outexchange',
        required=True,
        help='output message exchange name')
    parser.add_argument('-f', '--muxplugin-file', dest='file',
        required=True,
        help='multiplexer plugin python file to load.')

    parser.add_argument('-U', '--user', dest='username',
        default='guest',
        help='RabbitMQ username, default: guest')
    parser.add_argument('-P', '--pass', dest='password',
        default='guest',
        help='RabbitMQ password, default: guest')
    parser.add_argument('-H', '--host', dest='host',
        default='localhost',
        help='RabbitMQ hostname, default: localhost')
    return parser.parse_args()


def main():
    args = parse_args()

    _, muxplugin_class = load_plugin(args.file)
    credentials = pika.PlainCredentials(args.username, args.password)
    params = pika.ConnectionParameters(host=args.host,
        socket_timeout=5,
        credentials=credentials,
        client_properties={
            'connection_name': 'multiplexer',
        })
    agent = Multiplexer(params,
        args.inexchange_list, args.outexchange, muxplugin_class())
    try:
        while True:
            agent.run()
    except KeyboardInterrupt:
        logger.info('Terminating...')
    except Exception:
        logger.error(traceback.format_exc())


# PYTHONPATH='./' python3 ./pyalgotrade/apps/multiplexer.py -i raw_xauusd -i raw_gc -o processed_mux -f ./samples/muxplugins/mymuxplugin.py
if __name__ == '__main__':
    main()
