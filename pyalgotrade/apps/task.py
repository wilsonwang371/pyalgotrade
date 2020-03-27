import argparse
import datetime as dt
import enum
import errno
import importlib.machinery as machinery
import importlib.util as util
import inspect
import os
import shlex
import sys
import threading
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor, wait

import six
from six.moves.queue import Queue

import coloredlogs
import pika
import pyalgotrade.bar as bar
import pyalgotrade.logger
import pymongo.errors
from pyalgotrade.apps.utils.plugin import Plugin
from pyalgotrade.fsm import StateMachine, state
from pyalgotrade.mq import MQConsumer, MQProducer
from pyalgotrade.utils.misc import protected_function, pyGo

coloredlogs.install(level='INFO')
logger = pyalgotrade.logger.getLogger(__name__)

DATA_EXPIRE_SECONDS = 120


class TaskFSMState(enum.Enum):

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
        if inspect.isclass(item) and Plugin in item.__bases__:
            candids.append(i)
    if len(candids) > 1:
        raise ValueError('more than one plugin subclass. {}'.format(str(candids)))
    if len(candids) == 0:
        raise ValueError('no plugin subclass.')
    return (candids[0], getattr(mod, candids[0]))


class Task(StateMachine):

    def __init__(self, params, inexchange_list, outexchange_list, plugin):
        super(Task, self).__init__()
        self.__params = params
        self.__inexchange_list = inexchange_list
        self.__outexchange_list = outexchange_list
        self.__plugin = plugin

    @state(TaskFSMState.INIT, True)
    @protected_function(TaskFSMState.ERROR)
    def state_init(self):
        self.__consumer = {}
        self.__inbuf = Queue()
        for i in self.__inexchange_list:
            self.__consumer[i] = MQConsumer(self.__params, i,
                queue_name='{}_Task_{}'.format(i.upper(), uuid.uuid4()))
        def in_task(key, itm):
            while True:
                tmp = itm.fetch_one()
                self.__inbuf.put([key, tmp])
        for key, val in six.iteritems(self.__consumer):
            self.__consumer[key].start()
            pyGo(in_task, key, val)
        
        if self.__outexchange_list is None:
            self.__producer = {}
            self.__outbuf = Queue()
            return TaskFSMState.READY
        self.__producer = {}
        for i in self.__outexchange_list:
            self.__producer[i] = MQProducer(self.__params, i)
            expire = 1000 * DATA_EXPIRE_SECONDS
            self.__producer[i].properties = pika.BasicProperties(expiration=str(expire))
        self.__outbuf = Queue()
        def out_task():
            while True:
                try:
                    tmp = self.__outbuf.get()
                    if len(self.__producer) == 0:
                        continue
                    if len(self.__producer) == 1:
                        list(self.__producer.values())[0].put_one(tmp)
                    else:
                        k, v = tmp
                        self.__producer[k].put_one(v)
                except Exception as e:
                    logger.error('Error while getting data from output buffer. {}'.format(str(e)))
        for key, val in six.iteritems(self.__producer):
            self.__producer[key].start()
        pyGo(out_task)
        return TaskFSMState.READY

    @state(TaskFSMState.READY, False)
    @protected_function(TaskFSMState.ERROR)
    def state_ready(self):
        res = None
        try:
            if len(self.__consumer) != 0:
                key, itm = self.__inbuf.get()
                res = self.__plugin.process(key, itm)
            else:
                res = self.__plugin.process(None, None)
        except Exception as e:
            logger.warning('Plugin exception {}.'.format(str(e)))
            return TaskFSMState.READY
        if res is not None:
            self.__outbuf.put(res)
        return TaskFSMState.READY
    
    @state(TaskFSMState.RETRY, False)
    @protected_function(TaskFSMState.ERROR)
    def state_retry(self):
        return TaskFSMState.READY
    
    @state(TaskFSMState.ERROR, False)
    def state_error(self):
        logger.error('Fatal error, terminating...')
        sys.exit(errno.EFAULT)


def parse_args():
    parser = argparse.ArgumentParser(prog=sys.argv[0],
        description='Task for loading a plugin with multiple input & output data processing.')
    parser.add_argument('-i', '--inexchange', dest='inexchange_list',
        action='append', default=[],
        help=('input message exchange names, you can specify multiple '
            'inputs by using this option multiple times.'))
    parser.add_argument('-o', '--outexchange', dest='outexchange_list',
        action='append', default=[],
        help=('output message exchange names, you can specify multiple '
            'outputs by using this option multiple times.'))
    parser.add_argument('-f', '--plugin-file', dest='file',
        required=True,
        help='Task plugin python file to load.')
    parser.add_argument('-a', '--plugin-args', dest='pluginargs',
        default=None,
        help='Task plugin initialization arguments.')

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
    if args.pluginargs is not None:
        tokens = shlex.shlex(args.pluginargs, posix=True, punctuation_chars=True)
        tokens.whitespace_split = True
        pluginargs = list(tokens)
    else:
        pluginargs = []

    plugin_name, plugin_class = load_plugin(args.file)
    credentials = pika.PlainCredentials(args.username, args.password)
    params = pika.ConnectionParameters(host=args.host,
        socket_timeout=5,
        credentials=credentials,
        client_properties={
            'connection_name': 'Task_{}'.format(plugin_name),
        })
    try:
        plugin_ins = plugin_class(*pluginargs)
        plugin_ins.keys_in = args.inexchange_list
        plugin_ins.keys_out = args.outexchange_list
        agent = Task(params,
            args.inexchange_list, args.outexchange_list, plugin_ins)
        while True:
            agent.run()
    except KeyboardInterrupt:
        logger.info('Terminating...')
    except pymongo.errors.ServerSelectionTimeoutError as e:
        logger.error('Failed to connect to MongoDB server. {}'.format(str(e)))
    except Exception:
        logger.error(traceback.format_exc())


# PYTHONPATH='./' python3 ./pyalgotrade/apps/task.py -i raw_xauusd -i raw_gc -o cooked_data -f ./plugins/empty.py
if __name__ == '__main__':
    main()
