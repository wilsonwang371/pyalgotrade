import argparse
import datetime as dt
import enum
import errno
import importlib.machinery as machinery
import importlib.util as util
import inspect
import os
import os.path
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, wait

import coloredlogs
import pyalgotrade.bar as bar
import pyalgotrade.logger
import pyalgotrade.strategy as strategy
from flask import Flask
from pyalgotrade.bar import Frequency
from pyalgotrade.barfeed.driver.ibdatadriver import IBDataDriver
from pyalgotrade.barfeed.livefeed import RabbitMQLiveBarFeed
from pyalgotrade.fsm import StateMachine, StrategyFSM, state
from pyalgotrade.mq import MQProducer
from pyalgotrade.utils.misc import protected_function, pyGo

coloredlogs.install(level='INFO')
logger = pyalgotrade.logger.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(prog=sys.argv[0],
        description='runner program for StrategyFSM class.')
    parser.add_argument('-f', '--strategyfsm-file', dest='file',
        required=True,
        help='a StrategyFSM python file to load')
    parser.add_argument('-s', '--symbol', dest='symbol',
        required=True,
        help='strategy resource symbol name')
    parser.add_argument('-i', '--inexchange', dest='inexchange',
        required=True,
        help='in message exchange name')
    parser.add_argument('-S', '--web-server', dest='server', action='store_true',
        help='start webserver for strategy state data monitoring')
    parser.add_argument('-p', '--port', dest='port', type=int, default=8000,
        help='web server port, default: 8000')
    
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


def start_webserver(strategy, serverport):
    app = Flask(__name__)

    @app.route('/')
    def strategy_state():
        return str(strategy.states)

    def server_task(app, serverport):
        try:
            app.run(port=serverport)
        except PermissionError:
            logger.error(traceback.format_exc())
            os._exit(errno.EPERM)
        except KeyboardInterrupt:
            logger.info('Terminating...')
            os._exit(0)

    task = threading.Thread(target=server_task, args=(app, serverport), daemon=True)
    task.start()


def load_strategyfsm(filename):
    if not os.path.isfile(filename):
        raise FileNotFoundError('no such file: ' + filename)
    loader = machinery.SourceFileLoader('StrategyFSM', filename)
    spec = util.spec_from_loader(loader.name, loader)
    mod = util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    candids = []
    for i in dir(mod):
        item = getattr(mod, i)
        if inspect.isclass(item) and StrategyFSM in item.__bases__:
            candids.append(i)
    if len(candids) > 1:
        raise ValueError('more than one StrategyFSM subclass. {}'.format(str(candids)))
    if len(candids) == 0:
        raise ValueError('no StrategyFSM subclass.')
    return (candids[0], getattr(mod, candids[0]))


def main():
    try:
        args = parse_args()
        strategyfsm_name, strategyfsm_class = load_strategyfsm(args.file)

        logger.info('instantiating livefeed class...')
        livefeed = RabbitMQLiveBarFeed(args.url, args.symbol, args.inexchange,
            [Frequency.REALTIME, Frequency.DAY])

        logger.info('creating strategy \'{}\'...'.format(strategyfsm_name))
        livestrategy = strategy.LiveStrategy(livefeed, strategyfsm_class)

        if args.server:
            logger.info('starting webserver...')
            start_webserver(livestrategy, args.port)

        logger.info('starting strategy...')
        livestrategy.run()
    except Exception:
        logger.error('{}'.format(traceback.format_exc()))
        sys.exit(errno.EFAULT)
    except KeyboardInterrupt:
        logger.info('terminating...')
        sys.exit(0)


# PYTHONPATH='./' python3 pyalgotrade/apps/strategyd.py -f ./samples/strategy/strategyfsm.py -s XAUUSD -i ts_xauusd -u "amqp://guest:guest@localhost/%2f"
if __name__ == '__main__':
    main()
