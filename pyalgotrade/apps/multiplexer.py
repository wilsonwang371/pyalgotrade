import argparse
import datetime as dt
import enum
import errno
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, wait

import six
from six.moves.queue import Queue

import coloredlogs
import pika
import pyalgotrade.bar as bar
import pyalgotrade.logger
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


class Multiplexer(StateMachine):

    def __init__(self, params, inexchange_list, outexchange):
        super(Multiplexer, self).__init__()
        assert len(inexchange_list) != 0
        self.__params = params
        self.__inexchange_list = inexchange_list
        self.__outexchange = outexchange

    @state(MultiplexerFSMState.INIT, True)
    @protected_function(MultiplexerFSMState.ERROR)
    def state_init(self):
        self.__inbuf = {}
        self.__consumer = {}
        self.last_values = {}
        for i in self.__inexchange_list:
            self.__consumer[i] = MQConsumer(self.__params, i,
                queue_name='{}_MultiplexerQueue'.format(i.upper()))
            self.__inbuf[i] = Queue()
            self.last_values[i] = None
        self.__producer = MQProducer(self.__params, self.__outexchange)
        expire = 1000 * DATA_EXPIRE_SECONDS
        self.__producer.properties = pika.BasicProperties(expiration=str(expire))
        self.__outbuf = Queue()
        def out_task():
            while True:
                tmp = self.__outbuf.get()
                self.__producer.put_one(tmp)
        for key, val in six.iteritems(self.__consumer):
            val.start()
            def in_task():
                while True:
                    tmp = val.fetch_one()
                    self.__inbuf[key].put(tmp)
            pyGo(in_task)
        self.__producer.start()
        pyGo(out_task)


        #TODO: remove this later
        def tmptask():
            while True:
                time.sleep(60)
                for k, v in six.iteritems(self.last_values):
                    logger.info('{}: {}'.format(k, v))
        pyGo(tmptask)
        return MultiplexerFSMState.READY

    @state(MultiplexerFSMState.READY, False)
    @protected_function(MultiplexerFSMState.ERROR)
    def state_ready(self):
        for k, v in six.iteritems(self.__inbuf):
            while not v.empty():
                self.last_values[k] = v.get()
        #TODO: implement this later
        #self.__outbuf.put(itm)
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

    credentials = pika.PlainCredentials(args.username, args.password)
    params = pika.ConnectionParameters(host=args.host,
        socket_timeout=5,
        credentials=credentials)
    agent = Multiplexer(params,
        args.inexchange_list, args.outexchange)
    try:
        while True:
            agent.run()
    except KeyboardInterrupt:
        logger.info('Terminating...')
    except Exception:
        logger.error(traceback.format_exc())


# PYTHONPATH='./' python3 ./pyalgotrade/apps/multiplexer.py -i raw_xauusd -i raw_gc -o multiplexer_out
if __name__ == '__main__':
    main()
