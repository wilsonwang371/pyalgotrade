"""IBQuote Plugin
"""
import datetime as dt
import enum
import sys
import time

import pandas as pd
import six
from ib.opt import message

import ezibpy
import pyalgotrade.logger
from pyalgotrade.fsm import StateMachine, state
from pyalgotrade.utils.misc import protected_function, pyGo


logger = pyalgotrade.logger.getLogger(__name__)

IB_API_CLIENT_ID_DEFAULT = 624
IB_API_PORT_DEFAULT = 7497

READY_SLEEP_TIME = 30
NOTREADY_RETRY_TIME = 2
NOTREADY_RETRY_COUNT_MAX = 5
ERROR_SLEEP_TIME_DEFAULT = 8
ERROR_SLEEP_TIME_MAX = 512


class IBQuoteFSMState(enum.Enum):
    INIT = 1
    CONNECTING = 2
    CONNECTED = 3
    ERROR = 99


class IBDataDriver(StateMachine):

    def __init__(self, client=IB_API_CLIENT_ID_DEFAULT, port=IB_API_PORT_DEFAULT):
        super(IBDataDriver, self).__init__()
        self.__client_id = client
        self.__port = port
        self.__ibConn = None
        self.__contracts = {}
        self.__recover_time = ERROR_SLEEP_TIME_DEFAULT
        self.__connected = False

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

    @state(IBQuoteFSMState.INIT, True)
    def state_init(self):
        logger.debug('INIT')
        if self.__ibConn is not None:
            logger.info('disconnecting existing ib gateway connection')
            self.__ibConn.disconnect()
            del self.__ibConn
        self.__ibConn = None
        self.__quote_data = {}
        self.__tmp_historical_df = {}
        self.__historical_data = {}
        return IBQuoteFSMState.CONNECTING


    @state(IBQuoteFSMState.CONNECTING, False)
    @protected_function(IBQuoteFSMState.ERROR)
    def state_connecting(self):
        assert self.__ibConn is None
        logger.debug('CONNECTING')
        self.__ibConn = ezibpy.ezIBpy()
        self.__ibConn.ibCallback = self.ibCallback
        self.__ibConn.connect(clientId=self.__client_id,
                              host='localhost',
                              port=self.__port)
        self.__quote_data = {}
        self.__ready_next_state = None
        return IBQuoteFSMState.CONNECTED


    @state(IBQuoteFSMState.CONNECTED, False)
    @protected_function(IBQuoteFSMState.ERROR)
    def state_connected(self):
        logger.debug('CONNECTED')
        self.__connected = True
        if (len(self.__contracts.keys()) != 0 and
            len(self.__ibConn.contracts.keys()) == 0):
            logger.info('recovering saved contracts')
            old_contract = self.__contracts
            self.__contracts = {}
            for _, v in six.iteritems(old_contract):
                newk = self.__create_contract_internal(v)
                logger.info('reqId: {} contract: {}'.format(newk, v))
                self.__contracts[newk] = v
            del old_contract
        if self.__ready_next_state is not None:
            tmp = self.__ready_next_state
            self.__ready_next_state = None
            return tmp
        if self.__ibConn.connected:
            # reset recover time
            self.__recover_time = ERROR_SLEEP_TIME_DEFAULT
            time.sleep(READY_SLEEP_TIME)
            return IBQuoteFSMState.CONNECTED
        count = 0
        while (count < NOTREADY_RETRY_COUNT_MAX and
            self.__ibConn.connected == False):
            logger.error('IB Connection failed, retrying in %d seconds... (%d/%d)' %
                (NOTREADY_RETRY_TIME, count + 1, NOTREADY_RETRY_COUNT_MAX))
            time.sleep(NOTREADY_RETRY_TIME)
            count += 1
        if self.__ibConn.connected:
            logger.error('IB Connection re-established.')
            return IBQuoteFSMState.CONNECTED
        return IBQuoteFSMState.ERROR


    @state(IBQuoteFSMState.ERROR, False)
    def state_error(self):
        logger.debug('ERROR')
        logger.error('Error while connecting to server, resetting in %d seconds...' % self.__recover_time)
        self.__connected = False
        time.sleep(self.__recover_time)
        self.__recover_time = min(self.__recover_time * 2, ERROR_SLEEP_TIME_MAX)
        return IBQuoteFSMState.INIT


    @protected_function(None)
    def handleConnectionOpened(self, caller, msg, **kwargs):
        pass


    def handleConnectionClosed(self, caller, msg, **kwargs):
        pass


    def handleAccount(self, caller, msg, **kwargs):
        logger.debug('account information: {}'.format(msg))
        pass


    def handleOrders(self, caller, msg, **kwargs):
        logger.info('orders information: {}'.format(msg))


    def handleTickSize(self, caller, msg, **kwargs):
        pass


    def handleHistoricalData(self, caller, msg, **kwargs):
        if isinstance(msg, message.historicalData):
            reqId = msg.reqId
            name = self.__ibConn.tickerSymbol(reqId)
            if reqId not in self.__tmp_historical_df:
                df = pd.DataFrame(columns=['open',
                                           'high',
                                           'low',
                                           'close',
                                           'volume',
                                           'source'])
                df.index.name = 'timestamp'
                self.__tmp_historical_df[reqId] = df
            if msg.date[:8].lower() != 'finished':
                series = pd.Series([msg.open, msg.high, msg.low,
                    msg.close, msg.volume, 'ib'],
                    ['open', 'high', 'low',
                    'close', 'volume', 'source'])

                self.__tmp_historical_df[reqId].loc[msg.date] = series
                #logger.info('added {} date {}'.format(series, msg.date))
            else:
                histdf = self.__tmp_historical_df[reqId]
                histdf.iloc[:, :] = histdf.iloc[:, :].astype(float)
                histdf.iloc[:, 4] = histdf.iloc[:, 4].astype(int)
                histdf.index = pd.to_datetime(histdf.index, format='%Y%m%d',
                    utc=True)
                self.__tmp_historical_df.pop(reqId, None)
                self.__historical_data[reqId] = {'name': name, 'value': histdf}
                logger.info('historical data downloaded. id: {} name: {}'.format(reqId,
                    self.__historical_data[reqId]['name']))
                

    def handleError(self, caller, msg, **kwargs):
        if msg and hasattr(msg, 'errorCode'):
            errorCode = msg.errorCode
            if errorCode in [502, 504]:
                logger.info('disconnected from server')
        else:
            logger.error('ERROR not processed[{}]: {}'.format(caller, msg))


    def handleTickPrice(self, caller, msg, **kwargs):
        if isinstance(msg, message.tickPrice):
            tickerId = msg.tickerId
            name = self.__ibConn.tickerSymbol(tickerId)
            value = msg.price
            self.__quote_data[tickerId] = {
                'name': name,
                'time': dt.datetime.timestamp(dt.datetime.utcnow()),
                'price': value,
            }
            #logger.debug('updating {}: {}'.format(name, value))


    @protected_function(None)
    def ibCallback(self, caller, msg, **kwargs):
        """callback function for ezibpy
        
        Arguments:
            caller {[type]} -- [description]
            msg {ib.opt.message} -- message class
        """
        if caller and hasattr(self, caller) and callable(getattr(self, caller)):
            handler = getattr(self, caller)
            handler(caller, msg, **kwargs)
        else:
            logger.info('not able to handle event: {}'.format(caller))

    @protected_function(False)
    def has_contract(self, contract_tuple):
        for _, ctuple in six.iteritems(self.__contracts):
            if ctuple == contract_tuple:
                return True
        return False

    @property
    def connected(self):
        return self.__connected

    @protected_function(None)
    def create_contract(self, contract_tuple):
        if not self.__ibConn or not self.__connected:
            logger.error('ib server not connected')
            return None
        for tid, ctuple in six.iteritems(self.__contracts):
            if ctuple == contract_tuple:
                logger.info('trying to create duplicate contract'
                    ' {}'.format(contract_tuple))
                return tid
        tickId = self.__create_contract_internal(contract_tuple)
        self.__contracts[tickId] = contract_tuple
        return tickId


    def __create_contract_internal(self, contract_tuple):
        """create contract for data reqeusting
        
        Arguments:
            contract_tuple {tuple} -- a tuple for contract detail
        
        Returns:
            [type] -- [description]
        """
        if not self.__ibConn:
            logger.error('ib server not connected')
            return None
        contract_tuple = tuple(contract_tuple)
        if len(contract_tuple) != 7:
            logger.error('invalid data format')
            return None
        logger.debug('request tuple {}'.format(contract_tuple))
        if contract_tuple[0][0] == '@':
            newcontract = self.__ibConn.createFuturesContract(contract_tuple)
            return self.__ibConn.tickerId(self.__ibConn.contractString(newcontract))
        newcontract = self.__ibConn.createContract(contract_tuple)
        return self.__ibConn.tickerId(self.__ibConn.contractString(newcontract))


    @protected_function((None, None))
    def __ticker_or_symbol(self, value):
        if isinstance(value, str):
            try:
                tickerId = int(value)
                return (tickerId, None)
            except:
                return (None, value)
        elif isinstance(value, int):
            return (value, None)
        else:
            logger.info('no ticker and/or symbol found!')
            return (None, None)


    def __symbol_to_tickerId(self, symbol):
        candid = []
        for k, v in six.iteritems(self.__contracts):
            if symbol.find(v[0]) != -1:
                candid.append(k)
        if len(candid) == 1:
            return candid[0]
        return None


    @protected_function(None)
    def get_symbol_ticker_id(self, symbol):
        return self.__symbol_to_tickerId(symbol)


    # historical data related rpc calls

    @protected_function(False)
    def request_historical_data(self, value, resolution, lookback, data='TRADE'):
        if not self.__ibConn or not self.__connected:
            logger.error('ib server not connected')
            return False
        tickerId, symbol = self.__ticker_or_symbol(value)
        if tickerId is not None:
            logger.info('start requesting historical data for tikerId: {}'.format(tickerId))
            self.__ibConn.requestHistoricalData(contracts=self.__ibConn.contracts[tickerId],
                                                resolution=resolution,
                                                lookback=lookback,
                                                data=data)
            return True
        elif symbol is not None:
            tickerId = self.__symbol_to_tickerId(symbol)
            if tickerId is not None:
                if tickerId not in self.__ibConn.contracts.keys():
                    logger.error('tickerId {} not in contracts {}'.format(tickerId,
                        list(self.__ibConn.contracts.keys())))
                    return False
                self.__ibConn.requestHistoricalData(contracts=self.__ibConn.contracts[tickerId],
                                                    resolution=resolution,
                                                    lookback=lookback,
                                                    data=data)
                return True
        return False


    @protected_function(False)
    def cancel_historical_data(self, value):
        if not self.__ibConn:
            logger.error('ib server not connected')
            return False
        tickerId, symbol = self.__ticker_or_symbol(value)
        if tickerId is not None:
            logger.info('stop requesting historical data for {}'.format(tickerId))
            self.__ibConn.cancelHistoricalData(self.__ibConn.contracts[tickerId])
            return True
        elif symbol is not None:
            tickerId = self.__symbol_to_tickerId(symbol)
            if tickerId is not None:
                if tickerId not in self.__ibConn.contracts.keys():
                    logger.error('tickerId {} not in contracts {}'.format(tickerId,
                        list(self.__ibConn.contracts.keys())))
                    return False
                self.__ibConn.cancelHistoricalData(self.__ibConn.contracts[tickerId])
                return True
        return False


    # market data related rpc calls

    @protected_function(False)
    def request_market_data(self, value):
        """start collecting market data with added contracts
        
        Arguments:
            value {str or int} -- either symbol or ticketId
        
        Returns:
            bool -- success or failure
        """
        if not self.__ibConn:
            logger.error('ib server not connected')
            return False
        tickerId, symbol = self.__ticker_or_symbol(value)
        if tickerId is not None:
            logger.info('start requesting market data for tickerId: {}'.format(tickerId))
            self.__ibConn.requestMarketData(self.__ibConn.contracts[tickerId])
            return True
        elif symbol is not None:
            tickerId = self.__symbol_to_tickerId(symbol)
            if tickerId is not None:
                if tickerId not in self.__ibConn.contracts.keys():
                    logger.error('tickerId {} not in contracts {}'.format(tickerId,
                        list(self.__ibConn.contracts.keys())))
                    return False
                self.__ibConn.requestMarketData(self.__ibConn.contracts[tickerId])
                return True
        return False


    @protected_function(False)
    def cancel_market_data(self, value):
        """stop requesting market data from added contracts
        
        Arguments:
            value {str or int} -- either symbol or ticketId
        
        Returns:
            bool -- success or failure
        """
        if not self.__ibConn:
            logger.error('ib server not connected')
            return False
        tickerId, symbol = self.__ticker_or_symbol(value)
        if tickerId is not None:
            logger.info('stop requesting market data for {}'.format(tickerId))
            self.__ibConn.cancelMarketData(self.__ibConn.contracts[tickerId])
            return True
        elif symbol is not None:
            tickerId = self.__symbol_to_tickerId(symbol)
            if tickerId is not None:
                if tickerId not in self.__ibConn.contracts.keys():
                    logger.error('tickerId {} not in contracts {}'.format(tickerId,
                        list(self.__ibConn.contracts.keys())))
                    return False
                self.__ibConn.cancelMarketData(self.__ibConn.contracts[tickerId])
                return True
        return False


    @protected_function(None)
    def quote(self, value):
        """get a price quote for a symbol or tickerId
        
        Arguments:
            value {str or int} -- either symbol or tickerId
        
        Returns:
            tuple -- (tickerId, symbol), if not any, the value will be None
        """
        tickerId, symbol = self.__ticker_or_symbol(value)
        if tickerId != None:
            if tickerId in self.__quote_data:
                return self.__quote_data[tickerId]
        elif symbol is not None:
            for _, v in six.iteritems(self.__quote_data):
                if isinstance(v, dict) and 'name' in v:
                    if v['name'] == symbol:
                        return v
        return None


    @protected_function(None)
    def history(self, value):
        tickerId, symbol = self.__ticker_or_symbol(value)
        logger.info('current historical data {}'.format(self.__historical_data.keys()))
        if tickerId != None:
            if tickerId in self.__historical_data:
                return self.__historical_data[tickerId]['value']
        elif symbol is not None:
            for _, v in six.iteritems(self.__historical_data):
                if isinstance(v, dict) and 'name' in  v:
                    if v['name'] == symbol:
                        return v['value']
        return None


    @protected_function([])
    def contracts(self):
        """ get current contracts
        
        Returns:
            dict -- tickerId is the key, contract tuple is the value
        """
        return self.__contracts


    def reset(self):
        logger.info('reseting ibdatadriver instance')
        self.__ready_next_state = IBQuoteFSMState.INIT


    def set_config(self, config_dict):
        if isinstance(config_dict, dict):
            if 'port' in config_dict:
                logger.info('setting ibdatadriver instance '
                    'port to {}'.format(config_dict['port']))
                self.__port = config_dict['port']
            if 'client_id' in config_dict:
                logger.info('setting ibdatadriver instance '
                    'client_id to {}'.format(config_dict['client_id']))
                self.__client_id = config_dict['client_id']


if __name__ == '__main__':
    a = IBDataDriver()
    a.start()
    while True:
        time.sleep(2)