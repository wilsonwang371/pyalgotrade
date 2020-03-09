from pyalgotrade import bar
from pyalgotrade.dataseries import bards
from pyalgotrade import feed
from pyalgotrade import dispatchprio
from pyalgotrade.barfeed import BaseBarFeed, MultiFrequencyBarFeed
import pyalgotrade.logger
import pytz
import datetime
import time
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String, DateTime, Float
from sqlalchemy.orm import sessionmaker
import os
import os.path


# This is only for backward compatibility since Frequency used to be defined here and not in bar.py.
Frequency = bar.Frequency

logger = pyalgotrade.logger.getLogger('historybarfeed')


class HistoryBarFeed(MultiFrequencyBarFeed):

    def __init__(self, instrument, frequencies, maxLen=1000,
                 start=datetime.datetime(1988, 1, 1), quote_client=None):
        if not isinstance(frequencies, list):
            frequencies = [frequencies]
        super(HistoryBarFeed, self).__init__(frequencies, maxLen=maxLen)

        # proactivly register our instrument just incase not found if
        # someone tries to call getDataSeries()
        for i in frequencies:
            self.registerDataSeries(instrument, i)

        self.__instrument = instrument
        self.__last_date = None
        self.__bars_buf = None
        self.__start_date = start

        self.__quote_client = quote_client

    def getCurrentDateTime(self):
        raise NotImplementedError()

    def barsHaveAdjClose(self):
        return False

    def getNextBars(self):
        bars = self.__getNextBars()
        return bars

    def __getNextBars(self):
        if self.__bars_buf is None:
            self.__bars_buf = []
            logger.info('Featching history of {0}'.format(self.__instrument))
            for i in self.getFrequencies():
                tmp = None
                if i == Frequency.DAY:
                    tmp = history(self.__instrument, self.__start_date, None,
                                Frequency.DAY, add_missing_dates=False)
                elif i == Frequency.HOUR:
                    tmp = history(self.__instrument, self.__start_date, None,
                                Frequency.HOUR, add_missing_dates=False)
                if tmp is None:
                    continue
                for date, row in tmp.iloc[0].iterrows():
                    tmpbar = bar.BasicBar(date, row['open'], row['high'],
                                        row['low'], row['close'], 0, False,
                                        i)
                    self.__bars_buf.append(tmpbar)
            self.__bars_buf.sort(key=lambda i: i.getDateTime())
        if len(self.__bars_buf) != 0:
            tmp = self.__bars_buf.pop(0)
            return bar.Bars({self.__instrument: tmp}, frequecy=tmp.getFrequency())
        return None

    def join(self):
        pass

    def eof(self):
        if self.__bars_buf is not None and len(self.__bars_buf) == 0:
            return True
        return False

    def peekDateTime(self):
        return None

    def start(self):
        super(HistoryBarFeed, self).start()

    def stop(self):
        pass


if __name__ == '__main__':
    from pyalgotrade import strategy
    from pyalgotrade.barfeed import quandlfeed
    import pyalgotrade.barfeed.historyfeed as fd
    from pyalgotrade.stratanalyzer import returns


    class MyStrategy(strategy.BacktestingStrategy):
        def __init__(self, feed, instrument):
            super(MyStrategy, self).__init__(feed)
            self.__instrument = instrument

        def onBars(self, bars):
            logger.info('New bar')
            bar = bars[self.__instrument]
            self.info(bar.getClose())

    # Load the bar feed from the CSV file
    feed = fd.HistoryBarFeed('spot_gold@fx678', bar.Frequency.DAY)

    # Evaluate the strategy with the feed's bars.
    myStrategy = MyStrategy(feed, "spot_gold@fx678")
    returnsAnalyzer = returns.Returns()
    myStrategy.attachAnalyzer(returnsAnalyzer)
    myStrategy.run()
    print(returnsAnalyzer.getReturns())
