PyAlgoTrade
===========

[![Build Status](https://travis-ci.org/gbeced/pyalgotrade.png?branch=master)](https://travis-ci.org/gbeced/pyalgotrade)
[![Coverage Status](https://coveralls.io/repos/gbeced/pyalgotrade/badge.svg?branch=master)](https://coveralls.io/r/gbeced/pyalgotrade?branch=master)


PyAlgoTrade is an **event driven algorithmic trading** Python library. Although the initial focus
was on **backtesting**, **paper trading** is now possible using:

 * [Bitstamp](https://www.bitstamp.net/) for Bitcoins

and **live trading** is now possible using:

 * [Bitstamp](https://www.bitstamp.net/) for Bitcoins

To get started with PyAlgoTrade take a look at the [tutorial](http://gbeced.github.io/pyalgotrade/docs/v0.20/html/tutorial.html) and the [full documentation](http://gbeced.github.io/pyalgotrade/docs/v0.20/html/index.html).

# Main Features

 * Event driven.
 * Supports Market, Limit, Stop and StopLimit orders.
 * Supports any type of time-series data in CSV format like Yahoo! Finance, Google Finance, Quandl and NinjaTrader.
 * Bitcoin trading support through [Bitstamp](https://www.bitstamp.net/).
 * Technical indicators and filters like SMA, WMA, EMA, RSI, Bollinger Bands, Hurst exponent and others.
 * Performance metrics like Sharpe ratio and drawdown analysis.
 * Handling Twitter events in realtime.
 * Event profiler.
 * TA-Lib integration.
 * New realtime processing support

# Installation

PyAlgoTrade is developed and tested using Python 2.7/3.7 and depends on:

 * [NumPy and SciPy](http://numpy.scipy.org/).
 * [pytz](http://pytz.sourceforge.net/).
 * [dateutil](https://dateutil.readthedocs.org/en/latest/).
 * [requests](http://docs.python-requests.org/en/latest/).
 * [matplotlib](http://matplotlib.sourceforge.net/) for plotting support.
 * [ws4py](https://github.com/Lawouach/WebSocket-for-Python) for Bitstamp support.
 * [tornado](http://www.tornadoweb.org/en/stable/) for Bitstamp support.
 * [tweepy](https://github.com/tweepy/tweepy) for Twitter support.
 * [pika](https://pypi.org/project/pika/) for RabbitMQ support
 * [coloredlogs](https://pypi.org/project/coloredlogs/) for Colored logging support.
 * [pymongo](https://api.mongodb.com/python/current/) for MongoDB support.
 
You can install PyAlgoTrade using pip like this:

```
pip install pyalgotrade
```

# Realtime Processing

Now realtime data processing is enabled. The strategy can accept multiple time frequencies so that some special
needs can be met. For example, I want to monitor minute data to guide my trade which is mainly based on daily
OHLC data. In this way, I can avoid significant loss when price made a `yuge' change during a day.

## Design


## Topology

The following graph shows how the components interconnect to each other.

Data Agent: download data and send data to message queue.
Data Processing: get data from message queue and produce data and send newly generated data to message queue.
Strategyd: this is the actual logic for realtime strategy processing.

```
                                                             
       +-------+         +------------+        +------------+
       |       |         |  RabbitMQ  |        |            |
       | Data  |         |            |        | Data       |
       | Agent --------------------------------> Processing |
       |       |         |            |        |            |
       +-------+         |            |        |            |
                         |         |------------            |
                         |         |  |        |            |
                         |         |  |   ----->            |
       +-------+         |      ---|-----/     |            |
       |       |     ----------/   |  |        |            |
       | Data  -----/    |         |  |        +------------+
       | Agent |         |         |  |                      
       |       |         |         |  |        +------------+
       +------------\    |         |  |        |            |
                     ----------\   |-----------> Strategyd  |
       +-------+         |      ---------\     |            |
       |       |         |            |   ----->            |
       | Data  |         |            |        |            |
       | Agent -------------------------------->            |
       |       |         |            |        |            |
       +-------+         +------------+        +------------+
```

## Execution

We need at least 3 components running

*  Strategyd: a process that runs your strategy implemented as a subclass of StrategyFSM.
*  Agent: fetch and/or generate data and send it to message queue for strategyd to consume.
*  PluginTasks: users can implement their own plugin tasks to process incoming data from rabbitmq and generated output data and send to rabbitmq.
*  RabbitMQ: message queue that receives and dispatch market data.


## Example
An example for running a strategyd and ibagent is:
```bash
# by default, we assume rabbitmq is running on localhost

# a task for fetching spot gold
python3 ./pyalgotrade/apps/fx678agent.py -s XAUUSD -o raw_xauusd

# a task for fetching futures gold
python3 ./pyalgotrade/apps/fx678agent.py -s @GC -o raw_gc

# a task for comparing the diff between spot gold and futures gold
python3 ./pyalgotrade/apps/plugintask.py -i raw_xauusd -i raw_gc -o cooked_diff -f ./plugins/gcdiff.py

# a task for generating different frequency data
python3 ./pyalgotrade/apps/timeseries.py -i raw_xauusd -o ts_xauusd -f hour -f day -r

# if you have mongodb running on localhost:
python3 ./pyalgotrade/apps/plugintask.py -i raw_xauusd -i raw_gc -f ./plugins/mongodbstore.py -a='-H localhost'
```


# TODO

I am planning to add support for TradeStation and other agents.