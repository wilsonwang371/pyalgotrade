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

### Key components

*  Task: a task can load a plugin to process incoming data and/or generate output data.
*  Strategyd: read incoming data and send it to a live strategy state machine.


```
                                                             
       +-------+         +------------+        +------------+
       |       |         |  RabbitMQ  |        |            |
       | Data  |         |            |        | Data       |
       | Agent --------------------------------> Processing |
       | Task  |         |            |        | Task       |
       +-------+         |            |        |            |
                         |         |------------            |
                         |         |  |        |            |
                         |         |  |   ----->            |
       +-------+         |      ---|-----/     |            |
       |       |     ----------/   |  |        |            |
       | Data  -----/    |         |  |        +------------+
       | Agent |         |         |  |                      
       | Task  |         |         |  |        +------------+
       +------------\    |         |  |        |            |
                     ----------\   |-----------> Strategyd  |
       +-------+         |      ---------\     |            |
       |       |         |            |   ----->            |
       | Data  |         |            |        |            |
       | Agent -------------------------------->            |
       | Task  |         |            |        |            |
       +-------+         +------------+        +------------+
```

## Execution

TODO


## Example

An example setup

```bash
# by default, we assume rabbitmq is running on localhost

# a task for fetching spot gold
python3 ./pyalgotrade/apps/task.py -o raw_xauusd -f ./plugins/fx678agent.py -a='-s XAUUSD'

# a task for fetching futures gold
python3 ./pyalgotrade/apps/task.py -o raw_gc -f ./plugins/fx678agent.py -a='-s @GC'

# a task for comparing the diff between spot gold and futures gold
python3 ./pyalgotrade/apps/task.py -i raw_xauusd -i raw_gc -o cooked_diff -f ./plugins/gcdiff.py

# a task for generating different frequency data
python3 ./pyalgotrade/apps/timeseries.py -i raw_xauusd -o ts_xauusd -f hour -f day -r

# if you have mongodb running on localhost
# run a plugin task to save all spot gold and futures gold data to mongodb
python3 ./pyalgotrade/apps/task.py -i raw_xauusd -i raw_gc -f ./plugins/mongodbstore.py -a='-H localhost'
```


# TODO

I am planning to add support for TradeStation and other agents.