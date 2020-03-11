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

A realtime processing example topology. Here we use IB Agent. We can use other agents as well in the future.

```
+-------------+              +--------------+                   +-------------+
|             |              |              |                   |             |
|             |              |              |                   |             |
| Strategyd   |              |   IB  Agent  +------------------>+  IB Gateway |
|             |              |              |                   |             |
|             |              |              +<------------------+             |
|             |              |              |                   |             |
|             |              |              |                   |             |
+-------+-----+              +-------+------+                   +-------------+
        ^                            |
        |                            |
        |                            |
        |    +-----------------+     |
        |    |                 |     |
        |    |                 |     |
        |    |                 |     |
        +----+                 +<----+
             |  RabbitMQ       |
             |                 |
             |                 |
             |                 |
             +-----------------+
```

## Execution

We need at least 3 components running

*  Strategyd: a process that runs your strategy implemented as a subclass of StrategyFSM.
*  Agent: fetch and/or generate data and send it to message queue for strategyd to consume.
*  RabbitMQ: message queue that receives and dispatch market data.


## Example
An example for running a strategyd and ibagent is:
```bash
# to run ibagent
python3 pyalgotrade/apps/ibagent.py -s XAUUSD -u "amqp://guest:guest@localhost/%2f"

# to run a strategyd
python3 pyalgotrade/apps/strategyd.py -f ./samples/strategy/strategyfsm.py -s XAUUSD -u "amqp://guest:guest@localhost/%2f"
```


# TODO

I am planning to add support for TradeStation and other agents.