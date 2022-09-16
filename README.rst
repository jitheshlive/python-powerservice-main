========
Python Power Service
========

Package generated to interface with a trading system.

Package will return a random set of open trades for a given day, where each trade contains data for hours, trading volume, and an unique ID.

Note: The package will naively return series for a given date and not take into account for example if the date is in the future.
Further the numbers for the volume are random and do not have any relationship with previous or subsequent numbers as would normally be the case in real data.

Installation
============
Through a terminal navigate to the folder you have the powerservice folder and run

::

    pip install ./python-powerservice


Documentation
=============

The service will be part of the python environment and can be called in code
::

    from powerservice import trading

Example that will output some trades
::
    from powerservice import trading
    trades = trading.get_trades("29/06/2021")

    print(trades)



Developer:
===================

* The solution is provided in the intraday_report.py.

* Got different function:
    1. agg_values_per_hour() -> Aggregates the hourly trades including the trading for hour 23 last day
    2. data_profilling() -> Implemented a Statistical analysis for the field - volume
    3. quality_check() -> Implemented the count for all null fields for the volume and hour fields

* Used the setup.py to package the module:
    python setup.py build
    python setup.py install
 while pip install its not going through. So for the instan added the funcitons in the trading.py as well.

* Added the submit script in the folder - submit-scripts to run the spark jobs locally.


