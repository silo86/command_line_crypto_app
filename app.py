from logging import raiseExceptions
import click
import requests
from logger_config import LoggerConfig
import logging
import json
import sys
from util import Util
from upsert import Upsert
import datetime

loggerConfig = LoggerConfig()
logger = loggerConfig.get_logger()
util = Util()

#actual date in ISO8601 format
actual_date = datetime.datetime.now().date().strftime('%d-%m-%Y')

@click.command()
@click.option('--date', default=actual_date, help='date you want to look for.')
@click.option('--persist', is_flag=True, help='whether the json file will be inserted in postgres')
@click.option('--currency', prompt='insert the currency you want to look for',
              help='currency you want to look for.')
#@click.option('--fromdate_todate', '-daterange' ,help='insert the start_date following by the end_date')
def fetch(currency, date, persist):
    ''' Fetch currency data at a given date '''
    try:
        response, filename = util.get_response(currency,date)
        util.save_json(filename,response)

        if persist:
            upsert=Upsert()
            df = upsert.json_to_dataframe(filename)
            upsert.execute_query(df)
    except Exception as e:
        logger.info(e)
@click.command()
@click.option('--date_range', help='date_range in format dd-mm-yyyy_dd-mm-yyyy')
@click.option('--persist', is_flag=True, help='whether the json file will be inserted in postgres')
@click.option('--currency', prompt='insert the currency you want to look for',
              help='currency you want to look for.')
def fetchall(currency, date_range, persist=False):
    ''' Fetch currency data given a currency and a range of dates'''
    try:
        from_date, to_date = date_range.split('_')
        dates = util.get_date_range(from_date, to_date)
    except Exception as e:
        logger.info(e)
    for date in dates:
        try:
            response, filename = util.get_response(currency,date)
            util.save_json(filename,response)
            if persist:
                upsert=Upsert()
                df = upsert.json_to_dataframe(filename)
                upsert.execute_query(df)
        except Exception as e:
            logger.info(e)


if __name__ == '__main__':
    print(sys.argv)
    if '--date_range' in sys.argv:
        fetchall()
    else:
        fetch()



