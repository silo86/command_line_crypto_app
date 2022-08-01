from logging import raiseExceptions
import click
import requests
import datetime
from LoggerConfig import LoggerConfig
import logging
import json


loggerConfig = LoggerConfig()
logger = loggerConfig.get_logger()

#actual date in ISO8601 format
actual_date = datetime.datetime.now().date().strftime('%d-%m-%Y')

@click.command()
@click.option('--date', default=actual_date, help='date you want to look for.')
#--persist
@click.option('--currency', prompt='insert the currency you want to look for',
              help='currency you want to look for.')
def fetch(currency,date):
    try:
        response = requests.get(f'https://api.coingecko.com/api/v3/coins/{currency}/history?date={date}')
        #print(response.json())
        response = response.json()
        filename = f'{currency}_{date}.json'
        with open(filename, 'w') as json_file:
            json.dump(response, json_file)

    except Exception as e:
        logger.info(e)
        #logging.info(e)


if __name__ == '__main__':
    fetch()



