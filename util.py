import datetime
from datetime import timedelta
import requests
import os
import json


class Util:

    def get_date_list(self, start, end):
        ''' Given a start date and end date obtains the list of dates between those
        
        Args:
            start (datetime): the initial date
            end (datetime): the final date
        Returns:
            days (list): list of dates between start and end dates

        '''
        
        delta = end - start  # as timedelta
        days = [start + timedelta(days=i) for i in range(delta.days + 1)]
        return days

    def get_date_range(self, start:str, end:str) -> list:
        ''' Given a start date and end date obtains the list of dates between those
        
        Args:
            start (str): the initial date
            end (str): the final date
        Returns:
            dates (list): list of dates between start and end dates

        '''

        datelist = [start, end]
        datelist_ = []
        for date in datelist:
            splitted_date = date.split('-')
            date = '-'.join([i for i in splitted_date[::-1]])
            date = datetime.datetime.fromisoformat(date)
            datelist_.append(date)
        dates = self.get_date_list(datelist_[0],datelist_[1])
        dates = [date.strftime('%d-%m-%Y') for date in dates]
        return dates
    def get_response(self, currency:str, date:str):
        ''' Given a currency and a date get the response object from the coingecko API
        
        Args:
            currency (str): the initial date
            date (str): the final date
        Returns:
            response (json): json object with the response
            filename (str):  the name of the file

        '''

        response = requests.get(f'https://api.coingecko.com/api/v3/coins/{currency}/history?date={date}')
        response = response.json()
        filename = f'{currency}_{date}.json'
        return response,filename
    def save_json(self, filename, response):
        ''' Given the filename and the response object save the response as a json file
        
        Args:
            filename (str): the name of the file
            response (json object): the json object with the API response

        '''
        
        cwd = os.getcwd()
        os.chdir(cwd + '/json_files')
        with open(filename, 'w') as json_file:
            json.dump(response, json_file)
        os.chdir('..')
    def load_json(self,filename):
        ''' Given a filename load the object into memory
        
        Args:
            filename (str): the name of the file
        Returns:
            d (json): json object

        '''

        cwd = os.getcwd()
        os.chdir(cwd + '/json_files')
        with open(filename) as f:
            d = json.load(f)
        os.chdir('..')
        return d

