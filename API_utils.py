import pandas as pd
import numpy as np
import requests
import hashlib
import os
import glob
import time
import dask.dataframe as dd

from dask import delayed
import dask
from dask.diagnostics import ProgressBar

# ------------------------------------------------------------------------------------
# functions related to downloading from the API


def fetch_from_API(site, productcode, data_path, daterange = 'most recent'):
    '''TODO: make a docstring for this, and move it to neon_utils when all done.
    TODO: put sensor positions in its own loop; for site, for product. just use one date so we don't have duplicates
    
    '''
    base_url = 'https://data.neonscience.org/api/v0/'

    # determine which dates are available for the site/product
    url = f'{base_url}sites/{site}'
    response = requests.get(url)
    data = response.json()['data']
    dates = set(data['dataProducts'][0]['availableMonths'])

    # fileter the available dates to get the ones desired
    if daterange == 'most recent':
        dates = [max(dates)]
        print(f'{dates} is the most recent dataset for {productcode} at {site}')
    elif daterange == 'all':
        print(f'{len(dates)} dates are available for {productcode} at {site}')
    else:
        try:
            # filter dates to be in daterange
            assert isinstance(daterange,list)
            daterange = [np.datetime64(d) for d in daterange]
            begin, terminate = min(daterange), max(daterange)
            dates = [d  for d in dates if (d >= begin) and (d <= terminate)] 
            print(f'{len(dates)} dates are available for {productcode} for {daterange[0]} to {daterange[-1]} at {site}')                
        except AssertionError:
            raise Exception('daterange must be a list, e.g. [\'2020-10\', \'2019-10\']')
            return(None)
        for date in dates:
            try:
                sensor_positions(product, site, date, data_path)
            except:
                pass
            dload(product, site, date, base_url, data_path)
        
              
def get_common_dates(site, products, base_url):        
    dates_list = []
    for product in products:
        #this part determines which dates are available for the site/product
        url = f'{base_url}sites/{site}'
        response = requests.get(url)
        data = response.json()['data']
        dates = set(data['dataProducts'][0]['availableMonths'])
        dates_list.append(dates)
    dates = list(set.intersection(*dates_list))
    return(dates)
        
       
def dload(product, site, date, base_url, data_path):                     
    url = f'{base_url}data/{product}/{site}/{date}'
    response = requests.get(url)
    data = response.json()
    files = data['data']['files']
    os.makedirs(data_path, exist_ok=True)
    for f in files:
        if ('expanded' in f['name']) & ('_1' in f['name']) & (f['name'].endswith('.csv')):                        
            attempts = 0 
            while attempts < 4:
                try:
                    # get the file 
                    handle = requests.get(f['url'])
                    #check the md5
                    md5 = hashlib.md5(handle.content).hexdigest()
                    if md5 == f['md5']:
                        success = True
                        attempts = 4
                    else:
                        print(f'md5 mismatch on attempt {attempts}')
                        success = False
                        attempts = attempts + 1
                except Exception as e:
                    print(f'Warning:\n{e}')
                    success = False
                    attempts = attempts + 1
            # write the file
            if success:
                fname = os.path.join(data_path, f['name'])
                with open(fname, 'wb') as sink:
                    sink.write(handle.content)


def sensor_positions(product, site, date, data_path):
    '''Downloads sensor position file from NEON API'''
    attempts = 0
    success = False
    while (attempts < 4) & (success == False):
        success = download_sensor_positions(product, site, date, data_path)
        attempts = attempts + 1
        

def download_sensor_positions(product, site, date, data_path):
    '''Downloads sensor position file from NEON API,
    used sensor_positions, which includes a some exception handling.'''
    # find the url and name of sensor_positions file
    path = data_path.rstrip('/')
    base_url = 'https://data.neonscience.org/api/v0/'
    url = f'{base_url}data/{product}/{site}/{date}'
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f'Bad {url} returns {response.statuscode}')
    name, url, md5 = find_sensor_positions_url(response)
    # download and save the sensor positions file 
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f'Bad url for {name}')
    # check the md5
    if md5 == hashlib.md5(response.content).hexdigest():
        fname = path + '/' + name
        with open(fname, 'wb') as sink:
            sink.write(response.content)
            return(True)
    else:
        return(False)
        
def find_sensor_positions_url(response):
    '''Find url for sensor_positions file from NEON API response'''
    data = response.json()['data']
    for f in data['files']:
        if 'sensor_positions' in f['name']:
            return(f['name'], f['url'], f['md5'])               
    raise Exception('No sensor_positions file!')
