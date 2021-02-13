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

def show_dates(site, productcode):
    '''returns available dates for site and product'''
    
    base_url = 'https://data.neonscience.org/api/v0/'

    # determine which dates are available for the site/product
    url = f'{base_url}sites/{site}'
    response = requests.get(url)
    data = response.json()['data']
    dates = list(set(data['dataProducts'][0]['availableMonths']))
    dates.sort()
    return(dates)

def show_files_for_site_date(product, site, date):
    '''returns list of files available for the site and date'''
    base_url = 'https://data.neonscience.org/api/v0/'
    url = f'{base_url}data/{product}/{site}/{date}'
    response = requests.get(url)
    data = response.json()
    files = data['data']['files']
    return(files)


def generate_laz_download_info(productcode, site, date):
    '''Returns: time of url issueance, list of laz files'''
    t0 = time.time()
    files = show_files_for_site_date(productcode, site, date)
    laz = []
    for file in files:
        if 'classified_point_cloud_colorized.laz' in file['name']:
            laz.append(file)
    return(t0, laz)
    
    
def refresh_url(f, t0, productcode, site, date):
    '''If too much time has elapsed since url issued, modifies f to contain new url'''
    if time.time() - t0 < 3550:
        pass
    else:
        files = show_files_for_site_date(productcode, site, date)
        for file in files:
            if file['name'] == f['name']:
                f['url'] = file['url']
                
     
def download_from_NEON_API(f, data_path):
    '''Download file described by f to datapath.
       f         -- dict - This dict is suplied by generate_laz_download_info, see above.
       data_path -- Str  - path where data will be saved.
    '''
    attempts = 0 
    while attempts < 4:
        try:
            # get the file 
            handle = requests.get(f['url'])
            
            #check the md5 if it exists
            if f['md5']:
                md5 = hashlib.md5(handle.content).hexdigest()
                if md5 == f['md5']:
                    success = True
                    attempts = 4
                else:
                    fmd5 = f['md5']
                    print(f'md5 mismatch on attempt {attempts}')
                    success = False
                    attempts = attempts + 1
            else: 
                success = True
                attempts = 4
        except Exception as e:
            print(f'Warning:\n{e}')
            success = False
            attempts = attempts + 1
    # write the file
    if success:
        fname = os.path.join(data_path, f['name'])
        with open(fname, 'wb') as sink:
            sink.write(handle.content)
    else:
        raise Exception('failed to download')
        
