#%%
from API_utils import *
from dask import delayed, compute
from dask.diagnostics import ProgressBar
import os
import subprocess
import multiprocessing
import json
ncores = multiprocessing.cpu_count()
ncores

#%%
site = 'TEAK'
productcode = 'DP1.30003.001'
data_path = '/media/data/AOP'
date = '2018-06'
os.makedirs(data_path, exist_ok=True)

# %%
'''
t0, laz = generate_laz_download_info(productcode, site, date)

results = []
for f in laz:
    results.append(delayed(download_from_NEON_API)(f, data_path))
    
with ProgressBar():
    computed = compute(*results)
'''
# %%
config = {'input'  : data_path, 
          'output' : os.path.join(data_path, 'NEON_D17_TEAK_DP1_mosaic_classified_point_cloud_colorized.laz'),
          'srs'    : 'EPSG:26911',
          'threads': ncores}

with open('config.json', 'w') as dst:
    json.dump(config, dst)

cmd = 'entwine build -c config.json'

result = subprocess.run(cmd, shell=True, capture_output=True)

os.remove('config.json')
# %%
cmd
# %%
