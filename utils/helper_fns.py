from datetime import date
import datetime
import os
import re
import shutil
import sys

import geopandas as gpd
import glob
import numpy as np
import pandas as pd
import pathlib
import rasterio as rio
import rioxarray as rx
import s3fs
from tqdm import tqdm
import xarray as xr
import zarr
import dask 


def string_to_date(x: str):
    '''
    Converts 'YYYYMM' string to date
    '''
    
    x = str(x)
    
    return date(int(x[0:4]), int(x[4:6]), 1)

def string_to_date2(x):
    '''
    Converts 'YYYY-Mon'string to (month, year)
    '''

    x = str(x)

    try:
        year = re.search(r'\d{4}', x).group()
        mon = re.search(r'[a-zA-Z]{3}', x).group()
    except:
        print('ERROR: StartDate format incorrect in asset_date csv')
        sys.exit()

    return (datetime.datetime.strptime(mon, '%b').month, int(year))

def date_to_string(dt):

    if (dt.day == 10):
        return (str(dt.year) + str(dt.month) + 'd1')
    elif (dt.day == 20):
        return (str(dt.year) + str(dt.month) + 'd2')
    else:
        return (str(dt.year) + str(dt.month) + 'd3')



def read_asset_csv(path_to_csv):
    '''
    Reads asset_date.csv
    '''  

    asset_csv = pd.read_csv(path_to_csv, usecols = ['Asset_id', 'StartDate', 'EndDate'])
    asset_csv = asset_csv.dropna()
    asset_csv['start'] = [string_to_date2(x) for x in asset_csv['StartDate']]
    asset_csv['end'] = [string_to_date2(x) for x in asset_csv['EndDate']]
    asset_csv.drop(['StartDate', 'EndDate'], axis = 1, inplace = True)
    
    asset_csv.drop_duplicates(inplace = True)
    asset_csv.set_index('Asset_id', inplace = True)

    return asset_csv


def get_wet_dry(c_info: pd.DataFrame):
    '''
    Gets start and end of wet and dry season(s) from c_info 
    '''  
    
    # Get start and end of wet season(s)
    w = [(c_info['ws1_start'], c_info['ws1_end']), (c_info['ws2_start'], c_info['ws2_end'])]
    wet_season = [(int(x[0]), int(x[1])) for x in w if ~np.isnan(x[0])]
    
    # Get start and end of dry season
    dry_season = [(c_info['ds1_start'], c_info['ds1_end'])]
        
    return (wet_season, dry_season)


def delete_directory(folder: str):

    if os.path.isdir(folder):

        for filename in os.listdir(folder):
            file_path = os.path.join(folder, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except:
                pass


def read_ODC(url: str):

    fs = s3fs.S3FileSystem(anon=True, client_kwargs={'region_name': 'eu-central-1'})
    
    da = xr.open_zarr(s3fs.S3Map(url, s3=fs))
    da = da.assign_coords(time = pd.to_datetime([string_to_date(x) for x in da.time.values]))
    da = da.band

    return da


def ODC_to_disk(url: str, output_name: str, path_adm00: str):

    fs = s3fs.S3FileSystem(anon=True, client_kwargs={'region_name': 'eu-central-1'})
    
    da = xr.open_zarr(s3fs.S3Map(url, s3=fs))
    da = da.assign_coords(time = pd.to_datetime([string_to_date(x) for x in da.time.values]))

    da = crop_country(da, url, path_adm00)
    
    da.to_zarr(output_name, mode = 'w')
    

def check_asset_size(da, gdf):
    '''
    Check if the asset is big enough to be processed
    '''
    
    sufficient = True
    
    try:
        da.rio.clip(gdf.geometry.values, gdf.crs)
    except:
        sufficient = False
    
    return sufficient


def get_iso3(x):

    c = re.search(r'LIA/...', x).group()
    iso3 = c[-3:]

    return iso3


def crop_country(da, url, path_adm00):

    daC = da.copy()

    #daC = daC.load()

    iso3 = get_iso3(url)

    adm0 = gpd.read_file(path_adm00)
    adm0.set_index('iso3', inplace = True)

    if iso3 not in adm0.index:
        print('ERROR in iso3 country in URL')
        sys.exit()

    if adm0.loc[iso3]['geometry'].geom_type == 'MultiPolygon':
        daC = daC.rio.clip(adm0.loc[iso3]['geometry'])
    elif adm0.loc[iso3]['geometry'].geom_type == 'Polygon':
        daC = daC.rio.clip([adm0.loc[iso3]['geometry']])
    else:
        print('ERROR in Country Shapefile')
        sys.exit()

    return daC

