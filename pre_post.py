# pre/post comparison asset area vs surrounding 

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
from helper_fns import check_asset_size 

import warnings
warnings.filterwarnings("ignore")



def get_pre_dates(start_intervention, end_intervention, season):
    
    if start_intervention[0] < season[1]:
        end = (season[1], start_intervention[1] - 1)
    else:
        end = (season[1], start_intervention[1])
        
    if season[0] < season[1]:
        start = (season[0], end[1])
    else:
        start = (season[0], end[1] - 1)
        
    return [start, end]


def get_post_dates(start_intervention, end_intervention, season):
    
    if start_intervention[1] > season[0]:
        start = (season[0], end_intervention[1] + 1)
    else:
        start = (season[0], end_intervention[1])
        
    if season[0] < season[1]:
        end = (season[1], start[1])
    else:
        end = (season[1], start[1] + 1)
        
    return [start, end]


def get_pre_post_dates(start_intervention, end_intervention, wet_season, dry_season):
    
    pre_wet, post_wet = [], []
    pre_dry, post_dry = [], []
    
    for ws in wet_season:
        pre_wet.append(get_pre_dates(start_intervention, end_intervention, ws))
        post_wet.append(get_post_dates(start_intervention, end_intervention, ws))
    
    for ds in dry_season:
        pre_dry.append(get_pre_dates(start_intervention, end_intervention, ds))
        post_dry.append(get_post_dates(start_intervention, end_intervention, ds))
        
    return(pre_wet, post_wet, pre_dry, post_dry)


def unscaling(da, product_type: str):

	if product_type == 'NDVI':
		da = da.mean(['time'])
		da = da / 10000
	elif product_type == 'maxNDVI':
		da = da.max(['time'])
		da = da / 10000
	elif product_type == 'LST':
		da = da.max(['time'])
		da = da * 0.02 - 273.15
	else:
		print('ERROR: incorrect product_type in prepost')
		sys.exit()

	return da


def run(da, shapefiles: list, wet_season: list, dry_season: list, asset_info: pd.DataFrame, path_output: str, product_type: str):
    '''Possible product_types: NDVI, maxNDVI or LST'''
    
	# Create output folder
    folder_name = path_output + '/prepost'
    pathlib.Path(folder_name).mkdir(parents=True, exist_ok=True)

    unprocessed = []

    for i,shapefile in enumerate(shapefiles):
	    
		# Get asset ID
        ID = os.path.basename(shapefile)[:-4]
        print('-- Processing asset ' + ID + ' (' + str(i + 1) + '/' + str(len(shapefiles)) + ') --')

		# Reading asset
        gdf = gpd.read_file(shapefile)

		# 0.2 degree buffer around asset
        gdf_buf = gdf.buffer(0.2, cap_style = 3)
        
        # Check asset size        
        if not check_asset_size(da, gdf):
            print('The asset is too small to be processed')
            unprocessed.append([ID, 'Asset too small', 'N/A'])
            continue

        # Clip rasters
        da_clipped = da.rio.clip(gdf_buf.geometry.values, gdf_buf.crs)

		# Load da values
        da_clipped.load()

		# Get intervention dates
        start_intervention = asset_info.loc[ID].start
        end_intervention = asset_info.loc[ID].end

		# Get comparison dates
        (pre_wet, post_wet, pre_dry, post_dry) = get_pre_post_dates(start_intervention, end_intervention, wet_season, dry_season)

        t = da_clipped.time.values
        j = 1

        for prew, postw in zip(pre_wet, post_wet):

			# Check data is missing to process the asset
            if pd.to_datetime(date(postw[1][1], postw[1][0], 1)) > t[-1]:
                unprocessed.append([ID, 'No data post intervention', postw])
                if j==1:
                    print('There is no wet season post intervention') 
                else:
                    print('There is no second wet season post intervention')

            else:

                pre = da_clipped.sel(time = da_clipped.time[(t >= pd.to_datetime(date(prew[0][1], prew[0][0], 1))) & (t <= pd.to_datetime(date(prew[1][1], prew[1][0], 1)))])
                pre = unscaling(pre, product_type)
                name = ID + '_L_' + product_type + '_' + str(prew[0][1]) + '_wet' + str(j) + '.tif'
                pre.rio.to_raster(folder_name + '/' + name)

                post = da_clipped.sel(time = da_clipped.time[(t >= pd.to_datetime(date(postw[0][1], postw[0][0], 1))) & (t <= pd.to_datetime(date(postw[1][1], postw[1][0], 1)))])
                post = unscaling(post, product_type)
                name = ID + '_L_' + product_type + '_' + str(postw[0][1]) + '_wet' + str(j) + '.tif'
                post.rio.to_raster(folder_name + '/' + name)

                diff = post - pre
                name = ID + '_L_' + product_type + '_' + str(prew[0][1]) + '_' + str(postw[0][1]) + '_wet' + str(j) + '.tif'
                diff.rio.to_raster(folder_name + '/' + name)

            j += 1
		    
		    
        for pred, postd in zip(pre_dry, post_dry):

			# Check data is missing to process the asset
            if pd.to_datetime(date(postd[1][1], postd[1][0], 1)) > t[-1]:
                unprocessed.append([ID, 'No data post intervention', postd])
                print('There is no dry season post intervention') 
                
            else:
		    
                pre = da_clipped.sel(time = da_clipped.time[(t >= pd.to_datetime(date(pred[0][1], pred[0][0], 1))) & (t <= pd.to_datetime(date(pred[1][1], pred[1][0], 1)))])
                pre = unscaling(pre, product_type)
                name = ID + '_L_' + product_type + '_' + str(pred[0][1]) + '_dry.tif'
                pre.rio.to_raster(folder_name + '/' + name)

                post = da_clipped.sel(time = da_clipped.time[(t >= pd.to_datetime(date(postd[0][1], postd[0][0], 1))) & (t <= pd.to_datetime(date(postd[1][1], postd[1][0], 1)))])
                post = unscaling(post, product_type)
                name = ID + '_L_' + product_type + '_' + str(postd[0][1]) + '_dry.tif'
                post.rio.to_raster(folder_name + '/' + name)

                diff = post - pre
                name = ID + '_L_' + product_type + '_' + str(pred[0][1]) + '_' + str(postd[0][1]) + '_dry.tif'
                diff.rio.to_raster(folder_name + '/' + name)

    unprocessed = pd.DataFrame(unprocessed, columns = ['asset', 'issue', 'season'])
    name = 'Unprocessed_' + product_type + '.csv'
    unprocessed.to_csv(folder_name + '/' + name)