# TCI/VCI/VHI pre/post asset area vs surroundings

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
from tqdm import tqdm
import xarray as xr
import zarr
import dask 

from utils.helper_fns import delete_directory, check_asset_size

import warnings
warnings.filterwarnings("ignore")


def group_by_season(da, season: tuple):
    
    # Get month begin and end of season
    (m1, m2) = season
    
    # Get time dimension
    t = da.time
    
    # Create tuples of begin end seasons
    if m1 < m2:
        tt = [(pd.to_datetime(date(x, m1, 1)), pd.to_datetime(date(x, m2, 1))) for x in list(set(da.time.dt.year.values))]
    else:
        tt = [(pd.to_datetime(date(x, m1, 1)), pd.to_datetime(date(x + 1, m2, 1))) for x in list(set(da.time.dt.year.values))[0:-1]]

    
    # Remove begin and end tuple if out of da
    if tt[0][0] < t[0]:
        tt = tt[1:]
    if tt[-1][1] > t[-1]:
        tt = tt[:-1]

    
    # Group by seasons
    da_grp = da.sel(time = da.time[(t >= tt[0][0]) & (t <= tt[0][1])]).mean('time')    
    for tt_ in tt[1:]:
        temp = da.sel(time = da.time[(t >= tt_[0]) & (t <= tt_[1])]).mean('time')
        da_grp = xr.concat([da_grp,temp], dim = 'time')

    # Assign timestamps
    da_grp = da_grp.assign_coords(time = [x[0] for x in tt])
    
    return da_grp

def compute_tci_vci_vhi(LST, NDVI, season: tuple, alpha: float):


	# Compute VCI
	NDVI_grp = group_by_season(NDVI, season)
	VCI = (NDVI_grp.quantile(0.9, dim = 'time') - NDVI_grp) / (NDVI_grp.quantile(0.9, dim = 'time') - NDVI_grp.quantile(0.1, dim = 'time')) 
	VCI = VCI.where(VCI > 0, 0) 
	VCI = VCI.where(VCI < 1, 1)

	# Compute TCI
	LST_grp = group_by_season(LST, season)
	TCI = (LST_grp.quantile(0.9, dim = 'time') - LST_grp) / (LST_grp.quantile(0.9, dim = 'time') - LST_grp.quantile(0.1, dim = 'time')) 
	TCI = TCI.where(TCI > 0, 0) 
	TCI = TCI.where(TCI < 1, 1)

	# Compute VHI
	VHI = alpha*VCI + (1-alpha)*TCI

	return (VCI, TCI, VHI)


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



def save_rasters_wet(da, prew: list, postw: list, i: int, folder_name: str, pdct: str, ID: str):

	da_pre = da.sel(time = pd.to_datetime(date(prew[0][1], prew[0][0], 1)))
	name = ID + '_L_' + pdct + '_wet' + str(i) + '_' + str(prew[0][1]) + '.tif'
	da_pre.rio.to_raster(folder_name + '/' + name)

	da_post = da.sel(time = pd.to_datetime(date(postw[0][1], postw[0][0], 1)))
	name = ID + '_L_' + pdct + '_wet' + str(i) + '_' + str(postw[0][1]) + '.tif'
	da_post.rio.to_raster(folder_name + '/' + name)

	diff = da_post - da_pre
	name = ID + '_L_' + pdct + '_wet' + str(i) + '_' + str(prew[0][1]) + '_' + str(postw[0][1]) + '.tif'
	diff.rio.to_raster(folder_name + '/' + name)


def save_rasters_dry(da, pred: list, postd: list, folder_name: str, pdct: str, ID: str):

	da_pre = da.sel(time = pd.to_datetime(date(pred[0][1], pred[0][0], 1)))
	name = ID + '_L_' + pdct + '_dry' + '_' + str(pred[0][1]) + '.tif'
	da_pre.rio.to_raster(folder_name + '/' + name)

	da_post = da.sel(time = pd.to_datetime(date(postd[0][1], postd[0][0], 1)))
	name = ID + '_L_' + pdct + '_dry' + '_' + str(postd[0][1]) + '.tif'
	da_post.rio.to_raster(folder_name + '/' + name)

	diff = da_post - da_pre
	name = ID + '_L_' + pdct + '_dry' + '_' + str(pred[0][1]) + '_' + str(postd[0][1]) + '.tif'
	diff.rio.to_raster(folder_name + '/' + name)




def run(LST, NDVI, shapefiles: list, wet_season: list, dry_season: list, asset_info: pd.DataFrame, path_output: str, alpha: float):

	# Create output folder
	folder_name = path_output+ '/prepost/TCI_VCI_VHI'
	delete_directory(folder_name)
	pathlib.Path(folder_name).mkdir(parents=True, exist_ok=True)

	unprocessed = []

	for i,shapefile in enumerate(shapefiles):
	    
		# Get asset ID
		ID = os.path.basename(shapefile)[:-4]
		print('-- Processing asset ' + ID + ' (' + str(i + 1) + '/' + str(len(shapefiles)) + ') --')

		# Reading asset
		gdf = gpd.read_file(shapefile)

		# Check asset size        
		if (not check_asset_size(LST, gdf)) or (not check_asset_size(NDVI, gdf)):
			print('The asset is too small to be processed')
			unprocessed.append([ID, 'Asset too small', 'N/A'])
			continue

		# 0.2 degree buffer around asset
		gdf_buf = gdf.buffer(0.2, cap_style = 3)

		# Clip rasters
		LST_clipped = LST.rio.clip(gdf_buf.geometry.values, gdf_buf.crs)
		NDVI_clipped = NDVI.rio.clip(gdf_buf.geometry.values, gdf_buf.crs)

		# Load values
		LST_clipped.load()
		NDVI_clipped.load()

		# Crop NDVI and LST to same time length
		t = np.intersect1d(LST.time.values, NDVI.time.values)
		NDVI_clipped = NDVI_clipped.sel(time = t)
		LST_clipped = LST_clipped.sel(time = t)

		# Unscale
		NDVI_clipped = NDVI_clipped / 10000
		LST_clipped = LST_clipped * 0.02 - 273.15

		# Get intervention dates
		start_intervention = asset_info.loc[ID].start
		end_intervention = asset_info.loc[ID].end

		# Get pre and post dates nearest to intervention date
		(pre_wet, post_wet, pre_dry, post_dry) = get_pre_post_dates(start_intervention, end_intervention, wet_season, dry_season)

		#  COMPUTATION
		i = 0
		for prew, postw, ws in zip(pre_wet, post_wet, wet_season):

			# Check data is missing to process the asset
			if pd.to_datetime(date(postw[1][1], postw[1][0], 1)) > t[-1]:

				unprocessed.append([ID, postw])

			else:

				V_T_V = compute_tci_vci_vhi(LST_clipped, NDVI_clipped, ws, alpha)
				pdcts = ('VCI','TCI','VHI')

				for da, pdct in zip(V_T_V, pdcts):
					save_rasters_wet(da, prew, postw, i, folder_name, pdct, ID)

				i += 1

		for pred, postd, ds in zip(pre_dry, post_dry, dry_season):

			# Check data is missing to process the asset
			if pd.to_datetime(date(postd[1][1], postd[1][0], 1)) > t[-1]:

				unprocessed.append([ID, postd])

			else:
		    
			    V_T_V = compute_tci_vci_vhi(LST_clipped, NDVI_clipped, ds, alpha)
			    pdcts = ('VCI','TCI','VHI')


			    for da, pdct in zip(V_T_V, pdcts):
			    	save_rasters_dry(da, pred, postd, folder_name, pdct, ID)

	unprocessed = pd.DataFrame(unprocessed, columns = ['asset', 'season'])
	name = 'Unprocessed_TCI_VCI_VHI' + '.csv'
	unprocessed.to_csv(folder_name + '/' + name)





