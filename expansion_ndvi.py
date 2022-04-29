# Expansion of area with NDVI > 0.5 (map)

import os
import pathlib
import numpy as np
import pandas as pd
import xarray as xr
import geopandas as gpd
from datetime import date
import xarray.ufuncs as xru
from helper_fns import delete_directory, check_asset_size

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


def date_to_str(x: tuple):

    return str(x[1]) + "{0:0=2d}".format(x[0])

def run(da, shapefiles: list, wet_season: list, dry_season: list, asset_info: pd.DataFrame, path_output: str):

    # Create output folder
    folder_name = path_output + '/' + 'NDVI_expansion'
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
        if not check_asset_size(da, gdf):
            print('The asset is too small to be processed')
            unprocessed.append([ID, 'Asset too small', 'N/A'])
            continue

        # Clip rasters
        da_clipped = da.rio.clip(gdf.geometry.values, gdf.crs)

        # Load da values
        da_clipped.load()

        # Rescale the data
        da_clipped = da_clipped / 10000

        # Get intervention dates
        start_intervention = asset_info.loc[ID].start
        end_intervention = asset_info.loc[ID].end

        # Get comparison dates
        (pre_wet, post_wet, pre_dry, post_dry) = get_pre_post_dates(start_intervention, end_intervention, wet_season, dry_season)

        t = da_clipped.time.values

        for j, (prew, postw) in enumerate(zip(pre_wet, post_wet)):

            # Check if data is missing to process the post analysis of the asset
            if pd.to_datetime(date(postw[1][1], postw[1][0], 1)) > t[-1]:
                unprocessed.append([ID, 'No data post intervention', postw])
                if j==0:
                    print('There is no wet season post intervention')
                else:
                    print('There is no second wet season post intervention')

            else:
                pre = da_clipped.sel(time = da_clipped.time[(t >= pd.to_datetime(date(prew[0][1], prew[0][0], 1))) & (t <= pd.to_datetime(date(prew[1][1], prew[1][0], 1)))]).max(dim='time')
                exp_pre = xr.where(pre >= 0.5, 1, 0)
                exp_pre = exp_pre.where(xru.logical_not(xru.isnan(pre)), np.nan)
                name_pre = ID + '_L_' + 'NDVI' + '_' + str(prew[0][1]) + '_wet' + str(j+1) + '.tif'
                exp_pre.rio.to_raster(folder_name + '/' + name_pre)

                post = da_clipped.sel(time = da_clipped.time[(t >= pd.to_datetime(date(postw[0][1], postw[0][0], 1))) & (t <= pd.to_datetime(date(postw[1][1], postw[1][0], 1)))]).max(dim='time')
                exp_post = xr.where(post >= 0.5, 1, 0)
                exp_post = exp_post.where(xru.logical_not(xru.isnan(post)), np.nan)
                name_post = ID + '_L_' + 'NDVI' + '_' + str(postw[0][1]) + '_wet' + str(j+1) + '.tif'
                exp_post.rio.to_raster(folder_name + '/' + name_post)


        for pred, postd in zip(pre_dry, post_dry):

            # Check if data is missing to process the post analysis of the asset
            if pd.to_datetime(date(postd[1][1], postd[1][0], 1)) > t[-1]:
                unprocessed.append([ID, 'No data post intervention', postd])
                print('There is no dry season post intervention')

            else:
                pre = da_clipped.sel(time = da_clipped.time[(t >= pd.to_datetime(date(pred[0][1], pred[0][0], 1))) & (t <= pd.to_datetime(date(pred[1][1], pred[1][0], 1)))]).max(dim='time')
                exp_pre = xr.where(pre >= 0.5, 1, 0)
                exp_pre = exp_pre.where(xru.logical_not(xru.isnan(pre)), np.nan)
                name_pre = ID + '_L_' + 'NDVI' + '_' + str(pred[0][1]) + '_dry' + '.tif'
                exp_pre.rio.to_raster(folder_name + '/' + name_pre)

                post = da_clipped.sel(time = da_clipped.time[(t >= pd.to_datetime(date(postd[0][1], postd[0][0], 1))) & (t <= pd.to_datetime(date(postd[1][1], postd[1][0], 1)))]).max(dim='time')
                exp_post = xr.where(post >= 0.5, 1, 0)
                exp_post = exp_post.where(xru.logical_not(xru.isnan(post)), np.nan)
                name_post = ID + '_L_' + 'NDVI' + '_' + str(postd[1][1]) + '_dry' + '.tif'
                exp_post.rio.to_raster(folder_name + '/' + name_post)


    unprocessed = pd.DataFrame(unprocessed, columns = ['asset', 'issue', 'season'])
    name = 'Unprocessed.csv'
    unprocessed.to_csv(folder_name + '/' + name)
