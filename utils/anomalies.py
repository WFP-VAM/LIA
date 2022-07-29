# Anomalies of NDVI/LST/Rainfall against LTA

import os
import pathlib
import numpy as np
import pandas as pd
import xarray as xr
import geopandas as gpd
import matplotlib.pyplot as plt
from utils.helper_fns import delete_directory, check_asset_size

import warnings
warnings.filterwarnings("ignore")

def compute_anomalies(da_chirps, da_ndvi, da_lst, folder_name, asset=''):
    
        # Compute lta of the datasets
        da_ndvi_lta = da_ndvi.groupby(da_ndvi.time.dt.strftime("%m-%d")).mean()
        da_lst_lta = da_lst.groupby(da_lst.time.dt.strftime("%m-%d")).mean()
        da_chirps_lta = da_chirps.groupby(da_chirps.time.dt.strftime("%m-%d")).mean()

        # Take the mean over the coutry
        da_ndvi_country = da_ndvi.mean(dim=['latitude','longitude'])
        da_lst_country = da_lst.mean(dim=['latitude','longitude'])
        da_chirps_country = da_chirps.mean(dim=['latitude','longitude'])
    
        da_ndvi_lta = da_ndvi_lta.mean(dim=['latitude','longitude'])
        da_lst_lta = da_lst_lta.mean(dim=['latitude','longitude'])
        da_chirps_lta = da_chirps_lta.mean(dim=['latitude','longitude'])
    
        # Compute the anomalies    
        def scale_ndvi(x):
            x_lta = da_ndvi_lta.sel(strftime = x.time.dt.strftime("%m-%d"))
            return (x - x_lta)

        def scale_lst(x):
            x_lta = da_lst_lta.sel(strftime = x.time.dt.strftime("%m-%d"))
            return (x - x_lta)
    
        def scale_chirps(x):
            x_lta = da_chirps_lta.sel(strftime = x.time.dt.strftime("%m-%d"))
            return 100*(x - x_lta)/x_lta
    
        ndvi_anom = da_ndvi_country.groupby(da_ndvi_country.time.dt.strftime("%m-%d")).map(scale_ndvi)
        lst_anom = da_lst_country.groupby(da_lst_country.time.dt.strftime("%m-%d")).map(scale_lst)
        chirps_anom = da_chirps_country.groupby(da_chirps_country.time.dt.strftime("%m-%d")).map(scale_chirps)
    
        # Plot the anomalies 
        # NDVI
        color = []
        ndvi = ndvi_anom.values
        for i in range(len(ndvi)):
            if ndvi[i]<0:
                color.append('orange')
            else:
                color.append('green')
                
        temp = ndvi_anom.to_series()
        temp = temp.set_axis(temp.index.year)
       
        fig,ax = plt.subplots(1,1,figsize=(14,5))
        ax = temp.plot.bar(color=color, width=0.98, alpha=0.6)
        xticks = []
        for i, t in enumerate(ax.get_xticklabels()):
            if (i % 12) == 0:
                xticks.append(i)
        ax.set_xticks(xticks)    
        plt.grid()
        ax.set_ylabel('NDVI anomalies')
        max_y = np.abs(temp).max() + 0.03
        ax.set_ylim(-max_y,max_y)
    
        ax2 = ax.twinx()
        ax2.plot(chirps_anom, alpha=0.7, color='grey')
        ax2.set_ylabel('Rainfall anomalies (%)')
        max_y = np.abs(chirps_anom).max() + 5
        ax2.set_ylim(-max_y,max_y)
    
        plt.title('Anomalies')
        plt.savefig(folder_name + '/' + asset + 'NDVI_anom')
    
        # LST
        plt.clf()
        color = []
        lst = lst_anom.values
        for i in range(len(lst)):
            if lst[i]<0:
                color.append('darkblue')
            else:
                color.append('red')
            
        temp = lst_anom.to_series()
        temp = temp.set_axis(temp.index.year)

        ax = temp.plot.bar(color=color, width=0.98)
        xticks = []
        for i, t in enumerate(ax.get_xticklabels()):
            if (i % 12) == 0:
                xticks.append(i)
        ax.set_xticks(xticks)
        plt.title('LST Anomalies')
        plt.savefig(folder_name + '/' + asset + 'LST_anom')
        
        
def run(da_chirps, da_ndvi, da_lst, sat, shapefiles: list, asset_info: pd.DataFrame, path_output: str):
    
    # Create output folder
    folder_name = path_output + '/' + 'Anomalies'
    delete_directory(folder_name)
    pathlib.Path(folder_name).mkdir(parents=True, exist_ok=True)
    
    ### Anomalies over the whole country
    print('-- Computing anomalies over the whole country --')
    
    run = True
    if sat[1]:
        try:
            path_to_zarr = 'data/Rasters/MODIS/zarr_data/'
            da_ndvi = xr.open_zarr(path_to_zarr + 'NDVI.zarr').band
            da_lst  = xr.open_zarr(path_to_zarr + 'LST.zarr').band
        except:
            print('You have to download MODIS\'s NDVI and LST first by running LIA_dowload.py to compute the anomalies over the whole country')
            run = False
    
    if run:
        
        # Rescale the data
        da_ndvi = da_ndvi / 10000
        da_lst = da_lst * 0.02 - 273.15
        da_chirps = da_chirps * 3
    
        da_ndvi = da_ndvi.astype('float32')
        da_ndvi = da_ndvi.where(da_ndvi >= 0)
        da_lst = da_lst.astype('float32')
        da_lst = da_lst.where(da_lst >= 0)
        da_chirps = da_chirps.astype('float32')
        da_chirps = da_chirps.where(da_chirps >= 0)
        
        compute_anomalies(da_chirps, da_ndvi, da_lst, folder_name)
        
    
    ### NDVI at FFA against controle site / Anomalies at each control site  
    unprocessed = []
    
    for i,shapefile in enumerate(shapefiles):
        
        # Get asset ID
        ID = os.path.basename(shapefile)[:-4]
        print('--     Processing asset ' + ID + ' (' + str(i + 1) + '/' + str(len(shapefiles)) + ')     --')
        
        # Create folder for this asset 
        folder_asset = folder_name + '/' + ID
        pathlib.Path(folder_asset).mkdir(parents=True, exist_ok=True)
        
        if sat[1]:
            path = 'data/Rasters/LANDSAT_SENTINEL/' + ID 
            
            da_ndvi = xr.open_zarr(path + '/NDVI_smoothed_monthly.zarr')
            da_ndvi = da_ndvi.band.rio.write_crs("epsg:32637", inplace=True) 
            da_lst = xr.open_zarr(path + '/LST_smoothed_monthly.zarr')
            da_lst = da_lst.band.rio.write_crs("epsg:32637", inplace=True)
    
        # Reading asset
        gdf = gpd.read_file(shapefile)

        # 0.2 degree buffer around asset
        gdf_buf = gdf.buffer(0.1, cap_style = 3)
        
        # Check asset size  
        if not check_asset_size(da_ndvi, gdf):
            print('The asset is too small to be processed')
            unprocessed.append([ID, 'Asset too small for NDVI'])
            continue  
        if not check_asset_size(da_chirps, gdf_buf):
            print('The asset is too small to be processed')
            unprocessed.append([ID, 'Asset too small for CHIRPS'])
            continue 

        # Non asset site 
        gdf_buf_out = gdf_buf - gdf
        
        # Rescale the data
        da_ndvi = da_ndvi / 10000
        da_lst = da_lst * 0.02 - 273.15
        da_chirps = da_chirps * 3
    
        da_ndvi = da_ndvi.astype('float32')
        da_ndvi = da_ndvi.where(da_ndvi >= 0)
        da_lst = da_lst.astype('float32')
        da_lst = da_lst.where(da_lst >= 0)
        da_chirps = da_chirps.astype('float32')
        da_chirps = da_chirps.where(da_chirps >= 0)
        
        # Compute the anomalies
        compute_anomalies(da_chirps, da_ndvi, da_lst, folder_asset, ID+'_')
        
        # Clip rasters
        da_ndvi_clipped = da_ndvi.rio.clip(gdf.geometry.values, gdf.crs)
        da_ndvi_out = da_ndvi.rio.clip(gdf_buf_out.geometry.values, gdf.crs) 
        
        # Get intervention dates
        start_intervention = asset_info.loc[ID].start
        end_intervention = asset_info.loc[ID].end

        # Mean NDVI values (FFA and non FFA site)
        t = da_ndvi_clipped.time
        mean_ndvi_non = da_ndvi_out.mean(dim = ['longitude','latitude'])
        mean_ndvi_ffa = da_ndvi_clipped.mean(dim = ['longitude','latitude'])
        diff = mean_ndvi_ffa - mean_ndvi_non
        
        # Plot the difference in NDVI at the FFA and controle site
        color = []
        ndvi = diff.values
        for i in range(len(ndvi)):
            if ndvi[i]<0:
                color.append('orange')
            else:
                color.append('green')
        
        temp = diff.to_series()
        temp = temp.set_axis(temp.index.year)
        
        start_ind = np.where(temp.index==start_intervention[1])[0][start_intervention[0]-1]
        end_ind = np.where(temp.index==end_intervention[1])[0][end_intervention[0]-1]
        
        fig,ax = plt.subplots(1,1,figsize=(14,5))
        ax = temp.plot.bar(color=color, width=0.98, alpha=0.6)
        xticks = []
        for i, t in enumerate(ax.get_xticklabels()):
            if (i % 12) == 0:
                xticks.append(i)
        
        ax.set_xticks(xticks)    
        plt.grid()
        ax.set_ylabel('NDVI (FFA - Controle Site)')
        max_y = np.abs(temp).max() + 0.03
        ax.set_ylim(-max_y,max_y)

        ax.vlines(x = start_ind, ymin = -max_y, ymax = max_y, linestyles = 'dotted', color = 'red')
        ax.annotate('start\nintervention', (start_ind-18,max_y-0.04), color='red')
        ax.vlines(x = end_ind, ymin = -max_y, ymax = max_y, linestyles = 'dotted', color = 'red')
        ax.annotate('end\nintervention', (end_ind+1,max_y-0.04), color='red')

        plt.title('NDVI at FFA against Control Site')
        plt.savefig(folder_asset + '/' + ID + '_NDVI_FFA_CS_diff.png')


    unprocessed = pd.DataFrame(unprocessed, columns = ['asset', 'issue'])
    name = 'Unprocessed.csv'
    unprocessed.to_csv(folder_name + '/' + name)  