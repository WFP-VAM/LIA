# regional average monthly rainfall and max NDVI values during year, irrigation crops

import warnings
warnings.filterwarnings("ignore")

import os
import csv
import pathlib
import numpy as np
import pandas as pd
import xarray as xr
import geopandas as gpd
from datetime import date
import matplotlib.pyplot as plt
from scipy.stats import linregress
from utils.helper_fns import delete_directory, check_asset_size


def run(da_chirps, da_ndvi, sat, shapefiles: list, wet_season: list, dry_season: list, asset_info: pd.DataFrame, path_output: str):

    # Create output folder
	folder_name = path_output + '/' + 'rfh_ndvi'
	delete_directory(folder_name)
	pathlib.Path(folder_name).mkdir(parents=True, exist_ok=True)

	unprocessed = []
    
	for i,shapefile in enumerate(shapefiles):
 
        # Get asset ID
		ID = os.path.basename(shapefile)[:-4]
		print('-- Processing asset ' + ID + ' (' + str(i + 1) + '/' + str(len(shapefiles)) + ') --')
        
		if sat[1]:
			path = 'data/Rasters/LANDSAT_SENTINEL/' + ID 
			da_ndvi = xr.open_zarr(path + '/NDVI_smoothed_monthly.zarr')
			da_ndvi = da_ndvi.band.rio.write_crs("epsg:32637", inplace=True) 
        
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


        # Clip rasters
		da_chirps_clipped = da_chirps.rio.clip(gdf_buf.geometry.values, gdf_buf.crs)
		da_ndvi_clipped = da_ndvi.rio.clip(gdf.geometry.values, gdf.crs)
		da_ndvi_out = da_ndvi.rio.clip(gdf_buf_out.geometry.values, gdf.crs) 
        
        # Rescale values (chirps: sum over the month / ndvi: normalize)
		da_chirps_clipped = da_chirps_clipped * 3
		da_ndvi_clipped = da_ndvi_clipped / 10000
		da_ndvi_out = da_ndvi_out / 10000

        # Get intervention dates
		start_intervention = asset_info.loc[ID].start
		end_intervention = asset_info.loc[ID].end

        # Mean NDVI values (FFA and non FFA site)
		t = da_ndvi_clipped.time
		mean_ndvi_non = da_ndvi_out.mean(dim = ['longitude','latitude']).sel(time = t[(t.values >= pd.to_datetime(date(start_intervention[1]-3, 1, 1))) & (t.values <= pd.to_datetime(date(end_intervention[1]+3, 12, 31)))])
		mean_ndvi_ffa = da_ndvi_clipped.mean(dim = ['longitude','latitude']).sel(time = t[(da_ndvi_clipped.time.values >= pd.to_datetime(date(start_intervention[1]-3, 1, 1))) & (t.values <= pd.to_datetime(date(end_intervention[1]+3, 12, 31)))])
        
        # Maximum NDVI values (lta and not averaged)
		max_ndvi_lta = da_ndvi_clipped.groupby('time.month').mean().max(dim = ['longitude','latitude'])
		max_ndvi = da_ndvi_clipped.max(dim = ['longitude','latitude']).sel(time = t[(t.values >= pd.to_datetime(date(start_intervention[1]-3, 1, 1))) & (t.values <= pd.to_datetime(date(end_intervention[1]+3, 12, 31)))])
        
        # Mean CHIRPS values (lta and not averaged)
		t = da_chirps_clipped.time
		mean_chirps_lta = da_chirps_clipped.groupby('time.month').mean().mean(dim=['latitude','longitude'])
		mean_chirps = da_chirps_clipped.mean(dim=['latitude','longitude']).sel(time = t[(t.values >= pd.to_datetime(date(start_intervention[1]-3, 1, 1))) & (t.values <= pd.to_datetime(date(end_intervention[1]+3, 12, 31)))])
        
        # Create csv
		with open(folder_name + '/' + ID + '_L_rfh.csv', 'w', newline='', encoding='UTF8') as f:
            # create the csv writer
			writer = csv.writer(f, delimiter = ';')
            # define zip to write columns
			rows = zip([*['date'],*list(pd.to_datetime(da_chirps.time.values).strftime("%Y-%m"))], 
                       [*['max_ndvi'],*list(max_ndvi_lta.values)], 
                       [*['mean_chirps'],*list(mean_chirps_lta.values)])
            # write row by row to the csv file
			for row in rows:
				writer.writerow(row)

        # Crop to same timestamps
		t = list(set(mean_chirps.time.values).intersection(mean_ndvi_ffa.time.values))
		set(mean_chirps.time.values)
		t.sort()
		mean_chirps = mean_chirps.sel(time = t)
		mean_ndvi_ffa = mean_ndvi_ffa.sel(time = t)
		mean_ndvi_non = mean_ndvi_non.sel(time = t)
        
        # Linear regressions NDVI 
		lin_reg_non = linregress(range(1,len(mean_chirps.time.values)+1),mean_ndvi_non.values)
		lin_reg_ffa = linregress(range(1,len(mean_chirps.time.values)+1),mean_ndvi_ffa.values)       
            
        # Plot regional average monthly rainfall & NDVI
		name = ID + '_L_lta_rfh.png'
		fig,ax = plt.subplots(1,1,figsize=(20,10))
		ax.bar(range(1,13), mean_chirps_lta, color = '#a7c3d1', label = 'Average monthly rainfall')
		ax.set_ylabel('Rainfall (mm)',fontsize=19)
		ax.tick_params(axis='both', labelsize=15)
		ax.set_xticks(np.arange(min(range(1,13)), max(range(1,13))+1, 1.0))
		ax.set_xticklabels(['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'])
		ax2 = ax.twinx()
		ax2.plot(range(1,13), max_ndvi_lta, 'o-', color = 'green', label = 'Maximum average NDVI')
		ax2.set_ylabel('NDVI',fontsize=19)
		ax2.tick_params(axis='both', labelsize=15)
		ax2.set_ylim(min(max_ndvi_lta)-0.1, max(max_ndvi_lta)+0.1)        
		fig.legend()
		fig.suptitle('Regional Average Monthly Rainfall & NDVI', fontsize=24) 
		plt.title(ID, fontsize=20) 
		plt.savefig(folder_name + '/' + name)
        
        # Plot regional monthly rainfall & NDVI
		name = ID + '_L_rfh.png'
		n_years = end_intervention[1] - start_intervention[1] + 3 + 4
		t = range(1,len(mean_chirps.time.values)+1)
		fig,ax = plt.subplots(1,1,figsize=(20,10))
		ax.bar(t, mean_chirps, color = '#a7c3d1', label = 'Average monthly rainfall')
		ax.set_ylabel('Rainfall (mm)',fontsize=19)
		ax.tick_params(axis='both', labelsize=15)
		ax.set_xticks(np.arange(1, len(mean_chirps.time.values)+1, 1.0))
		ax.set_xticklabels((['J','F','M','A','M','J','J','A','S','O','N','D']*n_years)[:len(t)])
		ax2 = ax.twinx()
		ax2.plot(t, mean_ndvi_ffa, '-', color = 'green', label = 'NDVI at FFA site')
		ax2.plot(t, mean_ndvi_non, '-', color = 'darkslategrey', label = 'NDVI Non FFA site')
		ax2.plot(t, lin_reg_ffa.slope * t + lin_reg_ffa.intercept, ':', color = 'green', label = 'Linear (NDVI at FFA site)')
		ax2.plot(t, lin_reg_non.slope * t + lin_reg_ffa.intercept, ':', color = 'darkslategrey', label = 'Linear (NDVI Non FFA site)')
		ax2.vlines(x = np.array(t[::12][1:])-0.5, ymin = min(mean_ndvi_ffa)-0.1, ymax = max(mean_ndvi_ffa)+0.1, linestyles = 'dashdot')
		if pd.to_datetime(date(start_intervention[1], start_intervention[0], 1)) > mean_chirps.time.values[0]:
			ax2.vlines(x = np.array(t[36+start_intervention[0]-1]), ymin = min(mean_ndvi_ffa)-0.1, ymax = max(mean_ndvi_ffa)+0.1, linestyles = 'dotted', color = 'red')
			ax2.annotate('start\nintervention', (t[36+start_intervention[0]-1]+0.1,max(mean_ndvi_ffa)+0.07), color='red')
		if pd.to_datetime(date(end_intervention[1], end_intervention[0], 1)) < mean_chirps.time.values[-1]:
			ax2.vlines(x = np.array(t[12*(n_years-4)+end_intervention[0]-1]), ymin = min(mean_ndvi_ffa)-0.1, ymax = max(mean_ndvi_ffa)+0.1, linestyles = 'dotted', color = 'red')
			ax2.annotate('end\nintervention', (t[12*(n_years-4)+end_intervention[0]-1]+0.1,max(mean_ndvi_ffa)+0.07), color='red')
		for i in range(n_years): ax2.annotate(str(start_intervention[1]-3+i),(6+12*i,max(mean_ndvi_ffa)+0.05), fontsize = 18)
		ax2.annotate('R2 = '+str(round(lin_reg_ffa.rvalue**2,4)),(t[-1],lin_reg_ffa.slope * t[-1] + lin_reg_ffa.intercept))
		ax2.annotate('R2 = '+str(round(lin_reg_non.rvalue**2,4)),(t[-1],lin_reg_non.slope * t[-1] + lin_reg_ffa.intercept))
		ax2.set_ylabel('NDVI',fontsize=19)
		ax2.tick_params(axis='both', labelsize=15)
		ax2.set_ylim(min(mean_ndvi_ffa)-0.1, max(mean_ndvi_ffa)+0.1)  
		fig.legend()
		fig.suptitle('Regional Monthly Rainfall & NDVI trends ' + str(start_intervention[1]-3) + ' to ' + str(min(2022,end_intervention[1]+3)), fontsize=24) 
		plt.title(ID, fontsize=20) 
		plt.savefig(folder_name + '/' + name)
        
        
	unprocessed = pd.DataFrame(unprocessed, columns = ['asset', 'issue'])
	name = 'Unprocessed.csv'
	unprocessed.to_csv(folder_name + '/' + name)        