# average annual rainfall

import os
import csv
import pathlib
import numpy as np
import pandas as pd
import geopandas as gpd
from datetime import date
import matplotlib.pyplot as plt

import warnings
warnings.filterwarnings("ignore")


def run(da, shapefiles: list, wet_season: list, dry_season: list, asset_info: pd.DataFrame, path_output: str):

    # Create output folder
	folder_name = path_output + '/' + 'enso'
	pathlib.Path(folder_name).mkdir(parents=True, exist_ok=True)

    
	for i,shapefile in enumerate(shapefiles):
	    
		# Get asset ID
		ID = os.path.basename(shapefile)[:-4]
		print('-- Processing asset ' + ID + ' (' + str(i + 1) + '/' + str(len(shapefiles)) + ') --')

		# Reading asset
		gdf = gpd.read_file(shapefile)

		# 0.5 degree buffer around asset
		gdf_buf = gdf.buffer(0.2, cap_style = 3)

		# Clip rasters
		da_clipped = da_chirps.rio.clip(gdf_buf.geometry.values, gdf_buf.crs)

		# Load CHIRPS values
		da_chirps_clipped.load() * 3
		da_ndvi_clipped.load() / 10000
        
		# Rescale values (chirps: sum over the month / ndvi: normalize)
		da_chirps_clipped = da_chirps_clipped * 3
		da_ndvi_clipped = da_ndvi_clipped / 10000

		# Get intervention dates
		start_intervention = asset_info.loc[ID].start
		end_intervention = asset_info.loc[ID].end

		# Get comparison dates - A VOIR S'IL FAUT GARDER
		#(pre_wet, post_wet, pre_dry, post_dry) = get_pre_post_dates(start_intervention, end_intervention, wet_season, dry_season)

		# Maximum NDVI values (lta and not averaged)
		max_ndvi_lta = da_ndvi_clipped.groupby('time.month').mean().max(dim = ['longitude','latitude'])
		max_ndvi = da_ndvi_clipped.max(dim = ['longitude','latitude']).sel(time = da_ndvi_clipped.time[(da_ndvi_clipped.time.values >= pd.to_datetime(date(start_intervention[1]-1, 1, 1))) & (da_ndvi_clipped.time.values <= pd.to_datetime(date(end_intervention[1]+1, 12, 31)))])
        
		# Mean CHIRPS values (lta and not averaged)
		mean_chirps_lta = da_chirps_clipped.groupby('time.month').mean().mean(dim=['latitude','longitude'])
		mean_chirps = da_chirps_clipped.mean(dim=['latitude','longitude']).sel(time = da_chirps_clipped.time[(da_chirps_clipped.time.values >= pd.to_datetime(date(start_intervention[1]-1, 1, 1))) & (da_chirps_clipped.time.values <= pd.to_datetime(date(end_intervention[1]+1, 12, 31)))])
        
		# Create csv
		with open(folder_name + '/' + ID + '_rfh.csv', 'w', encoding='UTF8') as f:
            # create the csv writer
			writer = csv.writer(f)
            # write first row to the csv file
			writer.writerow(da_chirps.time.values)
            # write max_ndvi
			writer.writerow(max_ndvi_lta.values)
            # write mean_chirps
			writer.writerow(mean_chirps_lta.values)

        # Crop to same timestamps
		t = list(set(mean_chirps.time.values).intersection(max_ndvi.time.values))
		set(mean_chirps.time.values)
		t.sort()
		mean_chirps = mean_chirps.sel(time = t)
		max_ndvi = max_ndvi.sel(time = t)
            
		# Plot regional average monthly rainfall & NDVI
		name = ID + '_lta_rfh.png'
		fig,ax = plt.subplots(1,1,figsize=(20,10))
		ax.bar(range(1,13), mean_chirps_lta, color = '#a7c3d1', label = 'Average monthly rainfall')
		ax.set_ylabel('Rainfall (mm)')
		ax.set_xticks(np.arange(min(range(1,13)), max(range(1,13))+1, 1.0))
		ax.set_xticklabels(['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'])
		ax2 = ax.twinx()
		ax2.plot(range(1,13), max_ndvi_lta, 'o-', color = 'green', label = 'Maximum average NDVI')
		ax2.set_ylabel('NDVI')
		ax2.set_ylim(min(max_ndvi)-0.1, max(max_ndvi)+0.1)        
		plt.legend()
		plt.suptitle('Regional Average Monthly Rainfall & NDVI') 
		plt.title(ID) 
		plt.savefig(folder_name + '/' + name)

		# Plot regional monthly rainfall & NDVI
		n_years = end_intervention[1] - start_intervention[1] + 3        
		name = ID + '_rfh.png'
		fig,ax = plt.subplots(1,1,figsize=(20,10))
		ax.bar(range(1,12*n_years+1), mean_chirps, color = '#a7c3d1', label = 'Average monthly rainfall')
		ax.set_ylabel('Rainfall (mm)')
		ax.set_xticks(np.arange(min(range(1,12*n_years+1)), max(range(1,12*n_years+1))+1, 1.0))
		ax.set_xticklabels(['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']*n_years)
		ax2 = ax.twinx()
		ax2.plot(range(1,12*n_years+1), max_ndvi, '-', color = 'green', label = 'Maximum average NDVI')
		ax2.set_ylabel('NDVI')
		ax2.set_ylim(min(max_ndvi)-0.1, max(max_ndvi)+0.1)        
		plt.legend()
		plt.suptitle('Regional Monthly Rainfall & NDVI trends ' + str(start_intervention[1]-1) + ' to ' + str(end_intervention[1]+1)) 
		plt.title(ID) 
		plt.savefig(folder_name + '/' + name)