# regional average monthly rainfall and max NDVI values during year, irrigation crops

import os
import csv
import pathlib
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt

import warnings
warnings.filterwarnings("ignore")


def run(da_chirps, da_ndvi, shapefiles: list, wet_season: list, dry_season: list, asset_info: pd.DataFrame, path_output: str):

    # Create output folder
	folder_name = path_output + '/' + 'rainfall'
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
		da_chirps_clipped = da_chirps.rio.clip(gdf_buf.geometry.values, gdf_buf.crs)
		da_ndvi_clipped = da_ndvi.rio.clip(gdf.geometry.values, gdf.crs)

		# Load CHIRPS values
		da_chirps_clipped.load() * 3
		da_ndvi_clipped.load() / 10000
        
        # Rescale values (chirps: sum over the month / ndvi: normalize)
		da_chirps_clipped = da_chirps_clipped * 3
		da_ndvi_clipped = da_ndvi_clipped / 10000

		# Get intervention dates - A VOIR S'IL FAUT GARDER
		#start_intervention = asset_info.loc[ID].start
		#end_intervention = asset_info.loc[ID].end

		# Get comparison dates - A VOIR S'IL FAUT GARDER
		#(pre_wet, post_wet, pre_dry, post_dry) = get_pre_post_dates(start_intervention, end_intervention, wet_season, dry_season)

        # Maximum NDVI values per month
		max_ndvi = da_ndvi_clipped.groupby('time.month').max().max(dim = ['longitude','latitude'])
        
        # Mean CHIRPS values per month
		mean_chirps = da_chirps_clipped.groupby('time.month').mean().mean(dim=['latitude','longitude'])
        
        
        # Create csv
		with open(folder_name + '/' + ID + '_rfh.csv', 'w', encoding='UTF8') as f:
            # create the csv writer
			writer = csv.writer(f)
            # write first row to the csv file
			writer.writerow(da_chirps.time.values)
            # write max_ndvi
			writer.writerow(max_ndvi.values)
            # write mean_chirps
			writer.writerow(mean_chirps.values)

            
        # Plot regional average monthly rainfall & NDVI
		name = ID + '_lta_rfh.png'
		fig,ax = plt.subplots(1,1,figsize=(20,10))
		ax.bar(range(1,13), mean_chirps, color = '#a7c3d1', label = 'Average monthly rainfall')
		ax.set_ylabel('Rainfall (mm)')
		ax.set_xticks(np.arange(min(range(1,13)), max(range(1,13))+1, 1.0))
		ax.set_xticklabels(['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'])
		ax2 = ax.twinx()
		ax2.plot(range(1,13), max_ndvi, 'o-', color = 'green', label = 'Maximum average NDVI')
		ax2.set_ylabel('NDVI')
		ax2.set_ylim(min(max_ndvi)-0.1, max(max_ndvi)+0.1)        
		plt.legend()
		plt.suptitle('Regional Average Monthly Rainfall & NDVI') 
		plt.title(ID) 
		plt.savefig(folder_name + '/' + name)
