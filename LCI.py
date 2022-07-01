import argparse
import calendar
from datetime import date
import datetime
import os
import re
import sys

import dask 
import geopandas as gpd
import glob
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pathlib
import rasterio as rio
import rioxarray as rx
import shutil
from sklearn.linear_model import LinearRegression
from sklearn.cluster import KMeans
from tqdm import tqdm
import xarray as xr
import zarr

from utils.helper_fns import *

import shapely
shapely.speedups.disable()

import warnings
warnings.filterwarnings("ignore")


def filename_to_date(x: str):
	'''
	Converts x string to dekadal date. 
	'''
	# seeking for date pattern in the filename
	dek = re.search(r'\d{4}j\d{3}', x).group()
	
	# getting year, month and dekad
	year = dek[2:4]
	day = dek[5:8]
	
	greg_date = datetime.datetime.strptime(year + day, '%y%j').date()
		
	return greg_date

def filename_to_date2(x: str):
	'''
	Converts x string to dekadal date. 
	'''
	# seeking for date pattern in the filename
	dek = re.search(r'\d{4}\d{2}d\d{1}', x).group()
	
	# getting year, month and dekad
	year = int(dek[0:4])
	month = int(dek[4:6])
	d = dek[7:8]  
	
	if d=="1":
		greg_date = date(year,month,10)
	elif d=="2":
		greg_date = date(year,month,20)
	else:
		greg_date = date(year,month,28)
		
	return greg_date


def create_zarr(files_path: str, zarr_path: str):
    '''
    Create an .zarr file from .tif files
    '''
    
    print('-- Downloading rasters --')
    
    xarray_path = os.path.join(zarr_path, 'xarray.zarr')
    
    files = glob.glob(files_path)

    with xr.open_rasterio(files[0]) as rx0:
        rx0 = rx0.assign_coords({'time':pd.to_datetime(filename_to_date2(files[0]))})
        rx0 = rx0.expand_dims('time')
        rx0.name = 'ndvi'
        rx0_ds = rx0.to_dataset()
        rx0_ds.to_zarr(xarray_path, mode = 'w') #overwrite if exists
    
    for i,file in tqdm(enumerate(files[1:])):
        with xr.open_rasterio(file) as da:
            da = da.assign_coords({'time':pd.to_datetime(filename_to_date2(file))})
            da = da.expand_dims('time')    
            da.name = 'ndvi'
            ds = da.to_dataset()
            ds.to_zarr(xarray_path, append_dim="time")
        
    print('-- Download complete --')


def convert_date(x):
	'''
	Converts string date to month and year
	'''

	x = str(x)

	try:

		try:
			year = re.search(r'\d{4}', x).group()
		except:
			year = '20' + re.search(r'\d{2}', x).group()

		try:
			mon = re.search(r'[a-zA-Z]{3}', x).group()
		except:
			try: 
				mon = re.search(r'[a-zA-Z]{4}', x).group()
			except:
				mon = 'Jan'
	except:
		print('ERROR: StartDate format incorrect in asset_date csv')
		sys.exit()

	return (datetime.datetime.strptime(mon, '%b').month, int(year))



def read_asset_csv_lci(path_to_csv):
	'''
	Reads asset_date csv
	'''
	

	asset_csv = pd.read_csv(path_to_csv, usecols = ['Asset_id', 'StartDate'], index_col = 'Asset_id')
	asset_csv = asset_csv.dropna()	
	asset_csv['month'], asset_csv['year'] = list(zip(*[convert_date(x) for x in asset_csv['StartDate']]))
	asset_csv.drop('StartDate', axis = 1, inplace = True)
	
	return asset_csv


def elbow_method(X, output_path):

	distortions = []
	elbow_threshold = 0.02

	K = range(3,10)

	for k in K:
	    kmeanModel = KMeans(n_clusters=k)
	    kmeanModel.fit(X)
	    distortions.append(kmeanModel.inertia_)

	dist = np.array(distortions)
	dist = dist / sum(dist)
	deltaD = dist[:-1] - dist[1:]
	deltadeltaD = deltaD[:-1] - deltaD[1:]
	kopt = np.array(K[1:-1])[deltadeltaD > elbow_threshold][-1]
	    
	plt.figure(figsize=(16,8))
	plt.plot(K, distortions, 'o-', zorder = 1)
	plt.scatter(kopt, distortions[kopt - 3], color = 'red', zorder = 2)
	plt.xlabel('k')
	plt.ylabel('Distortion')
	plt.title('The Elbow Method showing the optimal k')
	plt.savefig(output_path)


	distortions = np.array(distortions)
	dist = distortions / sum(distortions)
	deltaD = dist[:-1] - dist[1:]
	deltadeltaD = deltaD[:-1] - deltaD[1:]
	kopt = np.array(K[1:-1])[deltadeltaD > elbow_threshold][-1]



	return kopt


def produce_csv(ndvi, clusters, cl):
	'''
	Produces the output csvs
	'''

	df = pd.DataFrame(index = np.repeat([date_to_string(dt) for dt in pd.DatetimeIndex(ndvi.time.values)], len(cl)), 
					  columns = ['ID', 'NPix', 'Mean', 'Stdev', 
								 'Min', 'P10', 'P20', 'P30', 'P40', 'P50', 'P60', 'P70', 'P80', 'P90', 'Max'])
	df.index.name = 'Name'

	df['ID'] = np.tile(cl, len(ndvi.time.values))

	for c in cl:
		ndvi_cl = ndvi.where(clusters == c)
		df.loc[df.ID == c, 'NPix'] = np.count_nonzero(~np.isnan(ndvi_cl[0]))
		df.loc[df.ID == c, 'Mean'] = ndvi_cl.mean(['longitude', 'latitude']).round(2)
		df.loc[df.ID == c, 'Stdev'] = ndvi_cl.std(['longitude', 'latitude']).round(2)
		df.loc[df.ID == c, df.columns[4:15]] = ndvi_cl.quantile([0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1], dim = ['longitude', 'latitude']).T
	
	return df


def produce_plot1(diff_avg, lr_avg, diff_max, lr_max, year, ylim, output_path):
	'''
	Produces the plot1
	'''

	plt.figure(figsize=(15,5))
	plt.grid(axis = 'y')

	plt.plot(diff_avg.year, diff_avg, c = 'royalblue')
	plt.plot(diff_avg.year, lr_avg.predict(diff_avg.year.values.reshape(-1,1)), c = 'royalblue', ls = '--')
	plt.plot(diff_max.year, diff_max, c = 'darkorange')
	plt.plot(diff_max.year, lr_max.predict(diff_max.year.values.reshape(-1,1)), c = 'darkorange', ls = '--')

	plt.xticks(diff_max.year, [y for y in diff_max.year.values])
	if ylim != False:
		plt.ylim(ylim[0], ylim[1])
	plt.legend(['Average difference per year', 'Linear (Average difference per year)', 
				'Max of difference per year', 'Linear (Max of difference per year)'])
	plt.axvline(year, c = 'black', ls = '--')

	plt.savefig(output_path)


def produce_plot2(anom, output_path):
	'''
	Produces the plot2
	'''

	plt.figure(figsize=(15,5))
	plt.grid(axis = 'y')

	plt.plot(anom.time, anom, c = 'green')
	plt.axhline(1, c = 'red', ls = '--', linewidth = 2)
	plt.axhline(1.1, c = 'black', ls = '--', linewidth = 1)
	plt.axhline(0.9, c = 'black', ls = '--', linewidth = 1)
	plt.plot(anom.time, anom, c = 'green')

	plt.xticks(anom.time.values, 
			   [calendar.month_name[t.dt.month.values][:3] + '\n' + str(t.dt.year.values) for t in anom.time])
	ymax = anom.max() + 0.1 if anom.max() > 1.5 else 1.5
	ymin = anom.min() - 0.1 if anom.min() < 0.7 else 0.7
	plt.ylim([ymin, ymax])
	plt.yticks(plt.yticks()[0], ['{:,.0%}'.format(x) for x in plt.yticks()[0]])
	plt.legend(['NDVI Anomaly', 'Long-term Average'])

	plt.savefig(output_path)

def produce_plot3(avg, lta, output_path):
	'''
	Produces the plot3
	'''

	plt.figure(figsize=(15,5))
	plt.grid(axis = 'y')

	plt.plot(avg.time, lta/10000, c = 'red', ls = '--', linewidth = 2)
	plt.plot(avg.time, avg/10000, c = 'yellowgreen')

	plt.xticks(avg.time.values, 
			   [calendar.month_name[t.dt.month.values][:3] + '\n' + str(t.dt.year.values) for t in avg.time])
	ymax = 0.9 if avg.max() > 0.8 else 0.8
	ymin = 0
	plt.ylim([ymin, ymax])
	plt.legend(['Long-term Average NDVI','Average NDVI'])

	plt.savefig(output_path)


def main(buffersize: int):

	# Creating output folders
	output_directory = 'output/LCI/'
	pathlib.Path(output_directory + 'csv').mkdir(parents=True, exist_ok=True) 
	pathlib.Path(output_directory + 'plots').mkdir(parents=True, exist_ok=True) 
	pathlib.Path(output_directory + 'elbow_method').mkdir(parents=True, exist_ok=True) 
	pathlib.Path(output_directory + 'unprocessed_assets').mkdir(parents=True, exist_ok=True) 

	# Initialise lists to store kopt, time series and IDs
	kopts = []
	plot1 = []
	plot2 = []
	plot3 = []
	IDs_all = []
	IDs = []
	bad_assets = []


	####################
	# Downloading Data #
	####################

	# rasters
	path_to_zarr = 'data/Rasters/zarr_data/'
	da = xr.open_zarr(path_to_zarr + 'NDVI.zarr').band

	# shapefiles
	path_to_shp = 'data/Shapefiles/*.shp'
	shapefiles = glob.glob(path_to_shp)
	IDs_all = [os.path.basename(shapefile)[:-4] for shapefile in shapefiles]

	# seasonal calendar
	path_to_sea_can = 'data/Dataframes/seasonal_calendar.csv'
	sea_can = pd.read_csv(path_to_sea_can, index_col = 0)

	# asset date
	path_to_asset_csv = 'data/Dataframes/asset_info.csv'
	asset_csv = read_asset_csv_lci(path_to_asset_csv)


	########################
	# Computing indicators #
	########################

	# Max difference CSV
	max_diff = pd.DataFrame(index = IDs_all, columns = ['difference', 'absolute difference (%)'])


	for i,shapefile in enumerate(shapefiles):

		ID = IDs_all[i]
		iso3 = ID[:3]

		print('-- Processing asset ' + ID + ' (' + str(i + 1) + '/' + str(len(shapefiles)) + ') --')

		# Separate pre and post intervention periods
		year, month = asset_csv.loc[ID][['year', 'month']]
		pre = da.sel(time = da.time[da.time.values < pd.to_datetime(date(year,month,1))])
		post = da.sel(time = da.time[da.time.values >= pd.to_datetime(date(year,month,1))])

		# Reading asset
		gdf = gpd.read_file(shapefile)

		# Check asset size        
		if not check_asset_size(da, gdf):
			print('The asset is too small to be processed')
			bad_assets.append(ID)
			clusters.rio.to_raster(output_directory + 'unprocessed_assets/' + ID + '_clusters.tif')
			continue

		# Converting CRS from degree to km
		gdf = gdf.to_crs("EPSG:32635")

		# buffer around asset (default = 15km)
		gdf_buf = gdf.buffer(buffersize * 1000)

		# Clip rasters
		da_clipped = da.rio.clip(gdf_buf.geometry.values, gdf_buf.crs)
		pre_clipped = pre.rio.clip(gdf_buf.geometry.values, gdf_buf.crs)


		# Load Dask arrays
		da_clipped.load()
		pre_clipped.load()


		# Clustering
		pre_clipped_avg = pre_clipped.mean('time', keep_attrs = True)
		X = pre_clipped_avg.values
		X_ = X[~np.isnan(X)]
		X_ = X_.reshape(-1,1)
		k = elbow_method(X_, output_directory + 'elbow_method/' + ID + '.png') 
		kopts.append(k)
		kmeans = KMeans(n_clusters = k).fit(X_)
		clusters_np = pre_clipped_avg.values
		clusters_np[~np.isnan(pre_clipped_avg.values)] = kmeans.labels_
		clusters = pre_clipped_avg.copy()
		clusters.values = clusters_np

		# Exclude clusters that don't fall in asset
		clusters_asset = clusters.rio.clip(gdf.geometry.values, gdf.crs)
		m = clusters_asset.values
		unique, counts = np.unique(m[~np.isnan(m)], return_counts=True)
		proportion = counts / counts.sum()
		cl = unique[proportion > 0.1]

		# Asset site
		asset_site = clusters.rio.clip(gdf.geometry.values, gdf.crs, drop = False)
		asset_site = np.isin(asset_site.values, cl)

		# Control site
		control_site = clusters.rio.clip(gdf.geometry.values, gdf.crs, drop = False, invert = True)
		control_site = np.isin(control_site.values, cl)

		# CSV EXTRACTION

		IDs.append(ID)

		# Data Extraction asset site
		ndvi_asset = da_clipped.where(asset_site)
		df_asset = produce_csv(ndvi_asset, clusters, cl)
		df_name = iso3 + 'vim' + ID[4:].replace('_', '') + '.csv'
		df_asset.to_csv(output_directory + 'csv/' + df_name)

		# Data Extraction control site
		ndvi_control = da_clipped.where(control_site)
		df_control = produce_csv(ndvi_control, clusters, cl)
		df_name = iso3 + 'vim' + ID[4:].replace('_', '') + '_ControlSites.csv'
		df_control.to_csv(output_directory + 'csv/' + df_name)


		# PLOT1

		# Monthly difference
		if (ndvi_asset.time.dt.day.values[-1] == 10): # Only one dek for last month
			ndvi_asset = ndvi_asset[:-1]
			ndvi_control = ndvi_control[:-1]
		ndvi_asset_month = ndvi_asset.resample(time="M").mean()
		ndvi_control_month = ndvi_control.resample(time="M").mean()
		diff_month = ndvi_asset_month.mean(['longitude', 'latitude']) - ndvi_control_month.mean(['longitude', 'latitude'])

		# Mean difference per year
		diff_avg = diff_month.groupby('time.year').mean('time') / 10000
		lr_avg = LinearRegression().fit(diff_avg.year.values.reshape(-1,1),diff_avg.values.reshape(-1,1))

		# Max difference per year
		diff_max = diff_month.groupby('time.year').max('time') / 10000
		lr_max = LinearRegression().fit(diff_max.year.values.reshape(-1,1),diff_max.values.reshape(-1,1))

		# saving result
		plot1.append([diff_avg, lr_avg, diff_max, lr_max, year])


		# PLOT2

		# Average mean NDVI 
		begin_season = int(sea_can.loc[iso3.upper()].values)

		l = list(ndvi_asset_month.time.dt.month.values[:-11])
		l.reverse()
		season_date = pd.to_datetime(ndvi_asset_month.time.values[len(l) - l.index(begin_season) - 1])
		avg = ndvi_asset_month.sel(time = ndvi_asset_month.time[ndvi_asset_month.time.values >= season_date])[0:12].mean(['longitude', 'latitude'])

		# LTA
		ndvi_asset_month_pre = ndvi_asset_month.sel(time = ndvi_asset_month.time[ndvi_asset_month.time.values < pd.to_datetime(date(year,month,1))])
		lta = ndvi_asset_month_pre.groupby('time.month').mean(['time', 'longitude', 'latitude'])
		lta = xr.concat([lta[begin_season - 1:], lta[:begin_season - 1]], dim = 'month')

		# Anomaly
		anom = avg.copy()
		anom.values = avg.values / lta.values

		# saving result
		plot2.append(anom)


		# PLOT 3

		# saving result
		plot3.append([avg, lta])


		# MAX DIFF
		max_diff.loc[ID] = [round(max(avg.values - lta.values)/10000,2), round(max(avg.values/lta.values - 1),2)*100]



	# Saving Kopts
	df_kopt = pd.DataFrame(kopts, index = IDs_all, columns = ['K optimal'])
	df_kopt.to_csv(output_directory + 'elbow_method/' + 'Kopt.csv')

	# Saving bad assets
	df_badassets = pd.DataFrame(bad_assets)
	df_badassets.to_csv(output_directory + 'unprocessed_assets/list.csv', header = False, index = False)

	# Saving max diff
	max_diff.to_csv(output_directory + 'csv/max_diff.csv')


	# Getting ylim for plot
	try:
		ymax = max([plt1[2].max() for plt1 in plot1]) + 0.1
		ymin = min([plt1[0].min() for plt1 in plot1]) - 0.1
		ylim = [ymin, ymax]	
	except:
		ylim = False

	# Producing plots
	for plt1, plt2, plt3, ID in zip(plot1, plot2, plot3, IDs):

		plot1_name = output_directory + 'plots/' + ID + '._plot1.png'
		produce_plot1(plt1[0], plt1[1], plt1[2], plt1[3], plt1[4], ylim, plot1_name)

		plot2_name = output_directory + 'plots/' + ID + '._plot2.png'
		produce_plot2(plt2, plot2_name)

		plot3_name = output_directory + 'plots/' + ID + '._plot3.png'
		produce_plot3(plt3[0], plt3[1], plot3_name)


if __name__ == '__main__':

	# Instantiate the parser
	parser = argparse.ArgumentParser(description='LCI processing')

	# Flags
	parser.add_argument("--buffersize", type = int, default = 15, help = "Input buffer size (in km) around the assets")

	# Parse
	args = parser.parse_args()

	main(args.buffersize)













