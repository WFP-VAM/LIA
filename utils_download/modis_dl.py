import pandas as pd

from utils.helper_fns import delete_directory, ODC_to_disk, read_ODC

import warnings
warnings.filterwarnings("ignore")


def modis_dl(check_dates, run):
    
	# Set paths
	path_to_ODC_url = 'data/Rasters/ODC_url.csv'
	path_zarr = 'data/Rasters/MODIS/zarr_data/'
	path_adm00 = 'data/ADM00/_2020_global_adm0.shp'

	# Rasters
	ODC_url = pd.read_csv(path_to_ODC_url, index_col = 0, header = None)
	ODC_url.rename(columns = {1: 'url'}, inplace = True) 
    
    # Check dates of the datasets if --check_dates
	if check_dates:

		if (not run[0] and not run[1]) and run[2]:
			print('   --- \n Check dates of CHIRPS ---\n')
		else:
			print('   --- Check dates of the datasets ---\n')

		if run[0] or run[1]:

			# print NDVI dates
			NDVI = read_ODC(ODC_url.loc['NDVI']['url'])
			start_date = str(pd.to_datetime(NDVI.time[0].values).date())
			end_date = str(pd.to_datetime(NDVI.time[-1].values).date())
			print('NDVI dates range: ' + start_date + '...' + end_date)

			# print LST dates
			LST = read_ODC(ODC_url.loc['LST']['url'])
			start_date = str(pd.to_datetime(LST.time[0].values).date())
			end_date = str(pd.to_datetime(LST.time[-1].values).date())
			print('LST dates range: ' + start_date + '...' + end_date)

		# print CHIRPS data
		CHIRPS = read_ODC(ODC_url.loc['CHIRPS']['url'])
		start_date = str(pd.to_datetime(CHIRPS.time[0].values).date())
		end_date = str(pd.to_datetime(CHIRPS.time[-1].values).date())
		print('CHIRPS dates range: ' + start_date + '...' + end_date)

		print('\n---------------------------------------------')

	else:

		print('\n---------------------------------------------\n')

		# NDVI
		if run[0] == 1:
			print('   --- Downloading NDVI Data ---')
			name = path_zarr + 'NDVI.zarr'
			ODC_to_disk(ODC_url.loc['NDVI']['url'], name, path_adm00)

		# LST
		if run[1] == 1:
			print('   --- Downloading LST Data ---')
			name = path_zarr + 'LST.zarr'
			ODC_to_disk(ODC_url.loc['LST']['url'], name, path_adm00)

		# CHIRPS
		if run[2] == 1:
			print('   --- Downloading CHIRPS Data ---')
			name = path_zarr + 'CHIRPS.zarr'
			ODC_to_disk(ODC_url.loc['CHIRPS']['url'], name, path_adm00)

		print('\n---------------------------------------------')