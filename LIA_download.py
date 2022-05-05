import argparse
import pandas as pd
import pathlib
import s3fs
import xarray as xr
import zarr

from helper_fns import delete_directory, ODC_to_disk, read_ODC



def main(check_dates:bool):

	# Set paths
	path_to_ODC_url = 'data/Rasters/ODC_url.csv'
	path_zarr = 'data/Rasters/zarr_data/'


	# Rasters
	ODC_url = pd.read_csv(path_to_ODC_url, index_col = 0, header = None)
	ODC_url.rename(columns = {1: 'url'}, inplace = True)
    
    
    # Check dates of the datasets if --check_dates
	if check_dates:

		print('   --- Check dates of the datasets ---\n')

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

		delete_directory(path_zarr)
		pathlib.Path(path_zarr).mkdir(parents=True, exist_ok=True)

		print('\n---------------------------------------------\n')

		NDVI
		print('   --- Downloading NDVI Data ---')
		name = path_zarr + 'NDVI.zarr'
		ODC_to_disk(ODC_url.loc['NDVI']['url'], name)

		# LST
		print('   --- Downloading LST Data ---')
		name = path_zarr + 'LST.zarr'
		ODC_to_disk(ODC_url.loc['LST']['url'], name)

		# CHIRPS
		print('   --- Downloading CHIRPS Data ---')
		name = path_zarr + 'CHIRPS.zarr'
		ODC_to_disk(ODC_url.loc['CHIRPS']['url'], name)

		print('\n---------------------------------------------')





if __name__ == '__main__':

	# Instantiate the parser
	parser = argparse.ArgumentParser(description='LIA Data download')

	# Flags
	parser.add_argument('--check_dates', action='store_true', help='Check available dates for each dataset')

	# Parse
	args = parser.parse_args()

	main(args.check_dates)