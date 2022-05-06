import argparse
import pandas as pd
import pathlib
import s3fs
from tkinter import *
import xarray as xr
import zarr

from utils.helper_fns import delete_directory, ODC_to_disk, read_ODC



def main(check_dates:bool, select: bool):

	# Checkbox
	if select:

		a = Tk()
		a.title('Select')

		positionRight = int(a.winfo_screenwidth()/2 - a.winfo_reqwidth()/2)
		positionDown = int(a.winfo_screenheight()/2 - a.winfo_reqheight()/2)
		a.geometry("+{}+{}".format(positionRight, positionDown))

		var1 = IntVar()
		Checkbutton(a, text = "NDVI.zarr", variable = var1).grid(row = 0, sticky = W)
		var2 = IntVar()
		Checkbutton(a, text = "LST.zarr", variable = var2).grid(row = 1, sticky = W)
		var3 = IntVar()
		Checkbutton(a, text = "CHIRPS.zarr", variable = var3).grid(row = 2, sticky = W)

		Button(a, text = 'Download', command = a.destroy).grid(row = 3)
		a.mainloop()

		run = [var1.get(), var2.get(), var3.get()]

	else:

		run = [1, 1, 1]

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

		# NDVI
		if run[0] == 1:
			print('   --- Downloading NDVI Data ---')
			name = path_zarr + 'NDVI.zarr'
			ODC_to_disk(ODC_url.loc['NDVI']['url'], name)

		# LST
		if run[1] == 1:
			print('   --- Downloading LST Data ---')
			name = path_zarr + 'LST.zarr'
			ODC_to_disk(ODC_url.loc['LST']['url'], name)

		# CHIRPS
		if run[2] == 1:
			print('   --- Downloading CHIRPS Data ---')
			name = path_zarr + 'CHIRPS.zarr'
			ODC_to_disk(ODC_url.loc['CHIRPS']['url'], name)

		print('\n---------------------------------------------')





if __name__ == '__main__':

	# Instantiate the parser
	parser = argparse.ArgumentParser(description='LIA Data download')

	# Flags
	parser.add_argument('--check_dates', action='store_true', help='Check available dates for each dataset')
	parser.add_argument('--select', action='store_true', help='Select datasets to download')

	# Parse
	args = parser.parse_args()

	main(args.check_dates, args.select)