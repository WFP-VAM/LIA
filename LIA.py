import os
import argparse
import xarray as xr
from tkinter import *
from utils.helper_fns import *
import utils.pre_post as pre_post
import utils.tci_vci_vhi as tci_vci_vhi
import utils.rfh_ndvi as rfh_ndvi
import utils.expansion_ndvi as expansion_ndvi
import utils.enso as enso



def main(select: bool, satellite: bool):

	# Checkbox
	if select:

		a = Tk()
		a.title('Select')

		positionRight = int(a.winfo_screenwidth()/2 - a.winfo_reqwidth()/2)
		positionDown = int(a.winfo_screenheight()/2 - a.winfo_reqheight()/2)
		a.geometry("+{}+{}".format(positionRight, positionDown))

		var1 = IntVar()
		Checkbutton(a, text = "NDVI pre/post implementation", variable = var1).grid(row = 0, sticky = W)
		var2 = IntVar()
		Checkbutton(a, text = "max NDVI pre/post implementation", variable = var2).grid(row = 1, sticky = W)
		var3 = IntVar()
		Checkbutton(a, text = "LST pre/post implementation", variable = var3).grid(row = 2, sticky = W)
		var4 = IntVar()
		Checkbutton(a, text = "TCI/VCI/VHI pre/post implementation", variable = var4).grid(row = 3, sticky = W)
		var5 = IntVar()
		Checkbutton(a, text = "Rainfall and max NDVI", variable = var5).grid(row = 4, sticky = W)
		var6 = IntVar()
		Checkbutton(a, text = "Expansion NDVI", variable = var6).grid(row = 5, sticky = W)
		var7 = IntVar()
		Checkbutton(a, text = "ENSO analysis", variable = var7).grid(row = 6, sticky = W)

		Button(a, text = 'Run', command = a.destroy).grid(row = 7)
		a.mainloop()

		run = [var1.get(), var2.get(), var3.get(), var4.get(), var5.get(), var6.get(), var7.get()]

	else:

		run = [1, 1, 1, 1, 1, 1, 1]


	# Satellite
	if satellite:

		a = Tk()
		a.title('Select')

		positionRight = int(a.winfo_screenwidth()/2 - a.winfo_reqwidth()/2)
		positionDown = int(a.winfo_screenheight()/2 - a.winfo_reqheight()/2)
		a.geometry("+{}+{}".format(positionRight, positionDown))

		var1 = IntVar()
		Checkbutton(a, text = "MODIS", variable = var1).grid(row = 0, sticky = W)
		var2 = IntVar()
		Checkbutton(a, text = "LANDSAT/SENTINEL", variable = var2).grid(row = 1, sticky = W)

		Button(a, text = 'Download', command = a.destroy).grid(row = 2)
		a.mainloop()

		sat = [var1.get(), var2.get()]

	else:

		sat = [1,0]


	# Set paths
	path_to_shapefile = 'data/Shapefiles/'
	path_to_asset_info = 'data/Dataframes/asset_info.csv'
	path_to_country_info = 'data/Dataframes/country_info.csv'
	path_to_enso = 'data/Dataframes/ENSO.csv'
	path_output = 'output/LIA/'
	path_to_zarr = 'data/Rasters/MODIS/zarr_data/'

	# DOWNLOAD DATA

	# Shapefiles
	shapefiles = glob.glob(path_to_shapefile + '*.shp')

	# CSV
	asset_info = read_asset_csv(path_to_asset_info)
	country_info = pd.read_csv(path_to_country_info, index_col = 1, header = 0, encoding='ISO-8859-1')
	ENSO = pd.read_csv(path_to_enso, index_col = 0, header = 0, sep = ';', encoding='utf8')

	# Get Country information 
	iso3 = os.path.basename(shapefiles[0])[:3]
	c_info = country_info.loc[iso3]
	(wet_season, dry_season) = get_wet_dry(c_info)
	alpha = float(c_info['VCI_alpha'])

	if sat[0]:
		# Rasters
		try:
			NDVI = xr.open_zarr(path_to_zarr + 'NDVI.zarr').band
			LST = xr.open_zarr(path_to_zarr + 'LST.zarr').band
			CHIRPS = xr.open_zarr(path_to_zarr + 'CHIRPS.zarr').band
		except:   
			print('You first need to download the datasets by running LIA_download.py')
			return None

	else:
		# Rasters
		try:
			NDVI = None
			LST = None
			CHIRPS = xr.open_zarr(path_to_zarr + 'CHIRPS.zarr').band
		except:   
			print('You first need to download the datasets by running LIA_download.py')
			return None

	# PROCESSING
	if run[0]:
		print('\n' + ' ## NDVI pre/post implementation ##')
		pre_post.run(NDVI, shapefiles, wet_season, dry_season, asset_info, path_output, 'NDVI')
	if run[1]:
		print('\n' + ' ## max NDVI pre/post implementation ##')
		pre_post.run(NDVI, shapefiles, wet_season, dry_season, asset_info, path_output, 'maxNDVI')
	if run[2]:
		print('\n' + ' ## LST pre/post implementation ##')
		pre_post.run(LST, shapefiles, wet_season, dry_season, asset_info, path_output, 'LST')
	if run[3]:
		print('\n' + ' ## TCI/VCI/VHI pre/post implementation ##')
		tci_vci_vhi.run(LST, NDVI, shapefiles, wet_season, dry_season, asset_info, path_output, alpha)
	if run[4]:
		print('\n' + '## Rainfall & max NDVI ##')
		rfh_ndvi.run(CHIRPS, NDVI, sat, shapefiles, wet_season, dry_season, asset_info, path_output)
	if run[5]:
		print('\n' + ' ## Expansion NDVI ##')
		expansion_ndvi.run(NDVI, shapefiles, wet_season, dry_season, asset_info, path_output)
	if run[6]:
		print('\n' + ' ## ENSO analysis ##')
		enso.run(CHIRPS, shapefiles, wet_season, dry_season, ENSO, path_output, n_years=5)



if __name__ == '__main__':

	# Instantiate the parser
	parser = argparse.ArgumentParser(description='LIA processing')

	# Flags
	parser.add_argument('--select', action='store_true', help='Select analysis')
	parser.add_argument('--satellite', action='store_true', help='Select analysis')

	# Parse
	args = parser.parse_args()

	main(args.select, args.satellite)



