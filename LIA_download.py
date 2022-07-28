#import zarr
#import s3fs
#import pathlib
import argparse
#import xarray as xr
#import pandas as pd
from tkinter import *

from utils_download import modis_dl as md
from utils_download import landsat_sentinel_dl as lsd


def main(check_dates: bool, select: bool, satellite: bool):

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

		sat = [0,1]


	if sat[0]:

		md.modis_dl(check_dates, run)

	if sat[1]:

		lsd.landsat_sentinel_dl(check_dates)

		if not sat[0]:

			md.modis_dl(check_dates, [0, 0, run[2]])





if __name__ == '__main__':

	# Instantiate the parser
	parser = argparse.ArgumentParser(description='LIA Data download')

	# Flags
	parser.add_argument('--check_dates', action='store_true', help='Check available dates for each dataset')
	parser.add_argument('--select', action='store_true', help='Select datasets to download')
	parser.add_argument('--satellite', action='store_true', help='Select satellite to download')

	# Parse
	args = parser.parse_args()

	main(args.check_dates, args.select, args.satellite)