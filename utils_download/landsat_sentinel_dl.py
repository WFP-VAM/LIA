import os
import glob
import pandas as pd
import xarray as xr
import geopandas as gpd

import seasmon_xr

from utils.helper_fns import check_asset_size
from utils_download.SDS import SDS_tools, SDS_download, SDS_process


def stack_landsat(path, nodata = -3000):
    
    # stack NDVI 
 	l5 = xr.open_zarr(path+'/L5/NDVI_zarr')
 	l7 = xr.open_zarr(path+'/L7/NDVI_zarr')
 	l8 = xr.open_zarr(path+'/L8/NDVI_zarr')
 	l9 = xr.open_zarr(path+'/L9/NDVI_zarr')
 	
 	l5 = l5.chunk(dict(time=-1))
 	l7 = l7.chunk(dict(time=-1))
 	l8 = l8.chunk(dict(time=-1))
 	l9 = l9.chunk(dict(time=-1))

 	pix_l5 = l5.groupby(l5.time.dt.strftime("%Y-%m-%d")).mean(dim='time')
 	pix_l7 = l7.groupby(l7.time.dt.strftime("%Y-%m-%d")).mean(dim='time')
 	pix_l8 = l8.groupby(l8.time.dt.strftime("%Y-%m-%d")).mean(dim='time')
 	pix_l9 = l9.groupby(l9.time.dt.strftime("%Y-%m-%d")).mean(dim='time')

 	lst = xr.concat([pix_l5, pix_l7, pix_l8, pix_l9], dim='satellite', join='outer')

 	lst = lst.mean(dim='satellite')
 	lst = lst.rename_dims({'strftime':'time'})
 	lst = lst.assign_coords(time=pd.to_datetime(lst.strftime.values)).drop('strftime') 

 	lst = lst.chunk(dict(time=-1))
    
 	lst_wcv = lst.band.hdc.whit.whitswcv(nodata = nodata, p = 0.8)

 	lst_wcv = lst_wcv.chunk(dict(time=-1,latitude=256,longitude=256))
 	lst_wcv_month = lst_wcv.groupby(lst_wcv.time.dt.strftime("%Y-%m")).mean(dim='time')
 	lst_wcv_month = lst_wcv_month.assign_coords(time=pd.to_datetime(lst_wcv_month.strftime.values))
 	lst_wcv_month = lst_wcv_month.drop('strftime') 
 	t = lst_wcv_month.time.values
 	lst_wcv_month = lst_wcv_month.band.rio.write_crs("epsg:32637", inplace=True)        
 	lst_wcv_month['strftime'] = t
 	lst_wcv_month = lst_wcv_month.rename({'strftime': 'time'})
 	lst_wcv_month = lst_wcv_month.to_dataset()
    
 	lst_wcv_month.to_zarr(path+'/NDVI_smoothed_monthly.zarr')
    
    # stack LST
 	l5 = xr.open_zarr(path+'/L5/LST_zarr')
 	l7 = xr.open_zarr(path+'/L7/LST_zarr')
 	l8 = xr.open_zarr(path+'/L8/LST_zarr')
 	l9 = xr.open_zarr(path+'/L9/LST_zarr')
	
 	l5 = l5.chunk(dict(time=-1))
 	l7 = l7.chunk(dict(time=-1))
 	l8 = l8.chunk(dict(time=-1))
 	l9 = l9.chunk(dict(time=-1))

 	pix_l5 = l5.groupby(l5.time.dt.strftime("%Y-%m-%d")).mean(dim='time')
 	pix_l7 = l7.groupby(l7.time.dt.strftime("%Y-%m-%d")).mean(dim='time')
 	pix_l8 = l8.groupby(l8.time.dt.strftime("%Y-%m-%d")).mean(dim='time')
 	pix_l9 = l9.groupby(l9.time.dt.strftime("%Y-%m-%d")).mean(dim='time')

 	lst = xr.concat([pix_l5, pix_l7, pix_l8, pix_l9], dim='satellite', join='outer')

 	lst = lst.mean(dim='satellite')
 	lst = lst.rename_dims({'strftime':'time'})
 	lst = lst.assign_coords(time=pd.to_datetime(lst.strftime.values)).drop('strftime') 

 	lst = lst.chunk(dict(time=-1))
    
 	lst_wcv = lst.band.hdc.whit.whitswcv(nodata = nodata, p = 0.8)

 	lst_wcv = lst_wcv.chunk(dict(time=-1,latitude=256,longitude=256))
 	lst_wcv_month = lst_wcv.groupby(lst_wcv.time.dt.strftime("%Y-%m")).mean(dim='time')
 	lst_wcv_month = lst_wcv_month.assign_coords(time=pd.to_datetime(lst_wcv_month.strftime.values))
 	lst_wcv_month = lst_wcv_month.drop('strftime') 
 	t = lst_wcv_month.time.values
 	lst_wcv_month = lst_wcv_month.band.rio.write_crs("epsg:32637", inplace=True)        
 	lst_wcv_month['strftime'] = t
 	lst_wcv_month = lst_wcv_month.rename({'strftime': 'time'})
 	lst_wcv_month = lst_wcv_month.to_dataset()
    
 	lst_wcv_month.to_zarr(path+'/LST_smoothed_monthly.zarr')
    

def landsat_sentinel_dl(check_dates): 

    # Set paths
	path_to_shapefile = 'data/Shapefiles/'

    # Shapefiles
	shapefiles = glob.glob(path_to_shapefile + '*.shp')

	for i,shapefile in enumerate(shapefiles):

		# Get asset ID
		ID = os.path.basename(shapefile)[:-4]

		# Reading asset
		gdf = gpd.read_file(shapefile)

		# 0.2 degree buffer around asset
		gdf_buf = gdf.buffer(0.1, cap_style = 3)

		# it's recommended to convert the polygon to the smallest rectangle (sides parallel to coordinate axes)       
		polygon = SDS_tools.smallest_rectangle(gdf_buf)

        # date range
		dates = ['2002-01-01', '2022-06-01']

        # satellite missions
		sat_list = ['L5','L7','L8','L9']

        # name of the site
		sitename = ID

        # directory where the data will be stored
		filepath = 'data/Rasters/LANDSAT_SENTINEL/'

        # put all the inputs into a dictionnary
		inputs = {'polygon': polygon, 'dates': dates, 'sat_list': sat_list, 'sitename': sitename, 'filepath':filepath}


		if check_dates:

			print('   --- Check available images of NDVI/LST  ---\n')
            # before downloading the images, check how many images are available for your inputs
			SDS_download.check_images_available(inputs)

		else:
			metadata = SDS_download.get_metadata(inputs)
#			metadata = SDS_download.retrieve_images(inputs)
			settings = { 
				# general parameters:
				'cloud_thresh': 0.5,        # threshold on maximum cloud cover
				'cloud_mask_issue': False,  # switch this parameter to True if pixels are masked (in black) on many images  
				# add the inputs defined previously
				'inputs': inputs
			}
                
#			SDS_process.get_ndvi_lst(metadata, settings)
            
			stack_landsat(filepath+ID)