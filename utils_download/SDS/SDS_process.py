# load modules
import os
import pdb
import glob
import numpy as np
import xarray as xr
import pandas as pd
from tqdm import tqdm
from osgeo import gdal, osr
import matplotlib.pyplot as plt
import matplotlib.colors as colors

# image processing modules
import skimage.filters as filters
import skimage.measure as measure
import skimage.morphology as morphology

# other modules
import pickle
from pylab import ginput
import matplotlib.cm as cm
from datetime import date
from datetime import datetime
from matplotlib import gridspec
import matplotlib.lines as mlines
import matplotlib.patches as mpatches


# Modules
from utils_download.SDS import SDS_tools, SDS_preprocess

np.seterr(all='ignore') # raise/ignore divisions by 0 and nans


def get_ndvi_lst(metadata, settings):
    
    """
    Reads the metadata of all Landsat and Sentinel Surface Reflectance images to compute the NDVI from the multispectral     bands and save the NDVI and LST as tiff and zarr files

    Arguments:
    -----------
    metadata: dict
        contains all the information about the satellite images that were downloaded
    settings: dict with the following keys
        'inputs': dict
                'sitename': str
                name of the site
                'polygon': list
                    polygon containing the lon/lat coordinates to be extracted,
                    longitudes in the first column and latitudes in the second column,
                    there are 5 pairs of lat/lon with the fifth point equal to the first point:
                    ```
                    polygon = [[[151.3, -33.7],[151.4, -33.7],[151.4, -33.8],[151.3, -33.8],
                    [151.3, -33.7]]]
                    ```
                'dates': list of str
                    list that contains 2 strings with the initial and final dates in 
                    format 'yyyy-mm-dd':
                    ```
                    dates = ['1987-01-01', '2018-01-01']
                    ```
                'sat_list': list of str
                    list that contains the names of the satellite missions to include: 
                    ```
                    sat_list = ['L5', 'L7', 'L8', 'L9' or 'S2']
                    ```
                'filepath_data': str
                    filepath to the directory where the images are downloaded
                    
        'cloud_thresh': float
            value between 0 and 1 indicating the maximum cloud fraction in 
            the cropped image that is accepted
        'cloud_mask_issue': boolean
            True if there is an issue with the cloud mask and sand pixels
            are erroneously being masked on the images
        

    Returns:
    -----------
    Save NDVI and LST as tif and zarr files

    """
    
    sitename = settings['inputs']['sitename']
    filepath_data = settings['inputs']['filepath']
    
    # loop through satellite list
    for satname in metadata.keys():
        
        # create a subfolder to store the NDVI images
        filepath_ndvi = os.path.join(filepath_data, sitename, satname, 'NDVI')
        if not os.path.exists(filepath_ndvi):
            os.makedirs(filepath_ndvi)
            
        if satname in ['L5', 'L7', 'L8', 'L9']:
            # create a subfolder to store the LST images
            filepath_lst = os.path.join(filepath_data, sitename, satname, 'LST')
            if not os.path.exists(filepath_lst):
                os.makedirs(filepath_lst)

        # get images
        filepath = SDS_tools.get_filepath(settings['inputs'],satname)       
        filenames = metadata[satname]['filenames']
        
        #print('')
        print(satname,': \nSaving as tif files')
        
         # loop through the images
        for i in range(len(filenames)):
            print('\r%d%%' % (int(((i+1)/len(filenames))*100)), end='')
            #print('\r%s: Saving as tif files  %d%%' % (satname,int(((i+1)/len(filenames))*100)), end='')

            # get image filename
            fn = SDS_tools.get_filenames(filenames[i],filepath, satname)
                
            # preprocess image (cloud mask)
            im_ms, georef, cloud_mask, im_QA, im_nodata = SDS_preprocess.preprocess_single(fn, satname, settings['cloud_mask_issue'])
            # get image spatial reference system (epsg code) from metadata dict
            image_epsg = metadata[satname]['epsg'][i]
            # compute cloud_cover percentage (with no data pixels)
            cloud_cover_combined = np.divide(sum(sum(cloud_mask.astype(int))),
                                    (cloud_mask.shape[0]*cloud_mask.shape[1]))
            if cloud_cover_combined > 0.99: # if 99% of cloudy pixels in image skip
                continue
            # remove no data pixels from the cloud mask 
            # (for example L7 bands of no data should not be accounted for)
            cloud_mask_adv = np.logical_xor(cloud_mask, im_nodata) 
            # compute updated cloud cover percentage (without no data pixels)
            cloud_cover = np.divide(sum(sum(cloud_mask_adv.astype(int))),
                                    (sum(sum((~im_nodata).astype(int)))))
            # skip image if cloud cover is above user-defined threshold
            if cloud_cover > settings['cloud_thresh']:
                continue
            
            # Compute and save NDVI and LST ##
            if satname in ['S2']:
                dataset = gdal.Open(fn[0])
            else:
                dataset = gdal.Open(fn)
            
            # Un-scale Surface Reflectance to compute NDVI
            if satname in ['S2']:
                coef = 1e-4
                offset = 0.0
            else:
                coef = 2.75e-05
                offset = -0.2
            im_nir = coef * im_ms[:,:,3] + offset
            im_red = coef * im_ms[:,:,2] + offset
            
            # Compute NDVI                
            ndvi = SDS_tools.nd_index(im_nir, im_red, cloud_mask)

            # Get projection
            prj = dataset.GetProjection()
            geotransform = dataset.GetGeoTransform()
            
            output_file_ndvi = os.path.join(filepath_ndvi,filenames[i])
            
            # Create gtif file with rows and columns from parent raster 
            band = dataset.GetRasterBand(1)
            driver = gdal.GetDriverByName("GTiff")
            columns, rows = (band.XSize, band.YSize)
            
            ndvi_ds = driver.Create(output_file_ndvi, 
                                    columns, 
                                    rows, 
                                    1, 
                                    gdal.GDT_Int32)

            ##writting output raster
            ndvi_ds.GetRasterBand(1).WriteArray(ndvi*10000)

            #setting extension of output raster
            # top left x, w-e pixel resolution, rotation, top left y, rotation, n-s pixel resolution
            
            ndvi_ds.SetGeoTransform(geotransform)

            # setting spatial reference of output raster 
            srs = osr.SpatialReference(wkt = prj)
            ndvi_ds.SetProjection( srs.ExportToWkt() )
            
            #Close output raster dataset 
            ndvi_ds = None
            
            # Save LST for Landsat
            if satname in ['L5', 'L7', 'L8', 'L9']:
                lst = im_ms[:,:,5]
                output_file_lst = os.path.join(filepath_lst,filenames[i])
                lst_ds = driver.Create(output_file_lst, 
                                    columns, 
                                    rows, 
                                    1, 
                                    band.DataType)
                lst_ds.GetRasterBand(1).WriteArray( lst )
                lst_ds.SetGeoTransform(geotransform)
                lst_ds.SetProjection( srs.ExportToWkt() )
                lst_ds = None

            #Close main raster dataset
            dataset = None
            
            
        ### Save every datasets as zarr files ###
        print('\nSaving NDVI as Zarr')
        # filepath to store the NDVI zarr file
        filepath_ndvi_zarr = os.path.join(filepath_data, sitename, satname, 'NDVI_zarr')                

        # Get list of NDVI .tif files
        files = glob.glob(filepath_ndvi + "/*.tif")

        # Create Xarray dataset
        da = create_xarray(files)
        
        # Convert Floats to Integers
        da['band'] = da.band.astype('int32')
        
        # Add epsg to the dataset
        da = da.assign_coords(spatial_ref = metadata[satname]['epsg'][0])
        da = da.assign_attrs(crs = 'epsg:'+str(metadata[satname]['epsg'][0]), grid_mapping = 'spatial_ref')
        
        # Save as a zarr file
        da.to_zarr(filepath_ndvi_zarr, mode='a')
         
#        # Do the same for LST
#        if satname in ['L5', 'L7', 'L8', 'L9']:
#            print('\nSaving LST as Zarr')
#            # filepath to store the LST zarr file
#            filepath_lst_zarr = os.path.join(filepath_data, sitename, satname, 'LST_zarr')
#            
#            # Get list of NDVI .tif files
#            files = glob.glob(filepath_lst + "/*.tif")
#
#            # Create Xarray dataset
#            da = create_xarray(files)
#
#            # Convert Floats to Integers
#            da['band'] = da.band.astype('int32')
#            
#            # Add epsg to the dataset
#            da = da.assign_coords(spatial_ref = metadata[satname]['epsg'][0])
#            da = da.assign_attrs(crs = 'epsg:'+str(metadata[satname]['epsg'][0]), grid_mapping = 'spatial_ref')
#
#            # Save as a zarr file
#            da.to_zarr(filepath_lst_zarr, mode='a')


            
def filename_to_date(x: str):
    '''
    Converts file name to a gregorian date 
    '''  
    x = os.path.basename(x)
    # getting year, month and dekad
    year = int(x[:4])
    month = int(x[5:7])
    day = int(x[8:10])
    
    # Converting to gregorian date
    greg_date = date(year,month,day)
        
    return greg_date


def create_xarray(files: list):
    '''
    Concatenate all rasters (tiff) into an Xarray
    '''
    
    # Download first raster
    da = xr.open_rasterio(files[0])
    
    # Concatenate all rasters along time
    for file in tqdm(files[1:]):
        raster = xr.open_rasterio(file)
        da = xr.concat([da, raster], dim = 'time')
    
    # Get the timestamps from the filenames
    timestamps = [
            filename_to_date(file)
            for file in files
        ]

    # Assign them to the time dimension:
    da = da.assign_coords(time=pd.to_datetime(timestamps))
    
    # Remove band dimension
    da = da.squeeze('band')
    da = da.drop_vars('band')
    da = da.rename({'y':'latitude', 'x':'longitude', 'time':'time'})
    
    return da.to_dataset(name='band')
       

            
        
