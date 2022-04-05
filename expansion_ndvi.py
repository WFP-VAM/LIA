# regional average monthly rainfall and max NDVI values during year, irrigation crops

import csv
import pathlib
import pandas as pd

import warnings
warnings.filterwarnings("ignore")


def get_pre_dates(start_intervention, end_intervention, season):
    
    if start_intervention[0] < season[1]:
        end = (season[1], start_intervention[1] - 1)
    else:
        end = (season[1], start_intervention[1])
        
    if season[0] < season[1]:
        start = (season[0], end[1])
    else:
        start = (season[0], end[1] - 1)
        
    return [start, end]


def get_post_dates(start_intervention, end_intervention, season):
    
    if start_intervention[1] > season[0]:
        start = (season[0], end_intervention[1] + 1)
    else:
        start = (season[0], end_intervention[1])
        
    if season[0] < season[1]:
        end = (season[1], start[1])
    else:
        end = (season[1], start[1] + 1)
        
    return [start, end]


def get_pre_post_dates(start_intervention, end_intervention, wet_season, dry_season):
    
    pre_wet, post_wet = [], []
    pre_dry, post_dry = [], []
    
    for ws in wet_season:
        pre_wet.append(get_pre_dates(start_intervention, end_intervention, ws))
        post_wet.append(get_post_dates(start_intervention, end_intervention, ws))
    
    for ds in dry_season:
        pre_dry.append(get_pre_dates(start_intervention, end_intervention, ds))
        post_dry.append(get_post_dates(start_intervention, end_intervention, ds))
        
    return(pre_wet, post_wet, pre_dry, post_dry)


def date_to_str(x: tuple):
    
    return str(x[1]) + "{0:0=2d}".format(x[0])

def run(da, shapefiles: list, wet_season: list, dry_season: list, asset_info: pd.DataFrame, path_output: str):

    # Create output folder
	folder_name = path_output + '/' + 'NDVI' + '_expansion'
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
        da_clipped = da_ndvi.rio.clip(gdf.geometry.values, gdf.crs)

		# Load da values
        da_clipped.load()

		# Get intervention dates
		start_intervention = asset_info.loc[ID].start
		end_intervention = asset_info.loc[ID].end

		# Get comparison dates
		(pre_wet, post_wet, pre_dry, post_dry) = get_pre_post_dates(start_intervention, end_intervention, wet_season, dry_season)

        t = da_clipped.time.values
        
        for prew, postw in zip(pre_wet, post_wet):
		    
		    pre = da_clipped.sel(time = da_clipped.time[(t >= pd.to_datetime(date(prew[0][1], prew[0][0], 1))) & (t <= pd.to_datetime(date(prew[1][1], prew[1][0], 1)))])
		    post = da_clipped.sel(time = da_clipped.time[(t >= pd.to_datetime(date(postw[0][1], postw[0][0], 1))) & (t <= pd.to_datetime(date(postw[1][1], postw[1][0], 1)))])
		    
            exp_pre = pre.where(pre.band >= 0.5)
            print(exp_pre)
            exp_post = pre.where(pre.band >= 0.5)
        
		    
		    name_pre = ID + '_L_' + 'NDVI' + '_' + date_to_str(prew[0]) + '_' + date_to_str(prew[1]) + '_wet.tif'
		    exp_pre.rio.to_raster(folder_name + '/' + name_pre)
            ######### Modify above #############"
            name_post = 
		    
		    
		for pred, postd in zip(pre_dry, post_dry):
		    
		    pre = da_clipped.sel(time = da_clipped.time[(t >= pd.to_datetime(date(pred[0][1], pred[0][0], 1))) & (t <= pd.to_datetime(date(pred[1][1], pred[1][0], 1)))])
		    post = da_clipped.sel(time = da_clipped.time[(t >= pd.to_datetime(date(postd[0][1], postd[0][0], 1))) & (t <= pd.to_datetime(date(postd[1][1], postd[1][0], 1)))])
		    diff = post.mean(['time']) - pre.mean(['time'])

		    name = ID + '_L_' + product_type + '_' + date_to_str(pred[0]) + '_' + date_to_str(pred[1]) + '_' + date_to_str(postd[0]) + '_' + date_to_str(postd[1]) + '_dry.tif'
		    diff.rio.to_raster(folder_name + '/' + name)

        
        
        
        
        