# regional average monthly rainfall and max NDVI values during year, irrigation crops

import csv
import pathlib
import pandas as pd

import warnings
warnings.filterwarnings("ignore")


def run(da_chirps, da_ndvi, shapefiles: list, wet_season: list, dry_season: list, asset_info: pd.DataFrame, path_output: str):

    # Create output folder
	folder_name = path_output + '/' + product_type + '_rfh'
	pathlib.Path(folder_name).mkdir(parents=True, exist_ok=True)
    
    # Create max_ndvi.csv
    with open(folder_name + '/' + 'max_ndvi.csv', 'w', encoding='UTF8') as f:
        # create the csv writer
        writer_ndvi = csv.writer(f)
        # write first row to the csv file
        writer_ndvi.writerow(CHIRPS.time.values)
        
    # Create mean_chirps.csv
    with open(folder_name + '/' + 'mean_chirps.csv', 'w', encoding='UTF8') as f:
        # create the csv writer
        writer_chirps = csv.writer(f)
        # write first row to the csv file
        writer_chirps.writerow(CHIRPS.time.values)

    
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

		# Load da values
		da_chirps_clipped.load()
        da_ndvi_clipped.load()

		# Get intervention dates - A VOIR S'IL FAUT GARDER
		start_intervention = asset_info.loc[ID].start
		end_intervention = asset_info.loc[ID].end

		# Get comparison dates - A VOIR S'IL FAUT GARDER
		(pre_wet, post_wet, pre_dry, post_dry) = get_pre_post_dates(start_intervention, end_intervention, wet_season, dry_season)


        # PAS VERIFIE ET A PEU PRES - Maximum NDVI values per month
        max_ndvi = da_ndvi_clipped.groupby('month').max()
 		writer.writerow(max_ndvi)
         
        # PAS VERIFIE ET A PEU PRES - Mean CHIRPS values per month
        mean_chirps = da_chirps_clipped.groupby('month').mean()
 		writer.writerow(mean_chirps)
        
        # PAS VERIFIE ET A PEU PRES - Plot regional average monthly rainfall & NDVI
        name = ID + '_monthly_rainfall.png'
        plt.figure()
        plt.bar(range(1,13), mean_chirps, label = 'Average monthly rainfall')
        plt.bar(range(1,13), max_ndvi, 'o-', label = 'Maximum average NDVI')
        plt.legend()
        plt.savefig(name)
