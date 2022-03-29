from helper_fns import *
import pre_post

def main():

	# Set paths
	path_to_shapefile = 'data/Shapefiles/'
	path_to_asset_info = 'data/Dataframes/asset_info.csv'
	path_to_country_info = 'data/Dataframes/country_info.csv'
	path_to_ODC_url = 'data/Rasters/ODC_url.csv'

	# Delete outputs
	path_output = 'outputs'
	delete_directory(path_output)


	# DOWNLOAD DATA

	# Shapefiles
	shapefiles = glob.glob(path_to_shapefile + '*.shp')

	# CSV
	asset_info = read_asset_csv(path_to_asset_info)
	country_info = pd.read_csv(path_to_country_info, index_col = 1, header = 0)

	# Rasters
	ODC_url = pd.read_csv(path_to_ODC_url, index_col = 0, header = None)
	ODC_url.rename(columns = {1: 'url'}, inplace = True)

	fs = s3fs.S3FileSystem(anon=True, client_kwargs={'region_name': 'eu-central-1'})
	NDVI = xr.open_zarr(s3fs.S3Map(ODC_url.loc['NDVI']['url'], s3=fs))
	NDVI = NDVI.assign_coords(time = pd.to_datetime([string_to_date(x) for x in NDVI.time.values]))
	NDVI = NDVI.band


	# Get Country information 
	iso3 = os.path.basename(shapefiles[0])[:3]
	c_info = country_info.loc[iso3]
	(wet_season, dry_season) = get_wet_dry(c_info)


	# PROCESSING

	pre_post.run(NDVI, shapefiles, wet_season, dry_season, asset_info, path_output, 'NDVI')


if __name__ == '__main__':

	main()



