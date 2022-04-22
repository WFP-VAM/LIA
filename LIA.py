import pre_post
import rfh_ndvi
import expansion_ndvi
import enso
from helper_fns import *

def main():

    # Set paths
    path_to_shapefile = 'data/Shapefiles/'
    path_to_asset_info = 'data/Dataframes/asset_info.csv'
    path_to_country_info = 'data/Dataframes/country_info.csv'
    path_to_enso = 'data/Dataframes/ENSO.csv'
    path_to_ODC_url = 'data/Rasters/ODC_url.csv'

	# Delete outputs
    path_output = './outputs'
    delete_directory(path_output)

	# DOWNLOAD DATA

    # Shapefiles
    shapefiles = glob.glob(path_to_shapefile + '*.shp')

	# CSV
    asset_info = read_asset_csv(path_to_asset_info)
    country_info = pd.read_csv(path_to_country_info, index_col = 1, header = 0, encoding='ISO-8859-1')
    ENSO = pd.read_csv(path_to_enso, index_col = 0, header = 0, sep = ';', encoding='utf8')
    
	# Rasters - NDVI
    ODC_url = pd.read_csv(path_to_ODC_url, index_col = 0, header = None)
    ODC_url.rename(columns = {1: 'url'}, inplace = True)
    NDVI = read_ODC(ODC_url.loc['NDVI']['url'])
    LST = read_ODC(ODC_url.loc['LST']['url'])
    CHIRPS = read_ODC(ODC_url.loc['CHIRPS']['url'])

    # Get Country information 
    iso3 = os.path.basename(shapefiles[0])[:3]
    c_info = country_info.loc[iso3]
    (wet_season, dry_season) = get_wet_dry(c_info)

	# PROCESSING
    print(' ## NDVI pre/post implementation ##')
    #pre_post.run(NDVI, shapefiles, wet_season, dry_season, asset_info, path_output, 'NDVI')
    print(' ## max NDVI pre/post implementation ##')
    #pre_post.run(NDVI, shapefiles, wet_season, dry_season, asset_info, path_output, 'maxNDVI')
    print(' ## LST pre/post implementation ##')
    #pre_post.run(LST, shapefiles, wet_season, dry_season, asset_info, path_output, 'LST')
    print(' ## Rainfall & max NDVI ##')
    #rfh_ndvi.run(CHIRPS, NDVI, shapefiles, wet_season, dry_season, asset_info, path_output)
    print(' ## Expansion NDVI ##')
    #expansion_ndvi.run(NDVI, shapefiles, wet_season, dry_season, asset_info, path_output)
    print(' ## ENSO analysis ##')
    enso.run(CHIRPS,shapefiles, wet_season, dry_season, ENSO, path_output, n_years=5)

if __name__ == '__main__':

    main()



