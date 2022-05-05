# ENSO relationships 

import warnings
warnings.filterwarnings("ignore")

import os
import csv
import pathlib
import pandas as pd
from numpy import isnan
import geopandas as gpd
from datetime import date
from utils.helper_fns import delete_directory, check_asset_size


def get_dates(current_year, n_years, dry_season, wet_season, chirps_last, enso_last):

    wet_1 = wet_season[0]
    dry = dry_season[0]
    dates = []
    seasons = []
    for year in range(current_year - n_years, current_year):
        # First wet season
        if wet_1[0] < wet_1[1]:
            dates.append([(wet_1[0], year), (wet_1[1], year)])
        else:
            dates.append([(wet_1[0], year-1), (wet_1[1], year)])
        seasons.append('wet_1')
        if len(wet_season) == 1:
            # No second wet season
            # Dry season
            if dry[0] < dry[1]:
                dates.append([(dry[0], year), (dry[1], year)])
            else:
                dates.append([(dry[0], year), (dry[1], year+1)])
            seasons.append('dry')

        else:
            # Presence of a second wet period
            wet_2 = wet_season[1]
            # Check which season appears first dry or wet_2
            # Dry first
            if dry[0] < wet_2[0]:
                dates.append([(dry[0], year), (dry[1], year)])
                seasons.append('dry')
                if wet_2[0] < wet_2[1]:
                    dates.append([(wet_2[0], year), (wet_2[1], year)])
                else:
                    dates.append([(wet_2[0], year), (wet_2[1], year+1)])
                seasons.append('wet_2')

            # Wet_2 first
            else:
                if wet_2[0] < wet_2[1]:
                    dates.append([(wet_2[0], year), (wet_2[1], year)])
                    if dry[0] < dry[1]:
                        dates.append([(dry[0], year), (dry[1], year)])
                    else:
                        dates.append([(dry[0], year), (dry[1], year+1)])

                else:
                    dates.append([(wet_2[0], year), (wet_2[1], year+1)])
                    dates.append([(dry[0], year+1), (dry[1], year+1)])
                seasons.append('wet_2')
                seasons.append('dry')

    # For current year
    year = current_year
    if wet_1[1] <= chirps_last and wet_1[1] <= enso_last:
        if wet_1[0] < wet_1[1]:
            dates.append([(wet_1[0], year), (wet_1[1], year)])
        else:
            dates.append([(wet_1[0], year-1), (wet_1[1], year)])
        seasons.append('wet_1')

        if len(wet_season) == 1:
            # No second wet season
            if dry[1] <= chirps_last and dry[1] <= enso_last:
                dates.append([(dry[0], year), (dry[1], year)])
                seasons.append('dry')
        else:
            # Presence of a second wet period
            # Check which season appears first dry or wet_2
            # Dry first
            if dry[0] < wet_2[0]:
                if dry[1] <= chirps_last and dry[1] <= enso_last:
                    dates.append([(dry[0], year), (dry[1], year)])
                    seasons.append('dry')
                    if wet_2[1] <= chirps_last and wet_2[1] <= enso_last:
                        dates.append([(wet_2[0], year), (wet_2[1], year)])
                        seasons.append('wet_2')

            # Wet_2 first
            else:
                if wet_2[1] <= chirps_last and wet_2[1] <= enso_last:
                    dates.append([(wet_2[0], year), (wet_2[1], year)])
                    seasons.append('wet_2')
                    if dry[1] <= chirps_last and dry[1] <= enso_last:
                        dates.append([(dry[0], year), (dry[1], year)])
                        seasons.append('dry')

    return dates, seasons


def get_enso_coeff(enso, dates):

    all_months = ['DJF', 'JFM', 'FMA', 'MAM', 'AMJ', 'MJJ', 'JJA', 'JAS', 'ASO', 'SON', 'OND', 'NDJ']
    if dates[0][0] < dates[1][0]:
        months = all_months[dates[0][0]-1 : dates[1][0]]
        years = [dates[0][1]] * (dates[1][0] - dates[0][0] + 1)
    else:
        months = all_months[dates[0][0]-1:] + all_months[:dates[1][0]]
        years = [dates[0][1]]*len(all_months[dates[0][0]-1:]) + [dates[1][1]]*len(all_months[:dates[1][0]])

    coeff = 0
    for m, y in zip(months, years):
        coeff += enso.at[y, m] #enso[m].iloc[y]
    coeff = coeff / len(months)
    return coeff


def run(da_chirps, shapefiles: list, wet_season: list, dry_season: list, enso: pd.DataFrame, path_output: str, n_years = 4):
    """
    n_years: number of years to go back in time for the analysis

    """

    # Create output folder
    folder_name = path_output + '/' + 'enso'
    delete_directory(folder_name)
    pathlib.Path(folder_name).mkdir(parents=True, exist_ok=True)

    # Current year
    current_year = da_chirps.time.values[-1].astype('datetime64[Y]').astype(int) + 1970

    # Chirps temporal resolution
    t = da_chirps.time

    # Dates over n_years years
    chirps_last = t[-1].values.astype('datetime64[M]').astype(int) % 12 + 1
    enso_last = 0
    found = False
    while not(found):
        if isnan(enso.iloc[current_year-1950][enso_last]):
            found = True
        else:
            enso_last += 1
    dates, seasons = get_dates(current_year, n_years, dry_season, wet_season, chirps_last, enso_last)

    unprocessed = []

    for i,shapefile in enumerate(shapefiles):

        # Get asset ID
        ID = os.path.basename(shapefile)[:-4]
        print('-- Processing asset ' + ID + ' (' + str(i + 1) + '/' + str(len(shapefiles)) + ') --')

        # Reading asset
        gdf = gpd.read_file(shapefile)

        # Check asset size
        if not check_asset_size(da_chirps, gdf):
            print('The asset is too small to be processed')
            unprocessed.append([ID, 'Asset too small'])
            continue

        # Clip rasters
        da_chirps_clipped = da_chirps.rio.clip(gdf.geometry.values, gdf.crs)

        # Load CHIRPS values
        da_chirps_clipped.load()

        # Rescale values (chirps: sum over the month)
        da_chirps_clipped = da_chirps_clipped * 3

        # Process for all dates
        sum_chirps = []
        enso_list = []
        dates_csv = []
        for d in dates:
            if d[1][0] in [2]:
                sum_chirps.append(int(da_chirps_clipped.sel(time = t[(t.values >= pd.to_datetime(date(d[0][1], d[0][0], 1))) & (t.values <= pd.to_datetime(date(d[1][1], d[1][0], 28)))]).mean(dim=['latitude','longitude']).sum(dim='time').values))
            elif d[1][0] in [4,6,9,11]:
                sum_chirps.append(int(da_chirps_clipped.sel(time = t[(t.values >= pd.to_datetime(date(d[0][1], d[0][0], 1))) & (t.values <= pd.to_datetime(date(d[1][1], d[1][0], 30)))]).mean(dim=['latitude','longitude']).sum(dim='time').values))
            else :
                sum_chirps.append(int(da_chirps_clipped.sel(time = t[(t.values >= pd.to_datetime(date(d[0][1], d[0][0], 1))) & (t.values <= pd.to_datetime(date(d[1][1], d[1][0], 31)))]).mean(dim=['latitude','longitude']).sum(dim='time').values))

            enso_list.append(get_enso_coeff(enso, d))
            dates_csv.append(str(d[0][1]) + '-' + str(d[0][0]) + '/' + str(d[1][1]) + '-' + str(d[1][0]))

        # Create csv
        with open(folder_name + '/' + ID + '_L_enso.csv', 'w', newline='', encoding='UTF8') as f:
            # create the csv writer
            writer = csv.writer(f, delimiter=';')
            # define zip to write columns
            rows = zip([*['date'],*dates_csv],
                        [*['rainfall'],*sum_chirps],
                        [*['enso'],*enso_list],
                        [*['season'],*seasons])

            # write rows to the csv file
            writer.writerows(rows)

    # Write unprocessed assets in a csv
    unprocessed = pd.DataFrame(unprocessed, columns = ['asset', 'issue'])
    name = 'Unprocessed.csv'
    unprocessed.to_csv(folder_name + '/' + name)
