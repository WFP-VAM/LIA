# Landscape Impact Assessment 
## Automatization process

**Input Data**
 - *Shapefiles*
 - *Rasters* (csv with ODC url to .zarr files)
 - *DataFrames* (asset_info.csv and country_info.csv)

**Processing**
 - `LIA.py` (main script: download data and run analysis)
 - `helpers_fns.py` (helper script with functions for the main script)
 - Analysis scripts:
      - `pre_post.py` (pre/post comparison asset area vs surrounding)
      - ...

**Output**
 - *prepost* (pre/post comparison asset area vs surrounding rasters)
