# Landscape Impact Assessment 
## Automatization process

**Input Data**
 - *Shapefiles*
 - *Rasters* (csv with ODC url to .zarr files)
 - *DataFrames* (asset_info.csv and country_info.csv)

**Processing**
 - `LIA.py` (Main script: download data and run analysis)
 - `pre_post.py (pre/post comparison asset area vs surrounding)
 - ...

**Output**
 - *NDVI_prepost* (NDVI pre/post comparison asset area vs surrounding rasters)
