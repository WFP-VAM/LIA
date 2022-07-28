# Anomalies of NDVI/LST/Rainfall against LTA

import os
import pathlib
import numpy as np
import matplotlib.pyplot as plt
from utils.helper_fns import delete_directory

import warnings
warnings.filterwarnings("ignore")

def run(da_chirps, da_ndvi, da_lst, path_output: str):

    # Create output folder
    folder_name = path_output + '/' + 'Anomalies'
    delete_directory(folder_name)
    pathlib.Path(folder_name).mkdir(parents=True, exist_ok=True)

    # Rescale the data
    da_ndvi = da_ndvi / 10000
    da_lst = da_lst * 0.02 - 273.15
    da_chirps = da_chirps * 3
    
    da_ndvi = da_ndvi.astype('float32')
    da_ndvi = da_ndvi.where(da_ndvi >= 0)
    da_lst = da_lst.astype('float32')
    da_lst = da_lst.where(da_lst >= 0)
    da_chirps = da_chirps.astype('float32')
    da_chirps = da_chirps.where(da_chirps >= 0)
    
    # Compute lta of the datasets
    da_ndvi_lta = da_ndvi.groupby(da_ndvi.time.dt.strftime("%m-%d")).mean()
    da_lst_lta = da_lst.groupby(da_lst.time.dt.strftime("%m-%d")).mean()
    da_chirps_lta = da_chirps.groupby(da_chirps.time.dt.strftime("%m-%d")).mean()

    # Take the mean over the coutry
    da_ndvi = da_ndvi.mean(dim=['latitude','longitude'])
    da_lst = da_lst.mean(dim=['latitude','longitude'])
    da_chirps = da_chirps.mean(dim=['latitude','longitude'])
    
    da_ndvi_lta = da_ndvi_lta.mean(dim=['latitude','longitude'])
    da_lst_lta = da_lst_lta.mean(dim=['latitude','longitude'])
    da_chirps_lta = da_chirps_lta.mean(dim=['latitude','longitude'])
    
    # Compute the anomalies    
    def scale_ndvi(x):
        x_lta = da_ndvi_lta.sel(strftime = x.time.dt.strftime("%m-%d"))
        return (x - x_lta)

    def scale_lst(x):
        x_lta = da_lst_lta.sel(strftime = x.time.dt.strftime("%m-%d"))
        return (x - x_lta)
    
    def scale_chirps(x):
        x_lta = da_chirps_lta.sel(strftime = x.time.dt.strftime("%m-%d"))
        return 100*(x - x_lta)/x_lta
    
    ndvi_anom = da_ndvi.groupby(da_ndvi.time.dt.strftime("%m-%d")).map(scale_ndvi)
    lst_anom = da_lst.groupby(da_lst.time.dt.strftime("%m-%d")).map(scale_lst)
    chirps_anom = da_chirps.groupby(da_chirps.time.dt.strftime("%m-%d")).map(scale_chirps)
    
    # Plot the anomalies 
    # NDVI
    color = []
    ndvi = ndvi_anom.values
    for i in range(len(ndvi)):
        if ndvi[i]<0:
            color.append('orange')
        else:
            color.append('green')
            
    temp = ndvi_anom.to_series()
    temp = temp.set_axis(temp.index.year)
    

    
    fig,ax = plt.subplots(1,1,figsize=(14,5))
    ax = temp.plot.bar(color=color, width=0.98, alpha=0.6)
    xticks = []
    for i, t in enumerate(ax.get_xticklabels()):
        if (i % 12) == 0:
            xticks.append(i)
    ax.set_xticks(xticks)    
    plt.grid()
    ax.set_ylabel('NDVI anomalies')
    max_y = np.abs(temp).max() + 0.03
    ax.set_ylim(-max_y,max_y)
    
    ax2 = ax.twinx()
    ax2.plot(chirps_anom, alpha=0.7, color='grey')
    ax2.set_ylabel('Rainfall anomalies (%)')
    max_y = np.abs(chirps_anom).max() + 5
    ax2.set_ylim(-max_y,max_y)
    
    plt.title('Anomalies')
    plt.savefig(folder_name + '/' + 'NDVI')
    
    # LST
    plt.clf()
    color = []
    lst = lst_anom.values
    for i in range(len(lst)):
        if lst[i]<0:
            color.append('red')
        else:
            color.append('darkblue')
            
    temp = lst_anom.to_series()
    temp = temp.set_axis(temp.index.year)

    ax = temp.plot.bar(color=color, width=0.98)
    xticks = []
    for i, t in enumerate(ax.get_xticklabels()):
        if (i % 12) == 0:
            xticks.append(i)
    ax.set_xticks(xticks)
    plt.title('LST Anomalies')
    plt.savefig(folder_name + '/' + 'LST')
    
    
        