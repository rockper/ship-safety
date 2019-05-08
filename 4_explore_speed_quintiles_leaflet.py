# Objective:
# Visually explore Vessel Tracks from a Vessel Group with Speed Quintiles.
# Low ['cyan', 'orange', 'green', 'purple', 'red'] High Speed

# How to use in a Jupyter notebook:
# 1 Select a Vessel Group and period ('3m' or '3y)
# ['cargo', 'tanker', 'passenger', 'fishing', 'tugTow', 'pleasure', 
# 'publicService', 'military', 'research', 'unknown']
# pdf = get_vgroup_df(vgroup='passenger', period='3m')
# mmsi_arr = np.random.choice(pdf.mmsi, size=10, replace=False)
# mmsi = mmsi_arr[0]
# 2 Vessel Metadata & Speeds:
# get_vessel_metadata(mmsi)
# (get_max_speed(mmsi), get_median_speed(mmsi), get_speed_quintiles(mmsi))
# 3 To plot the speed quintiles:
# map_speed(mmsi)
# 4 To examine more vessels:
# mmsi = mmsi_arr[1]  # or 2 through 9
# Repeat steps 2 & 3

import numpy as np
import pandas as pd
import os
from ipyleaflet import Map, Heatmap, basemaps, basemap_to_tiles, Polyline, LayerGroup, LayersControl
from palettable.cartocolors.qualitative import Bold_5_r

esri_satellite_layer = basemap_to_tiles(bm=basemaps.Esri.WorldImagery)
DATA_DIR = '/media/rock/x/data/ais/output/'
CODE_DIR = '/media/rock/x/data/ais/'
os.chdir(CODE_DIR)

def get_vgroup_df(vgroup='cargo', period='3m'):
    """
    Returns a pandas DataFrame for the specified vessel group and period
    ['cargo', 'tanker', 'passenger', 'fishing', 'tugTow', 'pleasure', 
    'publicService', 'military', 'research', 'unknown']
    from exploratory data (period='3m') or modeling data (period='3y')
    """
    if period == '3y':
        filename = DATA_DIR + 'by_vessel_group_3y/' + vgroup + '_3y_pqt'
    else:
        filename = DATA_DIR + 'by_vessel_group_3m/' + vgroup + '_3m.parquet'
        
    vgroupDf = pd.read_parquet(path=filename)
    return vgroupDf

# Select a Vessel Group to explore its tracks
pdf = get_vgroup_df(vgroup='cargo', period='3y')
mmsi_arr = np.random.choice(pdf.mmsi, size=10, replace=False)
mmsi = mmsi_arr[0]

# Get the vessel's metadata:
col_names = ['mmsi', 'vessel_type', 'vessel_name', 'imo', 'call_sign', 'l', 'w', 'draft', 'cargo']
types = ['int32', 'category', 'object', 'object', 'object', 'float64', 'float64', 'float64', 'category']
col_dtypes = dict(zip(col_names, types))
metadata_file = CODE_DIR + 'output/' + 'metadata_3y.csv'
vessels = pd.read_csv(metadata_file, header=0, names=col_names, dtype=col_dtypes)

def get_vessel_metadata(mmsi):
    return vessels.loc[vessels.mmsi == mmsi]

def get_max_speed(mmsi):
    return np.max((pdf.sogs[pdf.mmsi == mmsi]).iloc[0])

def get_median_speed(mmsi):
    speed_arr = (pdf.sogs[pdf.mmsi == mmsi]).iloc[0]
    in_motion_speeds = speed_arr[speed_arr >= 1.0]
    return np.percentile(in_motion_speeds, 50)

def get_speed_quintiles(mmsi):
    speed_arr = (pdf.sogs[pdf.mmsi == mmsi]).iloc[0]
    in_motion_speeds = speed_arr[speed_arr >= 1.0]
    qs = np.asarray([20, 40, 60, 80])
    return np.percentile(in_motion_speeds, qs)

def make_speed_polylines(mmsi):
    """
    Creates a leaflet multipolyline
    """
    rowDf = (pdf[pdf.mmsi == mmsi])
    sogs = rowDf.sogs.iloc[0]
    q = get_speed_quintiles(mmsi)
    mpoly_list = [None] * 5
    for i in range(5):
        qlo = 0.0 if i == 0 else q[i-1]
        qhi = (np.max(sogs) + 0.1) if i == 4 else q[i]  # Add 0.1 because we must also select the max speed itself
        sogs_in_this_quantile = (np.where((sogs >= qlo) & (sogs < qhi)))[0]
        lats = (rowDf.lats.iloc[0])[sogs_in_this_quantile]
        lons = (rowDf.lons.iloc[0])[sogs_in_this_quantile]
        dts = (rowDf.base_dts.iloc[0])[sogs_in_this_quantile]
        split_index = (np.where((np.ediff1d(dts) / np.timedelta64(1, 'm')) > 2.0))[0]
        split_index += 1
        lats_split = np.split(lats, split_index)  # Create a list of subarrays. The list has length = len(split_index) + 1
        lons_split = np.split(lons, split_index)
        multipolyline_points = [[[(lats_split[j])[k], (lons_split[j])[k]] for k in range(len(lats_split[j]))] for j in range(len(lats_split))]
        mpoly_list[i] = multipolyline_points
        
    return mpoly_list

def map_speed(mmsi):
    """
    Maps the vessel tracks with relative speed quintiles
    """
    m = Map(center=(48.0, -122.65), zoom=10)
    m.layout.width = '60%'
    m.layout.height = '1000px'
    m.add_layer(esri_satellite_layer)
    m.add_control(LayersControl())
    mpoly_list = make_speed_polylines(mmsi)
    colors = ['cyan', 'orange', 'green', 'purple', 'red']
    lines = [Polyline(locations=mpoly_list[i], color=colors[i], fill_color=colors[i], fill_opacity=0.0) for i in range(5)]
    layer_group = LayerGroup(layers=tuple(lines))
    m.add_layer(layer_group)
    return m