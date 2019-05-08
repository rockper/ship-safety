# Vessel Type codes are a 2 or 4-digit (since 2017) code (See "vessel_type_codes_2018.pdf").
# There are 66 vessel type codes. 
# These will be collected into 10 broad vessel groups:
# 1 passenger
# 2 cargo
# 3 tanker
# 4 fishing
# 5 pleasure
# 6 tugTow
# 7 publicService (law enforcement & rescue)
# 8 military
# 9 research
# 10 unknown (vessels that have been assigned a mmsi, but no metadata is known)
#---------------------------------------------
import pandas as pd
import dask.dataframe as dd
import os

DATA_DIR = '/media/rock/x/data/ais/output/'
CODE_DIR = '/media/rock/x/data/ais/'
os.chdir(CODE_DIR)

# Vessel Type codes are in the metadata csv file:
col_names = ['mmsi', 'vessel_type', 'vessel_name', 'imo', 'call_sign', 'l', 'w', 'draft', 'cargo']
types = ['int32', 'category', 'object', 'object', 'object', 'float64', 'float64', 'float64', 'category']
col_dtypes = dict(zip(col_names, types))

metadata_file = DATA_DIR + 'metadata_3m.csv'
vessels = pd.read_csv(metadata_file, header=0, names=col_names, dtype=col_dtypes)

# Read the parquet tracks dataset created with Spark.
# Dask can read partitioned parquet.
ddf = dd.read_parquet(path=DATA_DIR + 'tracks_3m_pqt/')

vgroup2vtypecode_dict = {'pleasure': ['36', '37', '1019'], 
                    'publicService': [str(i) for i in list(range(50, 60))] + ['1018'], 
                    'passenger': [str(i) for i in list(range(60, 70))] + ['1012', '1013', '1014', '1015'], 
                    'cargo': [str(i) for i in list(range(70, 80))] + ['1003', '1004', '1016'], 
                    'tanker': [str(i) for i in list(range(80, 90))] + ['1017','1024'], 
                    'fishing': ['30', '1001', '1002'], 
                    'tugTow': ['21', '22', '31', '32', '52', '1023', '1025'],  
                    'military': ['35', '1021'], 
                    'research': ['1020'],
                    'unknown': ['']
                   }


def getDfByVesselGroup(vessel_group='cargo', ddf=ddf):
    """
    Returns a dask DataFrame for all vessels in the specified group.
    Has not been materialized to pandas with '.compute()'
    """
    if vessel_group == 'unknown':
        mmsis = (vessels[vessels['vessel_type'].isnull()])['mmsi']
    else:
        mmsis = (vessels[vessels['vessel_type'].isin(vgroup2vtypecode_dict[vessel_group])])['mmsi']
    vesselGroupDf = ddf[ddf['mmsi'].isin(mmsis)]
    return vesselGroupDf


def writeVesselGroupToParquet(vessel_group='cargo', period='3m', ddf=ddf):
    """
    Writes a single parquet file for all mmsis in a specified vessel category for '3m' or '3y'
    """
    ddf_filtered_by_vgroup = getDfByVesselGroup(vessel_group, ddf).compute()
    outputFile = DATA_DIR + 'by_vessel_group_' + period + '/' + vessel_group + '.parquet'
    ddf_filtered_by_vgroup.to_parquet(outputFile)
    return