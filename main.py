# first import the functions for downloading data from NWIS
import pandas as pd
import dataretrieval.nwis as nwis
import os

# List of key USGS gages throughout the Upper CO River Basin
site_list = [#'393259107194801', # 'COLORADO R ABV ROARING FORK R AT GLENWOOD SPGS, CO',
             '09095500',        # 'COLORADO RIVER NEAR CAMEO, CO.',
             '09109000',        # 'TAYLOR RIVER BELOW TAYLOR PARK RESERVOIR, CO.',
             '383103106594200', # 'GUNNISON RIVER AT CNTY RD 32 BELOW GUNNISON, CO',
             '09128000',        # 'GUNNISON RIVER BELOW GUNNISON TUNNEL, CO',
             '09152500',        # 'GUNNISON RIVER NEAR GRAND JUNCTION, CO.',
             '09180000',        # 'DOLORES RIVER NEAR CISCO, UT',
             '09180500',        # 'COLORADO RIVER NEAR CISCO, UT',
             '09211200',        # 'GREEN RIVER BELOW FONTENELLE RESERVOIR, WY',
             '09209400',        # ' GREEN RIVER NEAR LA BARGE, WY',
             '09234500',        # 'GREEN RIVER NEAR GREENDALE, UT',
             '09251000',        # YAMPA RIVER NEAR MAYBELL, CO
             '09260000',        # LITTLE SNAKE RIVER NEAR LILY, CO
             '09302000',        # DUCHESNE RIVER NEAR RANDLETT, UT,
             '09306500',        # WHITE RIVER NEAR WATSON, UTAH
             '09315000',        # GREEN RIVER AT GREEN RIVER, UT,
             '09328500',        # SAN RAFAEL RIVER NEAR GREEN RIVER, UT
             '09355500',        # SAN JUAN RIVER NEAR ARCHULETA, NM
             '09379500',        # SAN JUAN RIVER NEAR BLUFF, UT
             '09380000',        # COLORADO RIVER AT LEES FERRY, AZ
             '09382000',        # PARIA RIVER AT LEES FERRY, AZ
             '09402000',        # LITTLE COLORADO RIVER NEAR CAMERON, AZ
             '09402500',        # COLORADO RIVER NEAR GRAND CANYON, AZ
             '09415000',        # VIRGIN RV AT LITTLEFIELD, AZ
             '09421500',        # COLORADO RV BLW HOOVER DAM, AZ-NV,
             '09423000',        # COLORADO RIVER BELOW DAVIS DAM, AZ-NV
             '09426000',        # BILL WILLIAMS RIVER BELOW ALAMO DAM, AZ
             '09427520',        # COLORADO RIVER BELOW PARKER DAM, AZ-CA
             '09429490',        # COLORADO RIVER ABOVE IMPERIAL DAM, AZ-CA
             ]

# Start date for analyzing data 
start_date = '1980-01-01'

# If the directory 'DataBySite' does not exist, make it.
path = os.getcwd() + '/DataBySite'
if not(os.path.isdir(path)):
    os.mkdir('DataBySite')
os.chdir(path)

# Columns of metadata file
dict_meta = {'station_id': [], 'site_name': [], 'latitude': [], 'longitude': [], 'data_available': []}
df_meta = pd.DataFrame(data=dict_meta)
record_meta = [None] * 5

# Iterates through each site in site_list
for i in range(len(site_list)):
    
    df_basic_info = nwis.get_record(sites=site_list[i], service='site')
    df_series_data = nwis.get_record(sites=site_list[i], service='dv', start=start_date, parameterCd='00060')
    
    record_meta[0] = df_basic_info.loc[0, 'site_no']
    record_meta[1] = df_basic_info.loc[0, 'station_nm']
    record_meta[2] = df_basic_info.loc[0, 'dec_lat_va']
    record_meta[3] = df_basic_info.loc[0, 'dec_long_va']
    if (df_series_data.empty):
        record_meta[4] = 'NO'
    else:
        record_meta[4] = 'YES'
    df_meta.loc[len(df_meta.index)] = record_meta

    if not(df_series_data.empty):

        df_series_data.drop(['site_no','00060_Mean_cd'], axis=1, inplace=True)
        df_series_data.rename({"00060_Mean": "discharge_cfs"}, axis=1, inplace=True)
        df_series_data = df_series_data.rename_axis('date').reset_index()

        df_series_data['date'] = df_series_data['date'].apply(lambda x: str(x)[0:10])
        df_series_data['year'] = df_series_data['date'].apply(lambda x: str(x)[0:4])
        df_series_data['month'] = df_series_data['date'].apply(lambda x: str(x)[5:7])
        df_series_data['day'] = df_series_data['date'].apply(lambda x: str(x)[8:11])
        df_series_data.to_csv(site_list[i] + '_daily.csv', columns=['date', 'year', 'month', 'day', 'discharge_cfs'])

        # Aggregate daily series data to monthly
        df_series_data['date'] = df_series_data['date'].apply(lambda x: str(x)[0:7])
        df_monthly = df_series_data.groupby(['date', 'year', 'month']).agg({'discharge_cfs': ['min', 'max', 'mean']})
        df_monthly.to_csv(site_list[i] + '_monthly_summary.csv')

df_meta.to_csv('metadata.csv')



