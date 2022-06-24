# first import the functions for downloading data from NWIS
import pandas as pd
import dataretrieval.nwis as nwis
import os

# List of key USGS gages throughout the Upper CO River Basin
site_list = ['393259107194801', # 'COLORADO R ABV ROARING FORK R AT GLENWOOD SPGS, CO',
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


# specify the USGS site code for which we want data.
start_date = '1980-01-01'

# If the directory 'DataBySite' does not exist, make it.
path = os.getcwd() + '/DataBySite'
if not(os.path.isdir(path)):
    os.mkdir('DataBySite')
os.chdir(path)

dict = {'station_id': [], 'site_name': [], 'latitude': [], 'longitude': [], 'data_available': []}
df = pd.DataFrame(data=dict)
record = [None] * 5

for i in range(len(site_list)):
    print(i)
    # get basic info about the site
    df3 = nwis.get_record(sites=site_list[i], service='site')
    df4 = nwis.get_record(sites=site_list[i], service='dv', start=start_date, parameterCd='00060')
    record[0] = df3.loc[0, 'site_no']
    record[1] = df3.loc[0, 'station_nm']
    record[2] = df3.loc[0, 'dec_lat_va']
    record[3] = df3.loc[0, 'dec_long_va']
    if (df4.empty):
        record[4] = 'NO'
    else:
        record[4] = 'YES'
    df.loc[len(df.index)] = record

    if not(df4.empty):
        df4.drop(['site_no','00060_Mean_cd'], axis=1, inplace=True)
        df4.rename({"00060_Mean": "discharge_cfs"}, axis=1, inplace=True)
        df4.rename({"datetime": "datetime"}, axis=1, inplace=True)
        df4 = df4.rename_axis('date').reset_index()
        #df4['date'].str[0:8]
        df4['date'] = df4['date'].apply(lambda x: str(x)[0:10])
        df4['year'] = df4['date'].apply(lambda x: str(x)[0:4])
        df4['month'] = df4['date'].apply(lambda x: str(x)[5:7])
        df4['day'] = df4['date'].apply(lambda x: str(x)[8:11])
        df4.to_csv(site_list[i] + '.csv')

df.to_csv('metadata.csv')



