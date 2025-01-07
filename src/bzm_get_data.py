# @file    bzm_get_data.py
# @author  Egbert Klaassen
# @date    2025-01-05

import os
import pandas as pd
import requests
import pandas_geojson as pdg
from bs4 import BeautifulSoup


### Get geojson file

filename_geojson = 'bzm_telraam_segments_2025.geojson'
path_geojson = '/src' + '/' + filename_geojson
geojson = pdg.read_geojson(path_geojson)
df_geojson = geojson.to_dataframe()
df_geojson.columns = df_geojson.columns.str.replace('properties.segment_id', 'segment_id')
df_geojson.columns = df_geojson.columns.str.replace('properties.', '', regex=True)


### Get traffic file

# Retrieve file links
print('Getting traffic data files...')
url = 'https://berlin-zaehlt.de/csv/'
page = requests.get(url).text
soup = BeautifulSoup(page, 'html.parser')
filename_startswith = 'bzm_telraam_'
links = [url + '/' + node.get('href') for node in soup.find_all('a') if node.get('href').startswith(filename_startswith)]

# Add file contents to Dataframe
df_csv_append = pd.DataFrame()

for link in links:

    # Get filename from link
    filename = link.split('/')[-1]

    # Warning! - all years result in too large Excel file (million+ rows)

    # Loop through gz files, filter by "start with" string, add to Dataframe
    if filename[12:16] in ['2023','2024']:
        print('Processing: ' + filename)
        df = pd.read_csv(os.path.join(url, filename), compression='gzip', header=0, sep=',', quotechar='"')
        df_csv_append = df_csv_append._append(df, ignore_index=True)

    # Alternative: Loop through gz files, filter by "contains substrings", add to Dataframe
    # substrings= ['_07','_08','_09']
    # substrings = ['2024']
    #if any(sub in filename for sub in substrings):
    #    print('Processing: ' + filename)
    #    df = pd.read_csv(os.path.join(url,filename), compression='gzip', header=0, sep=',', quotechar='"')
    #    df_csv_append = df_csv_append._append(df, ignore_index=True)

# Merge traffic data with geojson information, select columns, define data formats and add date_time columns
print('Combining traffic and geojson data...')
df_comb = pd.merge(df_csv_append, df_geojson, on = 'segment_id', how = 'outer')

print('Creating df with selected columns')
selected_columns = ['date_local','segment_id','uptime','ped_lft','ped_rgt','ped_total','bike_lft','bike_rgt','bike_total','car_lft','car_rgt','car_total','heavy_lft','heavy_rgt','heavy_total','v85','car_speed0','car_speed10','car_speed20','car_speed30','car_speed40','car_speed50','car_speed60','car_speed70','osm.name','osm.highway','osm.length','osm.width','osm.lanes','osm.maxspeed']
traffic_df = pd.DataFrame(df_comb, columns=selected_columns)
traffic_df['date_local'] = pd.to_datetime(traffic_df['date_local'])

print('Drop empty rows...')
nan_rows = traffic_df[traffic_df['date_local'].isnull()]
traffic_df = traffic_df.drop(nan_rows.index)
nan_rows = traffic_df[traffic_df['osm.name'].isnull()]
traffic_df = traffic_df.drop(nan_rows.index)

print('Break down date_local to new columns...')
traffic_df.insert(0, 'weekday', traffic_df['date_local'].dt.dayofweek)
traffic_df.insert(0, 'hour', traffic_df['date_local'].dt.hour)
traffic_df.insert(0, 'day', traffic_df['date_local'].dt.day)
traffic_df.insert(0, 'month', traffic_df['date_local'].dt.month)
traffic_df.insert(0, 'year', traffic_df['date_local'].dt.year)

print('Exchange time data for labels...')
traffic_df['weekday'] = traffic_df['weekday'].map({0: 'Mon', 1: 'Tue', 2: 'Wed', 3: 'Thu', 4: 'Fri', 5: 'Sat', 6: 'Sun'})
traffic_df['month'] = traffic_df['month'].map({1: 'Jan', 2: 'Feb', 3: 'Mar', 4: 'Apr', 5: 'May', 6: 'Jun', 7: 'Jul', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'})

print('Setting data types and formats...')
traffic_df = traffic_df.astype({'weekday': int, 'month': int, 'year': int}, errors='ignore')
traffic_df.insert(0, 'year_month', traffic_df['date_local'].dt.strftime('%Y/%m'))
traffic_df.insert(0, 'date', traffic_df['date_local'].dt.strftime('%Y/%m/%d'))

# Save data package to file - change file name!
print("Saving data package...")
traffic_df.to_excel("D:/OneDrive/PycharmProjects/bzm_telraam/Data_files/traffic_df_2023-2024.xlsx", index=False)

print('Finished.')