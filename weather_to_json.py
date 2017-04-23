
#!/usr/bin/env python
#pylint: disable=E0401
#pylint: disable=E1101
# pylint doesn't know about pandas :/
import gzip
import json
import sys
import pandas as pd

from __future__ import division
from collections import OrderedDict
from os import listdir

from pylib.base.flags import Flags

HEADERS = ['Real_Date', 'RegionName', 'ZoneName', 'WoredaName', 'WoredaLat', 'WoredaLon', \
    'source', 'field', 'val']
FIELDS = ['precipitation', 'humid_max', 'humid_min', 'solar', 'temp_max', 'temp_min', 'wind_avg']

def get_geo_code(name_list):
    region, zone, woreda = name_list
    return ('%s__%s__%s__' % (region, zone, woreda)).lower()

def load_woreda_mapped(woreda_mapped_path):
    # Load useing dtype=str to prevent truncating the double
    woreda_df = pd.read_csv(woreda_mapped_path, dtype=str)
    # Remove columns used for name matching
    column_cut = [column for column in woreda_df.columns if 'match' not in column]
    woreda_df = woreda_df[column_cut]
    # Add GeoKey column
    woreda_df['GeoKey'] = [get_geo_code(name_list) for name_list in \
        woreda_df[['RegionName', 'ZoneName', 'WoredaName']].values]
    return woreda_df

def write_weather_to_gzjson(woreda_mapped_path, weather_dir, output_dir):
    woreda_df = load_woreda_mapped(woreda_mapped_path)
    with gzip.open('%s/weather.json.gz' % output_dir, 'wr') as f:
        # Loop through all of the Woreda weather data:
        for filename in [csvfile for csvfile in listdir(weather_dir) if '.csv' in csvfile]:
            # Load in csv as dataframe
            weather_df = pd.read_csv('%s/%s' % (weather_dir, filename), index_col=0)
            # Inner join the two dataframes on the GeoKey
            weather_merge = pd.merge(weather_df, woreda_df, how='inner', on='GeoKey')

            for row in weather_merge.iterrows():
                row = row[1]
                # Avoid pandas truncating the double
                lat = float(row['WoredaLat'])
                lon = float(row['WoredaLon'])
                woreda_consts = row[['date', 'RegionName', 'ZoneName', 'WoredaName']].tolist() \
                    + [lat, lon, 'weather']
                # Loop throgh all of the fields. Skip nan values.
                for field, value in row[FIELDS].dropna().iteritems():
                    val_list = woreda_consts + [field, value]
                    # OderedDict to perserve header order
                    f.write(json.dumps(\
                        OrderedDict([(key, value) for key, value in zip(HEADERS, val_list)])))
                    f.write('\n')

def main():
    Flags.PARSER.add_argument('--woreda_mapped_path', type=str, required=True,
                              help='path to woreda_mapped.csv')
    Flags.PARSER.add_argument('--weather_data_dir', type=str, required=True,
                              help='path folder with weather data csv')
    Flags.PARSER.add_argument('--output_dir', type=str, required=True,
                              help='path output dir')
    Flags.InitArgs()

    write_weather_to_gzjson(Flags.ARGS.woreda_mapped_path, \
            Flags.ARGS.weather_data_dir, \
            Flags.ARGS.output_dir)
    return 0

if __name__ == '__main__':
    sys.exit(main())
