
#!/usr/bin/env python
#pylint: disable=E0401
import datetime
import sys
import pandas as pd

from __future__ import division

from awhere import Awhere
from flags import Flags

DATE_FORMAT = '%Y-%m-%d'
# Max query date is up until yesterday
MAX_END_DATE = datetime.datetime.now() - datetime.timedelta(days=1)
MAX_END_DATE_STR = MAX_END_DATE.strftime(DATE_FORMAT)

AWHERE = Awhere()

def check_end_date(end_date_str):
    # Check if end date is valid:
    end_date = datetime.datetime.strptime(end_date_str, DATE_FORMAT)
    if end_date > MAX_END_DATE:
        end_date_str = MAX_END_DATE_STR
        print 'Max query date is up until yesterday: %s' % end_date_str
    return end_date_str

def load_woreda_dataframe(woreda_info_path):
     # Read in dataframe with woreda lat,lon, geokey as string
    woreda_df = pd.read_csv(woreda_info_path, dtype=str)
    # Filter the dataframe for non zero latidude
    return woreda_df[woreda_df['WoredaLat'].astype(float) != 0]

def process_batch(woreda_df, weather_folder_path, start_date_str, end_date_str):
    ''' Write a single csv file for all of the Woredas and dates.'''
    latlons = woreda_df[['WoredaLat', 'WoredaLon']].values
    geokeys = woreda_df['GeoKey'].values
    dataframe, fails = AWHERE.fetch_data_multiple(latlons, geokeys, start_date_str, end_date_str)
    if len(fails) > 0:
        print 'The following queries failed: \n', fails
    # Rename title as the GeoKey
    dataframe.rename(columns={'title': 'GeoKey'}, inplace=True)
    # Write all data to a single csv
    dataframe.to_csv('%s/batch_%s_%s.csv' % (weather_folder_path, start_date_str, end_date_str))

def process_single(woreda_df, weather_folder_path, start_date_str, end_date_str):
    ''' Loop through each Woreda and produce a csv for each.'''
    for lat, lon, geokey in woreda_df[['WoredaLat', 'WoredaLon', 'GeoKey']].values:
        dataframe, fails = AWHERE.fetch_data_single(lat, lon, geokey, start_date_str, end_date_str)
        if len(fails) > 0:
            print 'The following queries failed: \n', fails
        # Rename title as the GeoKey
        dataframe.rename(columns={'title': 'GeoKey'}, inplace=True)
        # Write weather data of each Woreda to csv
        dataframe.to_csv('%s/%s.csv' % (weather_folder_path, geokey.replace(' ', '_')))

def get_weather(woreda_info_path, weather_folder_path, start_date_str, end_date_str, batch_type):
    end_date_str = check_end_date(end_date_str)
    woreda_df = load_woreda_dataframe(woreda_info_path)
    if batch_type == 'batch':
        process_batch(woreda_df, weather_folder_path, start_date_str, end_date_str)
    else:
        process_single(woreda_df, weather_folder_path, start_date_str, end_date_str)

def main():
    Flags.PARSER.add_argument('--woreda_latlon_path', type=str, required=True,
                              help='path to woreda_info.csv')
    Flags.PARSER.add_argument('--output_data_path', type=str, required=True,
                              help='path folder with weather data csv')
    Flags.PARSER.add_argument('--start_date', type=str, required=True,
                              help='start date "YYYY-MM-DD"')
    Flags.PARSER.add_argument('--end_date', type=str, required=True,
                              help='end date "YYYY-MM-DD"')
    Flags.PARSER.add_argument('--batch_type', type=str, required=True,
                              help='single or batch')
    Flags.InitArgs()

    batch_type = Flags.ARGS.batch_type
    if batch_type not in {'single', 'batch'}:
        raise ValueError('batch_type needs to be either "single" or "batch"')

    get_weather(Flags.ARGS.woreda_latlon_path, \
            Flags.ARGS.output_data_path, \
            Flags.ARGS.start_date, \
            Flags.ARGS.end_date, \
            batch_type)

if __name__ == '__main__':
    sys.exit(main())
