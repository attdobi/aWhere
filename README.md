## Synopsis

The purpose of the aWhere class is the automation of batch jobs when quering data from aWhere using their API.
The user provides a list of latidudes, longidtutes, and geo_titles, startdate, enddate ... the data from aWhere will appear magically, formatted in a pandas dataframe :)


## Files
See the ipython notebook for example usage of the aWhere.py python API wrapper.
The data folder contains an example output.
The lat_lons folder contains an csv with example latitudes and longitudes.

## Code Example
```
pull_woreda_weather.py --woreda_latlon_path lat_lons --output_data_path data --start_date 2015-01-01 --end_date 2017-02-01 --batch_type single
```

## Make a single call:

```
lat, lon = 10.01, 40.02
awhere.single_call(lat, lon, '2016-02-01', '2017-02-01')
```

## Batch job for single geo location. Return a dataframe and write to csv
```
lat, lon, title = 10.01, 40.02, 'Woreda1'
dataframe, failed = awhere.fetch_data_single(lat, lon, title, '2016-02-01', '2017-02-01')
dataframe.to_csv('data/test_single.csv')
```

## Batch job for single geo location. Return a dataframe and write to csv
```
lats = [1, 2, 3, 4]
lons = [-1, -2, -3, -4]
latlons = zip(lats, lons)
titles = ['geo1', 'geo2', 'geo3', 'geo4']

dataframe, failed = awhere.fetch_data_multiple(latlons, titles, '2016-02-01', '2017-02-01')
dataframe.to_csv('data/test_multiple.csv')
```