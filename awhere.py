# -*- coding: utf-8 -*-
#pylint: disable=E0401
import pandas as pd

from datetime import datetime, timedelta
from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from time import sleep

KEY = ''
SECRET = ''
FIELD_URL = 'https://api.awhere.com/v2/fields'
AWHERE_DATE_FORMAT = '%Y-%m-%d'
QUERY_MAX = 120

class Awhere(object):
    ''' Class to interface with the aWhere API'''

    def __init__(self):
        self.key = KEY.strip()
        self.secret = SECRET.strip()
        self.location_url = 'https://api.awhere.com/v2/weather/locations'

    def fetch_token(self):
        oauth = OAuth2Session(client=BackendApplicationClient(client_id=self.key))
        token = oauth.fetch_token(token_url='https://api.awhere.com/oauth/token',\
                client_id=self.key, client_secret=self.secret)
        client = OAuth2Session(self.key, token=token)
        return client

    def single_call(self, lat, lon, startdate, enddate):
        ''' Input, latiduge, longitude, startdate and enddate.
        Date format: 'YYYY-MM-DD' 
        Return: weather information as json.'''
        client = self.fetch_token()
        url = '%s/%s,%s/observations/%s,%s/?limit=%s' % \
            (self.location_url, lat, lon, startdate, enddate, QUERY_MAX)
        print url
        result = client.get(url)
        return result.json()

    def norms_call(self, lat, lon, startdate, enddate, startyear, endyear):
        ''' Input, latiduge, longitude, startdate and enddate.
        Date format: 'YYYY-MM-DD' 
        Return normed call: averaged weather information over N-years as json.'''
        url = '%s/%s,%s/norms/%s,%s/years/%s,%s/?limit=%s' % \
            (self.location_url, lat, lon, startdate, enddate, startyear, endyear, QUERY_MAX)
        print url
        client = self.fetch_token()
        result = client.get(url)
        return result.json()

    def make_batch_call(self, batch, endpt):
        ''' Make a call given a list of json objects with the API request information '''
        mypayload = {
                'title': batch[0]['full_title'],
                 'type': 'batch',
                 'requests': batch
                }
        client = self.fetch_token()
        if endpt == 'old':
            result = \
                client.post('https://awhere-dev.apigee.net/gda_temporary/jobs', mjson=mypayload)
        else:
            result = \
                client.post('https://api.awhere.com/v2/jobs', json=mypayload)
        return result.json()

    def create_batch_single(self, lat, lon, title, start_date_str, end_date_str):
        ''' Given latidude,  longitude, start_date, end_date return a list of API
        calls to send to aWhere.
        Split the query into 120 day increments.
        AWHERE_DATE_FORMAT is set to '%Y-%m-%d'
        Pass in lat,lon as a string for better precision'''
        api_calls = []
        name = 'id_%s_%s_%s' % (lat, lon, title)

        start_date = datetime.strptime(start_date_str, AWHERE_DATE_FORMAT)
        end_date = datetime.strptime(end_date_str, AWHERE_DATE_FORMAT)

        while start_date <= end_date:
            api_call = {}
            # Advance (QUERY_MAX-1)to account for the intial day.
            next_date = start_date + timedelta(days=(QUERY_MAX-1))
            # Check if the next value has gone past the end date.
            if next_date > end_date:
                next_date = end_date
            start_date_str = start_date.strftime(AWHERE_DATE_FORMAT)
            next_date_str = next_date.strftime(AWHERE_DATE_FORMAT)
            api_call['full_title'] = '%s_%s' % (name, start_date_str)
            api_call['title'] = title
            api_call['api'] = 'GET /v2/weather/locations/%s,%s/observations/%s,%s/?limit=%s' \
                % (lat, lon, start_date_str, next_date_str, QUERY_MAX)
            api_calls.append(api_call)
            # Add one day from last date to get new start date
            start_date = next_date + timedelta(days=1)

        return api_calls

    def create_batch_multiple(self, latlon_list, title_list, start_date_str, end_date_str):
        ''' Loop through latlons_list and generate API calls.
        latlons_list can be either a list of lists or a list of tuples.
        See help for create_batch_single.'''
        # Check if every element of latlon_list has a title
        if len(latlon_list) != len(title_list):
            raise ValueError('Length of latlon_list does not match the length of title_list')
        api_calls = []
        for (lat, lon), title in zip(latlon_list, title_list):
            api_calls.append\
            (self.create_batch_single(lat, lon, title, start_date_str, end_date_str))
        return api_calls

    def create_and_make_call_single(self, lat, lon, title, start_year, end_year, endpt='new'):
        ''' Input lat, lon, title, start_year as string, end_year as string.
        Return a list containing a single response.
        The response contains the jobId needed to request the data from aWhere.'''
        batch = self.create_batch_single(lat, lon, title, start_year, end_year)
        response = self.make_batch_call(batch, endpt)
        return [response]

    def create_and_make_call_multiple(self, latlon_list, title_list, start_year, end_year, endpt='new'):
        ''' Input a list of latlons, list of titles, start_year as string, end_year as string.
        Return a list of responses containing the jobIds needed to request the data from aWhere.'''
        responses = []
        batches = self.create_batch_multiple(latlon_list, title_list, start_year, end_year)
        for batch in batches:
            responses.append(self.make_batch_call(batch, endpt))
        return responses

    #Funtions to take a query and return a pd dataframe #######################
    def fetch_data_single(self, lat, lon, title, start_year, end_year, endpt='new'):
        ''' Input: latidude, longitude, title, start_date, end_date, endpt.
        Split the query into 120 day increments and send as a batch job.
        AWHERE_DATE_FORMAT is set to '%Y-%m-%d'
        Pass in lat,lon as a string for better precision
        Returns a pandas dataframe and a list of failed queries'''
        results = self.create_and_make_call_single(lat, lon, title, start_year, end_year, endpt)
        return self.fetch_results_build_dataframe(results)

    def fetch_data_multiple(self, latlon_list, title_list, start_year, end_year, endpt='new'):
        ''' Input: list of latlons, a list of titles, start_date, end_date, endpt.
        Split the query into 120 day increments and send as a batch job.
        AWHERE_DATE_FORMAT is set to '%Y-%m-%d'
        Pass in lat,lon as a string for better precision
        Returns a pandas dataframe and a list of fialed queries'''
        results = \
        self.create_and_make_call_multiple(latlon_list, title_list, start_year, end_year, endpt)
        return self.fetch_results_build_dataframe(results)

    # Funtions to process the API results #######################
    def fetch_results_build_dataframe(self, results):
        '''
        Using the results from the batch calls build a list of jobids,
        loop through all the jobids and wait for the processing to complete.
        Then, build the data returned by the API as a dict and
        construct a pandas dataframe.
        ------------
        Input: results from self.create_and_make_call_*
        Outputs: results as a pandas dataframe, and a list of failed queries.
        '''
        jobids = [result['jobId'] for result in results]
        num_of_jobs = len(jobids)
        results_df, failed_queries = [], []
        # Loop through all jobids
        for job_num, jobid in enumerate(jobids):
            # Constants for each jobid
            jobsatus = 'Queued'
            counter, sleep_sec = 0, 5
            # Wait for the data to process and become available on aWhere.
            while jobsatus != 'Done':
                client = self.fetch_token()
                status = client.get('https://api.awhere.com/v2/jobs/%s' % jobid)
                jobsatus = status.json()['jobStatus']
                print 'Job status: %s. Job timer: %s seconds.' % (jobsatus, counter * sleep_sec)
                print 'On job %s out of %s' % (job_num + 1, num_of_jobs)
                counter += 1
                if jobsatus != 'Done':
                    sleep(sleep_sec)
            flat_data, failed_queries = self.flatten_batch(status.json())
            dataframe = pd.DataFrame(flat_data)
            results_df.append(dataframe)
            print 'Completed jobId: %s' % jobid
        return pd.concat(results_df, ignore_index=True), failed_queries

    def flatten_single(self, api_return):
        obsv_data = []
        for result in api_return['observations']:
            # Check httpStatus for success
            if result['httpStatus'] == 200:
                row = {
                        'date': result['date'],
                        'title': result['title'],
                        'precipitation': result['precipitation']['amount'],
                        'solar': result['solar']['amount'],
                        'humid_max': result['relativeHumidity']['max'],
                        'humid_min': result['relativeHumidity']['min'],
                        'wind_avg': result['wind']['average'],
                        'temp_max': result['temperatures']['max'],
                        'temp_min': result['temperatures']['min'],
                        'latitude': result['location']['latitude'],
                        'longitude': result['location']['longitude'],
                      }
                obsv_data.append(row)
            else:
                print '%s failed' % result['title']
        return obsv_data

    def flatten_batch(self, api_return):
        obsv_data, failed_data = [], []
        for result in api_return['results']:
            # Check httpStatus for success
            if result['httpStatus'] == 200:
                for obsv in result['payload']['observations']:
                    row = {
                            'date': obsv['date'],
                            'title': result['title'],
                            'precipitation': obsv['precipitation']['amount'],
                            'solar': obsv['solar']['amount'],
                            'humid_max': obsv['relativeHumidity']['max'],
                            'humid_min': obsv['relativeHumidity']['min'],
                            'wind_avg': obsv['wind']['average'],
                            'temp_max': obsv['temperatures']['max'],
                            'temp_min': obsv['temperatures']['min'],
                            'latitude': obsv['location']['latitude'],
                            'longitude': obsv['location']['longitude'],
                        }
                    obsv_data.append(row)
            else:
                print '%s failed' % result['payload']['_links']['self']
                failed_data.append(result['payload']['_links']['self'])
        return obsv_data, failed_data
