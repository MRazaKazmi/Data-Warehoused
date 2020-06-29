"""
The puprose of this custom operator is to export files from the local machine to an S3 bucket.
"""
import os
import glob
import requests
import json
import pandas as pd
import numpy as np
import time 
import s3fs

from airflow.hooks.S3_hook import S3Hook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class ExportSongsToS3(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 bucket_name = "",
                 aws_conn_id = "",
                 region = "",
                 bucket_key = "",
                 api_key="",
                 *args, **kwargs):

        super(ExportSongsToS3, self).__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.aws_conn_id = aws_conn_id
        self.region = region
        self.bucket_key  = bucket_key
        self.api_key = api_key
        
    def execute(self, context):
        
        # connects to S3 using the connection created in the airflow UI.
        s3 = S3Hook(aws_conn_id=self.aws_conn_id)

        aws_hook = AwsHook('aws_credentials_id')
        credentials = aws_hook.get_credentials() 
        os.environ['AWS_ACCESS_KEY_ID'] = credentials.access_key
        os.environ['AWS_SECRET_ACCESS_KEY'] = credentials.secret_key
        AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
        AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']


        if s3.check_for_bucket(self.bucket_name):
            self.log.info('Bucket exists.')
        else:
            s3.create_bucket(self.bucket_name,self.region)
            self.log.info('Bucket created.')
        
        self.log.info('Collecting data...')
            

        def lastfm_get(payload):
            # define URL
            url = 'http://ws.audioscrobbler.com/2.0/'

            # Add API key and format to the payload
            payload['api_key'] = self.api_key
            payload['format'] = 'json'

            response = requests.get(url, params=payload)
            return response

        def get_top_artists(country):
            top_artists_response = lastfm_get({
            'method': 'geo.getTopArtists',
            'country' : country
            })
            
            # return nonthing when error happen
            if top_artists_response.status_code != 200:
                return None

            #Since it's can be convert into dataframe directly, I'm going to use 
            #some variables to store it first
            artist = [t['name'] for t in top_artists_response.json()['topartists']['artist']]
            artist_listeners = [t['listeners'] for t in top_artists_response.json()['topartists']['artist']]
            
            #for not reach the limit
            if not getattr(top_artists_response, 'from_cache', False):
                time.sleep(0.25)
                
            return artist,artist_listeners



        self.log.info('Connecting to API to query data...')
            
        result = get_top_artists('United States')
        artist = result[0]
        artist_listeners = result[1]

        artist_info = pd.DataFrame(zip(artist,artist_listeners), columns =['artist','artist_listeners'])

        def get_top_tracks(artist):
                track_response = lastfm_get({
                'method': 'artist.getTopTracks',
                'artist' :artist
                })
                
                # if there are error, we don't return anything
                if track_response.status_code != 200:
                    return None
                
                #I'm intersted into top 3 tags
                song = [t['name'] for t in track_response.json()['toptracks']['track']]
                song_playcount = [t['playcount'] for t in track_response.json()['toptracks']['track']]    # rate limiting
                song_listeners = [t['listeners'] for t in track_response.json()['toptracks']['track']]    # rate limiting


                if not getattr(track_response, 'from_cache', False):
                    time.sleep(0.25)
                
                return song,song_playcount,song_listeners

        song = []
        song_playcount=[]
        song_listeners=[]
        for i in range(len(artist_info)):
            song.append(get_top_tracks(artist_info['artist'][i])[0])
            song_playcount.append(get_top_tracks(artist_info['artist'][i])[1])
            song_listeners.append(get_top_tracks(artist_info['artist'][i])[2])

        artist_info['song'] = song
        artist_info['song_playcount'] = song_playcount
        artist_info['song_listeners'] = song_listeners  


        def explode(df, columns):
                idx = np.repeat(df.index, df[columns[0]].str.len())
                a = df.T.reindex(columns).values
                concat = np.concatenate([np.concatenate(a[i]) for i in range(a.shape[0])])
                p = pd.DataFrame(concat.reshape(a.shape[0], -1).T, idx, columns)
                return pd.concat([df.drop(columns, axis=1), p], axis=1).reset_index(drop=True)
            
        artist_info = explode(artist_info, ['song','song_playcount','song_listeners'])

        self.log.info("Saving data to s3")

        bytes_to_write = artist_info.to_csv(sep=';').encode()
        fs = s3fs.S3FileSystem(key=AWS_ACCESS_KEY_ID, secret=AWS_SECRET_ACCESS_KEY)
        with fs.open('s3://mrazakazmi-data-project/songs.csv', 'wb') as f:
            f.write(bytes_to_write)

        self.log.info("Data saved")

