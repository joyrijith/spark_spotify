import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
#boto3 is the package created by AWS to access AWS services
import boto3
from datetime import datetime
import pytz


def lambda_handler(event, context):
    

    
    #storing the credentials to access spotify api in environment variable and accessing these credentials from the environment variables
    client_id=os.environ.get('client_id')
    client_secret=os.environ.get('client_secret')
    
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id , client_secret=client_secret)
    sp=spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    
    #getting current timezone date and time
    utc_now = datetime.now(pytz.utc)
    local_timezone = pytz.timezone('America/Edmonton')  
    local_date = utc_now.astimezone(local_timezone).date()
    local_time = utc_now.astimezone(local_timezone).time()
    
    
    #Playlist link from spotify that you want to analyze
    #this playlist gets updated everyday
    playlist_link = "https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M"
    playlist_uri=playlist_link.split("/")[-1]
    data=sp.playlist_tracks(playlist_uri)
    
    filename = "spotify_raw_dataset"+"_" + str(local_date) +" " +str(local_time)+".json"
    
    #creating connection with s3 and storing files in the respective location
    client=boto3.client('s3')
    client.put_object(
      Bucket="spotify-spark",
      Key="raw_data/to_be_processed/"+filename,
      Body=json.dumps(data)
      )
      
    #creating glue connection
    glue=boto3.client("glue")

    #name of the job given in AWS glue
    gluejobname="spark_spotify_job"
    
    #Triggering glue job once data is available in s3 storage 
    try:
        runId= glue.start_job_run(JobName=gluejobname)
        status= glue.get_job_run(JobName=gluejobname,RunId=runId['JobRunId'])
        print("Job Status : ",status['JobRun']['JobRunState'])
        
    except Exception as e:
      print(e)