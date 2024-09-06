import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3

##setting spark context and using glue context as we use AWS in this 
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
from pyspark.sql.functions import explode,col,to_date
from datetime import datetime
from awsglue.dynamicframe import DynamicFrame

#getting the raw data stored in S3
s3_path="s3://spotify-spark/raw_data/to_be_processed/"
source_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [s3_path]
    },
    format="json"
)
spotify_df=source_df.toDF()

sdf=spotify_df

#function to process album related data
def process_album(df):
        album_df=df.withColumn("items",explode("items")).select(
        col("items.track.album.id").alias("album_id"),
        col("items.track.album.name").alias("album_name"),
        col("items.track.album.release_date").alias("album_release_date"),
        col("items.track.album.total_tracks").alias("total_tracks"),
        col("items.track.album.external_urls.spotify").alias("url")).drop_duplicates(["album_id"])
        return album_df

#function to process artist related data    
def process_artist(df):
        ##added an additional step to explode fro artists, because if tried directly to explode and extract data like did for album and songs the values were returned as an array.
        ## so additional steps were added
        artist_exploded=df.select(explode(col("items")).alias("item")).select(explode(col("item.track.artists")).alias("artist"))
        artist_df=artist_exploded.select(
        col("artist.id").alias("artist_id"),
        col("artist.name").alias("artist_name"),
        col("artist.external_urls.spotify").alias("external_urls")).drop_duplicates(["artist_id"])
        return artist_df

#function to process songs related data    
def process_songs(df):
        songs_df=df.withColumn("items",explode("items")).select(
        col("items.track.id").alias("song_id"),
        col("items.track.name").alias("song_name"),
        col("items.track.duration_ms").alias("duration_ms"),
        col("items.track.external_urls.spotify").alias("url"),
        col("items.track.popularity").alias("popularity"),
        col("items.added_at").alias("song_added_date"),
        col("items.track.album.id").alias("album_id"),
        col("items.track.artists")[0]["id"].alias("first_artist_id")).drop_duplicates(["song_id"])
        
        songs_df=songs_df.withColumn("song_added_date",to_date(col("song_added_date")))
        return songs_df

#passing the spark dataframe to process album and extract album data
album_df=process_album(sdf)

#passing the spark dataframe to process album and extract song data
songs_df=process_songs(sdf)

#passing the spark dataframe to process album and extract artist data
artist_df=process_artist(sdf)

#function to add the data into AWS S3 bucket
def write_to_s3(df,path_suffix,format_type="csv"):
    dynamic_frame=DynamicFrame.fromDF(df,glueContext,"dynamic_frame")
    
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path":f"s3://spotify-spark/transformed_data/{path_suffix}/"},
        format=format_type
    )

#Writing the processed data into assigned files in the s3 bucket
write_to_s3(album_df,f"album/album_transformed_{datetime.now().strftime('%Y-%m-%d')}",'csv')
write_to_s3(artist_df,f"artist/artist_transformed_{datetime.now().strftime('%Y-%m-%d')}",'csv')
write_to_s3(songs_df,f"songs/songs_transformed_{datetime.now().strftime('%Y-%m-%d')}",'csv')

def list_s3_objects(bucket,prefix):
    s3_client=boto3.client('s3')
    response=s3_client.list_objects_v2(Bucket=bucket,Prefix=prefix)
    keys=[content['Key'] for content in response.get('Contents',[]) if content['Key'].endswith('.json')]
    return keys

    
bucket_name="spotify-spark"
prefix="raw_data/to_be_processed/"
spotify_keys=list_s3_objects(bucket_name,prefix)

def move_and_delete_files(spotify_keys,Bucket):
    s3_resource=boto3.resource('s3')
    for key in spotify_keys:
        copy_source={
            'Bucket': Bucket,
            'Key':key
        }
        
        destination_key= "raw_data/processed/" +key.split("/")[-1]
        
        s3_resource.meta.client.copy(copy_source,Bucket,destination_key)
        
        s3_resource.Object(Bucket,key).delete()

move_and_delete_files(spotify_keys, bucket_name)

job.commit()