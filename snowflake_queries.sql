--Creating database for this project
CREATE DATABASE spotify_db;

--Creating Storage connection for the project
create or replace storage integration s3_spark_spotify
TYPE=EXTERNAL_Stage
STORAGE_Provider=S3
ENABLED=TRUE
Storage_AWS_ROLE_ARN='arn:aws:iam::011528264778:role/snowflake_spark_Aws_connection'
Storage_allowed_locations=('s3://spotify-spark/')
Comment='S3 storage integration for spark project';

-- Getting info on the integration created.
Desc integration s3_spark_spotify;

--Creating schema for placing the file format
Create schema spotify_db.spotify_spark_file_format

--Creating schema for placing the external connect
Create schema spotify_db.spotify_external_stage_connections

--Creating a file format for the project
CREATE OR REPLACE FILE FORMAT spotify_db.spotify_spark_file_format.spark_ff
TYPE=CSV,
FIELD_DELIMITER=",",
SKIP_HEADER=1,
null_if=('NULL','null'),
empty_field_as_null=TRUE;


--Creating a Secure stage connection
CREATE OR REPLACE STAGE spotify_db.spotify_external_stage_connections.spotify_spark_stage
URL='s3://spotify-spark/transformed_data/'
STORAGE_INTEGRATION=s3_spark_spotify
FILE_FORMAT= spotify_db.spotify_spark_file_format.spark_ff;

--getting details about the stage connection 
desc stage spotify_db.spotify_external_stage_connections.spotify_spark_stage

--Validating that the connection is created and able to see the files in snowflake
list @spotify_db.spotify_external_stage_connections.spotify_spark_stage/songs


--Creating schema for tables
Create schema spotify_db.tables

--Creating Album table
CREATE OR REPLACE TABLE spotify_db.tables.tbl_album (
    album_id STRING,
    album_name STRING,
    album_release_date DATE,
    total_tracks INT,
    url STRING

);

--Creating Artists table
CREATE OR REPLACE TABLE spotify_db.tables.tbl_artists (
    artist_id STRING,
    artist_name STRING,
    external_urls STRING
);

--Creating Songs table
CREATE OR REPLACE TABLE spotify_db.tables.tbl_songs (
    song_id STRING,
    song_name STRING,
    duration_ms INT,
    url STRING,
    popularity INT,
    song_added_date DATE,
    album_id STRING,
    first_artist_id STRING
);

--Query to test if data can be copied into the snowflake table from AWS s3
COPY INTO tbl_album
FROM @spotify_db.spotify_external_stage_connections.spotify_spark_stage/album/album_transformed_2024-09-03/run-1725333223444-part-r-00000;

COPY INTO tbl_artists
FROM @spotify_db.spotify_external_stage_connections.spotify_spark_stage/artist/artist_transformed_2024-09-03/run-1725333310381-part-r-00000

COPY INTO tbl_songs
FROM @spotify_db.spotify_external_stage_connections.spotify_spark_stage/songs/songs_transformed_2024-09-03/run-1725333314497-part-r-00000
FILE_FORMAT= (FORMAT_Name= spotify_db.spotify_spark_file_format.spark_ff)
ON_ERROR='CONTINUE';


--- Creating snowpipe schema
CREATE OR REPLACE SCHEMA spotify_db.pipe

--Creating snowpipe for songs table
CREATE OR REPLACE PIPE spotify_db.pipe.tbl_songs_pipe
auto_ingest=TRUE
AS
COPY INTO spotify_db.tables.tbl_songs
FROM @spotify_db.spotify_external_stage_connections.spotify_spark_stage/songs/;

--Creating snowpipe for album table
CREATE OR REPLACE PIPE spotify_db.pipe.tbl_album_pipe
auto_ingest=TRUE
AS
COPY INTO spotify_db.tables.tbl_album
FROM @spotify_db.spotify_external_stage_connections.spotify_spark_stage/album/;

--Creating snowpipe for artist table
CREATE OR REPLACE PIPE spotify_db.pipe.tbl_artist_pipe
auto_ingest=TRUE
AS
COPY INTO spotify_db.tables.tbl_artists
FROM @spotify_db.spotify_external_stage_connections.spotify_spark_stage/artist/;

--Describing the songs table to get the SNS queue connection details to setup the snowpipe
DESC pipe tbl_songs_pipe;

--Describing the songs table to get the SNS queue connection details to setup the snowpipe
DESC pipe tbl_artist_pipe;

--Describing the songs table to get the SNS queue connection details to setup the snowpipe
DESC pipe tbl_album_pipe;

-- Query to get the logs of the snowpipe running
--This is helpful to view the logs in case of any failures in the pipe
Select system$pipe_status('spotify_db.pipe.tbl_songs_pipe')

