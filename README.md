# Udacity_DataEngineering_DataLake
Build a Data Lake and ETL piepline in spark that loads data from S3, processes the data into analytics tables and loads them back into S3

## Original Data
The original data in the S3://udacity-dend bucket has 2 folders
1. song data
2. log data

A json file from the song data looks as follows:

{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}

A json file from the log Data looks like follows:

![log-data](images/log-data.png)

## Processing using Spark SQL into Analytics tables
We need to process the song and log data and create the following tables
1. The songplay_table will hold information about user sessions, the songs played along with duration and access device (timestamp, user id, level, song id, artist id, session id, user location and access device)
2. The users table will hold information about the users (user id, first name, last name, gender, and level (of subscription))
3. The songs table will hold information about the songs (song ID, song name (title), artist id, year & duration of song)
4. The artist table will hold information abou the artists (artist id, name, location, latitude and longitude)
5. The time table will hold information about all the times all users accessed the songs, with the timestamp saved along with hr, day, week, month, year format.

All the new tables are stored as parquet files.
1. Users table is partitioned by year and month
2. Songplay table is partitioned by year and month

## Getting started
1. Create IAM user role with Administrator priviledges
2. Create SSH Key Pair
3. Create EMR clusters with Spark, Hadoop, Yarn, Jupyter Notebook , use the created SSH key 
4. Update Security Group for master to allow incoming SSH connection
5. SCP files to the EMR master
6. SSH to the master
7. Create S3 bucket where the analytics tables will be stored
8. spark-submit the etl file
9. The original data was already in an S3 bucket. If not then we need to create an S3 bucket and upload the data
