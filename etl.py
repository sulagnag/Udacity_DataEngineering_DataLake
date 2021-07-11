import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format , dayofweek, row_number 
from pyspark.sql.types import TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['Keys']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['Keys']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
        .config("spark.sql.parquet.filterPushdown", "true") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = os.path.join(input_data,"song_data/*/*/*/*.json")
    
    # read song data file
    df = spark.read.json(song_data)
    df.createOrReplaceTempView("song_all_data")
    
    # extract columns to create songs table
    songs_table = spark.sql("select song_id, title, artist_id,year, duration from song_all_data")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year','artist_id').mode("overwrite").parquet(output_data +"/songs.parquet")

    # extract columns to create artists table
    artists_table = spark.sql("select artist_id, artist_name, artist_location,artist_latitude, artist_longitude from song_all_data")
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + "/artist.parquet")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data =os.path.join(input_data,"log_data/*/*/*.json")

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df.createOrReplaceTempView("all_logdata")
    data=spark.sql("SELECT * from all_logdata where page = 'NextSong'")
    data.createOrReplaceTempView("all_data")
    
    # extract columns for users table    
    users_table = spark.sql("SELECT DISTINCT userId, firstName, lastName, gender, level from all_data")
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(output_data+"/userdata.parquet")

    # create timestamp column from original timestamp column
    spark.udf.register("get_timestamp", lambda x: datetime.fromtimestamp(x/1000.0), TimestampType())
    df=spark.sql("select *,get_timestamp(ts) as timestamp from all_data")
    df.createOrReplaceTempView("all_data_new")
    
    # create datetime column from original timestamp column
   # spark.udf.register("get_datetime", lambda x: date_format(x))
   # df = 
    
    # extract columns to create time table
    time_table = spark.sql("""select distinct timestamp, 
                              hour(timestamp) AS hour,  
                              dayofmonth(timestamp) AS day,
                              weekofyear(timestamp) AS week,
                              month(timestamp) AS month,
                              year(timestamp) AS year,
                              CASE WHEN dayofweek(timestamp) NOT IN (1, 7) THEN true ELSE false END AS is_weekday
                              from all_data_new""")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year','month').mode("overwrite").parquet(output_data + '/timedata.parquet')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "/songs.parquet")
    song_df.createOrReplaceTempView("songs_table")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""SELECT row_number() over(order by timestamp) as songplay_id, 
                                timestamp as start_time, 
                                userId, 
                                level,
                                song_id, 
                                artist_id, 
                                sessionId,
                                location, 
                                userAgent ,
                                year(timestamp) as year ,
                                month(timestamp) as month 
                                FROM all_data_new se
                                LEFT JOIN songs_table ss
                                ON se.song=ss.title AND
                                se.length=ss.duration   
                                """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year','month').mode("overwrite").parquet(output_data+'/songplay.parquet')


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sulagna-output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
