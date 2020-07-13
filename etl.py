import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates a Spark Session
    :return: spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process all the songs data
    :param spark: spark seesion
    :param input_data: input data path
    :param output_data: output data path
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]) \
                    .dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(
        path=os.path.join(output_data, 'songs'),
        mode='overwrite',
        partitionBy=['year', 'artist_id']
    )

    # extract columns to create artists table
    df.createOrReplaceTempView("songs_table")
    artists_table = spark.sql(
        """ SELECT DISTINCT songs.artist_id, 
                            songs.artist_name, 
                            songs.artist_location, 
                            songs.artist_latitude, 
                            songs.artist_longitude
            FROM songs_table AS songs 
            WHERE song.artist_id IS NOT NULL
        """)
    
    # write artists table to parquet files
    artists_table.write.parquet(
        path=os.path.join(output_data, 'artists'),
        mode='overwrite'
    )


def process_log_data(spark, input_data, output_data):
    """
    Process all the log data
    :param spark: spark seesion
    :param input_data: input data path
    :param output_data: output data path
    """
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table
    df.createOrReplaceTempView("logs_table")
    users_table = spark.sql(
        """ SELECT DISTINCT logs.userId AS user_id, 
                            logs.firstName AS first_name, 
                            logs.lastName AS last_name, 
                            logs.gender AS gender, 
                            logs.level AS level 
            FROM logs_table AS logs 
            WHERE logs.userId IS NOT NULL
            ORDER BY last_name
        """)
    
    # write users table to parquet files
    users_table.write.parquet(
        path=os.path.join(output_data, 'users'),
        mode='overwrite'
    )

    # create timestamp column from original timestamp column
    df = df.withColumn("log_timestamp", F.to_timestamp(df.ts/1000))
    
    # create datetime column from original timestamp column
    df = df.withColumn("log_datetime", F.to_date(df.log_timestamp))
    
    # extract columns to create time table
    df.createOrReplaceTempView("logs_table")
    time_table = spark.sql(
        """ SELECT DISTINCT log_timestamp AS start_time,
                            hour(log_datetime) AS hour,
                            dayofmonth(log_datetime) AS day,
                            weekofyear(log_datetime) AS week,
                            month(log_datetime) AS month,
                            year(log_datetime) AS year,
                            dayofweek(log_datetime) AS weekday
            FROM logs_table
        """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(
        path=os.path.join(output_data, 'time'),
        mode='overwrite',
        partitionBy=['year', 'month']
    )

    # read in song data to use for songplays table
    song_df = spark.read.parquet(os.path.join(output_data, 'songs')).withColumnRenamed("artist_id", "songs_artist_id")

    # extract columns from joined song and log datasets to create songplays table
    songs_df.createOrReplaceTempView("songs_table")
    songplays_table = spark.sql(
        """  SELECT monotonically_increasing_id() AS songplay_id, 
                    logs.log_timestamp AS start_time, 
                    month(logs.log_datetime) AS month,
                    year(logs.log_datetime) AS year, 
                    logs.userId AS user_id, 
                    logs.level AS level, 
                    songs.song_id AS song_id, 
                    songs.artist_id AS artist_id, 
                    logs.sessionId AS session_id, 
                    logs.location AS location, 
                    logs.userAgent AS user_agent 
             FROM logs_table logs JOIN songs_table songs ON logs.artist = s.artist_name AND logs.song = s.title 
        """)

    # write songplays table to parquet files partitioned by year and month
    songplays_tabletime_table.write.parquet(
        path=os.path.join(output_data, 'songplays'),
        mode='overwrite',
        partitionBy=['year', 'month']
    )


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://dend-yuhou/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
