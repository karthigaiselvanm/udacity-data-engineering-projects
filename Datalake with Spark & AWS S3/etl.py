import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import *
from pyspark.sql import types as T

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
     """
    Description: Process the songs data files and extract data into songtable and artisttable.
    parameter 1: spark: a spark session instance
    parameter 2: input_data: input file path
    parameter 2: output_data: output file path
    """

    # get filepath to song data file
    songdata = input_data + "song_data/*/*/*/*"

    # read song data file
    df = spark.read.json(songdata, mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record').drop_duplicates()

    # extract columns to create songs table
    songtable = df.select("song_id","title","artist_id","year","duration").drop_duplicates()

    # write songs table to parquet files partitioned by year and artist
    songtable.write.parquet(output_data + "songs/", mode="overwrite", partitionBy=["year","artist_id"])

    # extract columns to create artists table
    artisttable = df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude").drop_duplicates()

    # write artists table to parquet files
    artisttable.write.parquet(output_data + "artists/", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    Description: Process the event log file and extract data into tabletime and songplay.
    :param spark: a spark session instance
    :param input_data: input file path
    :param output_data: output file path
    """

    # get filepath to log data file
    logdata = os.path.join(input_data, "log-data/")

    # read log data file
    df = spark.read.json(logdata, mode='PERMISSIVE', columnNameOfCorruptRecord='corrupt_record').drop_duplicates()

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table
    usertable = df.select("userId","firstName","lastName","gender","level").drop_duplicates()

    # write users table to parquet files
    usertable.write.parquet(os.path.join(output_data, "users/") , mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), T.TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))

    # extract columns to create time table
    timetable = df.withColumn("hour",hour("start_time"))\
                    .withColumn("day",dayofmonth("start_time"))\
                    .withColumn("week",weekofyear("start_time"))\
                    .withColumn("month",month("start_time"))\
                    .withColumn("year",year("start_time"))\
                    .withColumn("weekday",dayofweek("start_time"))\
                    .select("ts","start_time","hour", "day", "week", "month", "year", "weekday").drop_duplicates()

    # write time table to parquet files partitioned by year and month
    timetable.write.parquet(os.path.join(output_data, "time_table/"), mode='overwrite', partitionBy=["year","month"])

    # read in song data to use for songplays table
    song_df = spark.read\
                .format("parquet")\
                .option("basePath", os.path.join(output_data, "songs/"))\
                .load(os.path.join(output_data, "songs/*/*/"))

    # extract columns from joined song and log datasets to create songplays table
    songplaytable = df.join(song_df, df.song == song_df.title, how='inner')\
                        .select(monotonically_increasing_id().alias("songplay_id"),col("start_time"),\
                        col("userId"),"level","song_id","artist_id", col("sessionId"), "location", col("userAgent"))

    songplaytable = songplaytable.join(time_table, songplays_table.start_time == time_table.start_time, how="inner")\
                        .select("songplay_id", songplays_table.start_time, "userId", "level", "song_id", "artist_id", "sessionId", "location", "userAgent", "year", "month")

    # write songplays table to parquet files partitioned by year and month    
    songplaytable.drop_duplicates().write.parquet(os.path.join(output_data, "songplays/"), mode="overwrite", partitionBy=["year","month"])


def main():
    spark = create_spark_session()
    songPath = 's3a://udacity-dend/'
    logPath = 's3a://udacity-dend/'
    output_data = 's3a://dendlake/'
    
    process_song_data(spark, songPath, output_data)    
    process_log_data(spark, logPath, output_data)


if __name__ == "__main__":
    main()