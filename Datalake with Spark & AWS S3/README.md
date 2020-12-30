
# Project - Data Lake
A music streaming startup called Sparkify has grown their user base and song database even more. 
Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON log data on the songs in their app.

In this project, we will build an ETL pipeline for a data lake hosted on S3. We will load data from S3, process the data into analytics tables using Spark, and load them back into S3. We will deploy this Spark process on a cluster using AWS.

## Deployement

File `dl.cfg` contains :


```
KEY=YOUR_AWS_ACCESS_KEY
SECRET=YOUR_AWS_SECRET_KEY
```

## ETL Pipeline
    
1.  Read data from S3
    
    -   Song data:  `s3://udacity-dend/song_data`
    -   Log data:  `s3://udacity-dend/log_data`
    
    The script reads song_data and load_data from S3.
    
3.  Process data using spark
    
    Transforms them to create five different tables listed below : 
    #### Fact Table
	 **songplays**  - records in log data associated with song plays i.e. records with page  `NextSong`
    -   songplay_id, start_time, userId, level, song_id, artist_id, session_id, location, userAgent

	#### Dimension Tables
	 **users**  - users in the app
		Fields -   userId, firstName, lastName, gender, level
		
	 **songs**  - songs in music database
    Fields - song_id, title, artist_id, year, duration
    
	**artists**  - artists in music database
    Fields -   artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    
	  **time**  - timestamps of records in  **songplays**  broken down into specific units
    Fields -   start_time, hour, day, week, month, year, weekday
    
4.  Load it back to S3
    
    Writes them to partitioned parquet files in table directories on S3.
