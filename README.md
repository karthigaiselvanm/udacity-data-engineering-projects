# udacity-data-engineering-projects
Few sample projects related to Udacity Data Engineer Program including Data modeling in Postgres &amp; Apache Cassandra, Setting up a Cloud Data Warehouse, Creating a data lake using Spark &amp; Data pipeline Setup using Apache Airflow

## Project 1: Data Modeling with Postgres
In this project, we apply Data Modeling with Postgres and build an ETL pipeline using Python. A startup wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. Currently, they are collecting data in json format and the analytics team is particularly interested in understanding what songs users are listening to.

Link: [Data_Modeling_with_Postgres](https://github.com/karthigaiselvanm/udacity-data-engineering-projects/tree/main/Data%20Modeling%20with%20Postgres)

## Project 2: Data Modeling with Cassandra
In this project, we apply Data Modeling with Cassandra and build an ETL pipeline using Python. We will build a Data Model around our queries that we want to get answers for. 
For our use case we want below answers: 

 - Get details of a song that was herad on the music app history during a particular session. 
 - Get songs played by a user during particular session on music app. 
 - Get all users from the music app history who listened to a particular song.

Link : [Data_Modeling_with_Apache_Cassandra](https://github.com/karthigaiselvanm/udacity-data-engineering-projects/tree/main/Data%20Modeling%20with%20Apace%20Cassandra)

## Project 3: Data Warehouse with Amazon Redshift
In this project, we'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. 
For our use case we want below answers: 

 - Create tables structures based on the star schema with a fact table 'songplays' and dimension tables user_dim, songs_dim, artists_dim, time_dim
 - Build ETL pipelines to load these fact & dimension tables
 - Document the purpose of this database and the dimensional modeling of it.
  
Link : [Data_Warehouse with Amazon Redshift](https://github.com/karthigaiselvanm/udacity-data-engineering-projects/tree/main/Datawarehouse%20with%20AWS%20Redshift)

## Project 5: Data Lake with Spark
In this project, we'll apply what you've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.
For our use case we want below answers: 

 - Load data from S3 bucket
 - Process the data into analytics tables using Spark
 - Load them back into another S3 bucket.
 
 Link : [Data_Lake with Spark & AWS S3](https://github.com/karthigaiselvanm/udacity-data-engineering-projects/tree/main/Datalake%20with%20Spark%20%26%20AWS%20S3)
 
## Project 4: Data Pipelines with Apache Airflow
To complete the project, we need to create our own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

We need to update the following four template operators provided with the steps required.
- stage_redshift.py - Steps for loading data from S3 bucket to Amazon Redshift
- load_fact.py - Steps for loading the fact table songplays_fact from the staging table stage_songs
- load_dimension.py - Steps for loading four dimension tables such as dim_users, dim_time, dim_songs and dim_artists
- data_quality.py - Steps for checking the Data Quality of the loaded tables

You'll be provided with a helpers class that contains all the SQL transformations. Thus, you won't need to write the ETL yourselves, but you'll need to execute it with your custom operators.

Link : [Data_Pipelines with Apache Airflow](https://github.com/karthigaiselvanm/udacity-data-engineering-projects/tree/main/Data%20Pipelines%20with%20Apache%20Airflow)
