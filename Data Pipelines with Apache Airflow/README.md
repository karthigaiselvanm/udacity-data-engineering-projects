## Introduction
A music streaming company, Sparkify, has decided that it is time to introduce 
more automation and monitoring to their data warehouse ETL pipelines and come 
to the conclusion that the best tool to achieve this is Apache Air􀃓ow.

They have decided to bring you into the project and expect you to create high 
grade data pipelines that are dynamic and built from reusable tasks, can be 
monitored, and allow easy backfills. They have also noted that the data quality 
plays a big part when analyses are executed on top the data warehouse and want 
to run tests against their datasets after the ETL steps have been executed to
catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data 
warehouse in Amazon Redshift. The source datasets consist of JSON logs that 
tell about user activity in the application and JSON metadata about the songs 
the users listen to.

## Overview
Build an automated Apache Airflow process that runs hourly.  Utilize custom 
operators to load the staging tables, populate the fact table, populate the 
dimension tables, and perform data quality checks.

Configure the task dependencies so that after the dependencies are set, the
graph view displays ![Workflow DAG](./example-dag.png)

### Files:
`README.md`
:  this file

`example-dag.png`
:  image of DAG workflow

`dags/udac_example_dag.py`
:  Main DAG

`dags/create_tables.sql`
:  SQL script to build Redshift tables

`plugins/helpers/sql_queries.py`
:  *Select* statements used to populate tables

`plugins/operators/data_quality.py`
:  DAG operator used for data quality checks

`plugins/operators/load_dimensions.py`
:  DAG operator used to populate dimension tables in a STAR schema

`plugins/operators/load_fact.py`
:  DAG operator used to populate fact tables

`plugins/operators/stage_redshift.py`
:  DAG operator to populate staging tables from source files

### Prerequisites
1.  Start Redshift cluster
2.  Start Airflow, e.g. /opt/airflow/start.sh
3.  Open Airflow web browser and *create* the following:

        Airflow AWS Connection
                    Conn id:  aws_credentials
                    Login:  [REDACTED]
                    Password: [REDACTED]

        Airflow Redshift Connection
                    Conn id:  redshift
                    Conn Type:  Postgres
                    Host:   [REDACTED]
                    Schema: dev
                    Login:  awsuser
                    Password: [REDACTED]
                    Port: 5439

4.  From the DAGs page, click the *Trigger Dag* link for `udac_example`.
