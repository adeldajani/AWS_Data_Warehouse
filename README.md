# Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to. You'll be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.

# Project Description
In this project, you'll apply what you've learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, you will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

# Project Datasets
You'll be working with two datasets that reside in S3. 

The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. 

The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

The log files in the dataset you'll be working with are partitioned by year and month. 

# Files
**1-create_tables.py: drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.**

**2-sql_queries.py: contains all your sql queries, and is imported into the last three files above.**

**3-create_cluster.ipynb:Create IAM role, Redshift cluster, and allow TCP connection from outside VPC.
Pass --delete flag to delete resources**

**4-etl.py: reads and processes files from song_data and log_data and loads them into your tables. You can fill this out based on your work in the ETL notebook.**

**5-dwh.cfg: Configure Redshift cluster and data import.**

# Run Scripts
Set environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.

Choose DB/DB_PASSWORD in dhw.cfg.

Create IAM role, Redshift cluster, and configure TCP connectivity

$ python create_cluster.py

Complete dwh.cfg with outputs from create_cluster.py

CLUSTER/HOST
IAM_ROLE/ARN
Drop and recreate tables

$ python create_tables.py

Run ETL pipeline

$ python etl.py

Delete IAM role and Redshift cluster

$ python create_cluster.py --delete