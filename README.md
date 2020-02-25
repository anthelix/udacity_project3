##### Udacity Data Engineering Nanodegree

<img alt="" align="right" width="150" height="150" src = "./image/aws_logo.png" title = "aws logo" alt = "aws logo">  
</br>
</br>
</br>

# Project 3 : Data Warehouse

About an ETL pipeline that extracts data from S3, stages them in Redshift, and transforms data into a set of dimensional tables. 

### Todo
[Udacity: Project Instructions](https://classroom.udacity.com/nanodegrees/nd027/parts/69a25b76-3ebd-4b72-b7cb-03d82da12844/modules/58ff61b9-a54f-496d-b4c7-fa22750f6c76/lessons/b3ce1791-9545-4187-b1fc-1e29cc81f2b0/concepts/14843ffe-212c-464a-b4b6-3f0db421aa32)
* set an anaconda environement(python3, psycopg2, configparsar, sql_queries) to work in spyder? 
* vois si meme fichier que dams le projet 1
* suivre le worflow dans le project instructions.

[Redshift Create Table Docs.](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html)

Note
The SERIAL command in Postgres is not supported in Redshift. The equivalent in redshift is IDENTITY(0,1), which you can read more on in the Redshift Create Table Docs.



### Table of contents

   - [About the project](#about-the-project)
   - [Purpose](#purpose)
   - [Getting started](#getting-started)
       - [Dataset EventData](#dataset-eventdata)
   - [Worflow](#worflow)
   - [Workspace](#workspace)
      - [My environnemets](#my-environements)
      - [Discuss about the database](#discuss-about-the-database)
      - [UML diagram](#uml-diagram)
      - [Chebotko diagram](#chebotko-diagram)
      - [Queries](#queries)
      - [Web-links](#web-links)
---

## About the project

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.  
They'd like a data engineer to build an ETL pipeline that extracts their data from S3, stages them in Redshift, nd transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. 

## Purpose

The purpose of this project is to implemeting data warehouse and build an ETL pipeline for a database hosted on Redshift. Project uses AWS S3 and AWS Redshift. 
* Load data from S3 to staging tables on Redshift
* Execute SQL statements that create the analytics tables from these staging tables

## Getting started

### Dataset

The twice datasetset reside in S3:  

* `Song data: `s3://udacity-dend/song_data`
* `Log data: s3://udacity-dend/log_data`

Log data json path: `s3://udacity-dend/log_json_path.json`

##### Song Dataset

The first dataset is a subset of real data from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. 
```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```
And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.
```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```
##### Log Dataset

The second dataset consists of log files in JSON format generated by this event simulator based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.
The log files in the dataset you'll be working with are partitioned by year and month. 
```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```
And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.
![log dataset image](./image/log_dataset.png)



### To Run
Steps one-by-one

    1. Update KEY AND SECRET in dwh.cfg
    
    2. Create Redshift Cluster
        `run commnad avoir` (see My environem
    


## Worflow

1. **Create Table Schemas**

    * Design schemas for fact and dimension tables
    * Write a SQL `CREATE` statement for each tables in `sql_queries.py`
    * Complete `create_tables.py` to connect the database and create these tables
    * Write SQL `DROP` statements
    * Launch a redshift cluster
    * Create an IAM role for acces to s3
    * Add Redshift database and IAM role info in dwh.cfg
    * Test by running `create_tables.py`

2. **Build ETL Pipeline**  

    * Implement to load data from s3 to staging tables on Redshift
    * Implement to load data staging tables to analytics tables on Redshift
    * Test
    * Delete the Redshift Cluster when finished

3. **Document Process**

* Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.
* State and justify database schema design and ETL pipeline.
* Provide example queries and results for song play analysis.s

## Workspace

### My environnemets

### Discuss about the database

### UML diagram

### Chebotko diagram

### Queries

### Web-links
