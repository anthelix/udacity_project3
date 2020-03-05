##### Udacity Data Engineering Nanodegree

<img alt="" align="right" width="150" height="150" src = "./image/aws_logo.png" title = "aws logo" alt = "aws logo">  
</br>
</br>
</br>

# Project 3 : Data Warehouse

About an ETL pipeline that extracts data from S3, stages them in Redshift, and transforms data into a set of dimensional tables. 

### Table of contents

   - [About the project](#about-the-project)
   - [Purpose](#purpose)
   - [Getting started](#getting-started)
       - [Dataset](#dataset)
       - [To run](#To-run)
   - [Worflow](#worflow)
        - [Get the data](#Get-the-data)
        - [Create a IAM user in redshift with your AWS](#Create-a-IAM-user-in-redshift-with-your-AWS)
        - [Complete the `dwh.cfg` with parameters](#Complete-the-`dwh.cfg`-with-parameters)
        - [Create the cluster](#Create-the-cluster)
        - [Create tables](#Create-tables)
        - [ETL](#ETL)
        - [Sparkify Analytical](#Sparkify-Analytical)
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
First, Postgres and Anaconda are installed on your computer then we will create a Python environment and its dependencies customized to this project, then create cluster and launch ETL

Steps one-by-one
   
0. Create environment to work in Anaconda
    * run `conda create --quiet --yes --name <Your env name> psycopg2 boto3 configparser numpy pandas tabulate botocore`
    * then `conda install --yes -c conda-forge ipython-sql`
    * to open do `conda activate <Your env name>
    * to close `conda deactivate`
    * to remove the environment `conda remove --name myenv --all`
    * and check `conda info  --envs`
    * go in the `sparkify.py` folder

1. Update KEY AND SECRET and your IP(or default 0.0.0.0\0) in dwh.cfg  
    
2. Create Redshift Cluster and all tables
    run `python sparkify.py`

3. Load data in staging tables and insert data in fact and dimensions tables
    run `python etl.py`
    
4. Test, explore data and run queries with `sparkify_analytic.ypnb` open in Jupyter Notebook

5. In you want remove and re-create the tables, run `create_tables.py`
    
3. Delete Redshift Cluster and Iam role
    run `python delete_cluster.py`

## Worflow

### Get the data

* Explore data on 'S3 bucket udacity-dend'. 
    * song_data contains one record(385253 rows), log_data contains few record(31 rows)
    * get the columns name in song_data  
        ```
        ['song_id"', '"num_songs"', '"title"', '"artist_name"', '"artist_latitude"', '"year"', '"duration"', '"artist_id"', '"artist_longitude"', '"artist_location"']
        ```  
    * get the columns name in log_data  
        ```
        ['artist"', 'auth"', 'firstName"', 'gender"', 'itemInSession"', 'lastName"', 'length"', 'level"', 'location"', 'TX', 'method"', 'page"', 'registration"', 'sessionId"', 'song"', 'status"', 'ts"', 'userAgent"', 'userId"']
        ```
### Create a IAM user in redshift with your AWS 
* "Access by programming"
* "Attach existing policies directly"
* "AmazonRedshiftFullAccess" 
* keep the "Access key Id" and " Secret access key"

### Complete the `dwh.cfg` with parameters
```
[AWS]
KEY=<Your Access key Id>
SECRET=<Your Secret access key>

[IAM_ROLE]
DWH_IAM_ROLE_NAME=dwhRole
...
```
### Create the cluster
* Create IAM Role
* Create Redshift Cluster
* Open ports

### Create tables
* Drop tables if exists
* Create 2 staging tables to load data because Amazon Redshift doesn't support upsert(update or insert). Then join the staging tables in different tables. 
    * `staging_events` with the log_data files
    * `staging_songs` with the song_data files

* Create dimensions tables 
    * `dimUser`, `dimArtist`, `dimSong` with their PRIMARY KEY as SORTKEY
    * `dimTime` with its PRIMARY KEY as SORTKEY AND as DISTKEY
        Example :
    ```
    time_table_create = ("""CREATE TABLE IF NOT EXISTS dimTime
    (
        start_time timestamp NOT NULL PRIMARY KEY DISTKEY SORTKEY,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday varchar
        )
    """)
    ```
* Create a fact tables `factSongplay` `artist_id` as SORTKEY and `start_time` as DISTKEY

I choose `artist_id` as SORTKEY for the factSongplay table because I need twice for my analytical queries and  `start_time` as DISTKEY for a better distribution on the slices.

|schema 	|table 	|tableid 	|distkey 	|skew 	|sortkey 	|#sks 	|mbytes 	|pct_of_total 	|enc 	|rows 	|unsorted_rows |pct_unsorted
|-|-|-|-|-|-|-|-|-|-|-|-|-|
|public 	|factsongplay 	|100526 	|start_time 	|1.000000| 	artist_id| 	1 	|192 |	0.020000 |	y |	333| 	0.000000| 	0.000000|
|public| 	staging_events |	100524 	|None |	1.000000 |	None |	0 |	168 |	0.020000 	|y |	8056 |	nan |	nan
|public |	dimtime| 	100543 	|start_time |	1.000000 |	start_time |	1 |	160 |	0.020000 	|y 	|8023 	|0.000000 |	0.000000
|public |	dimartist| 	100539 	|None |	1.000000 |	artist_id |	1 |	128 |	0.010000 |	y |	10025 |	0.000000 |	0.000000|
|public |	dimuser |	100531| 	None |	1.000000 |	user_id |	1 |	128 |	0.010000 |	y |	105 |	0.000000 |	0.000000
|public |	dimsong |	100535 |	None |	1.000000 |	song_id |	1 |	128 |	0.010000 |	y |	14896 |	0.000000 |	0.000000
|public |	staging_songs| 	100385 |	None |	1.000000 |	None |	0 |	104 	|0.010000 |	y |	74480 |	nan |	nan

#####  Stagings Tables

|*staging_events*|*staging_songs*|       
|:-|:-|
|artist|num_songs
|auth|artist_id|
|firstName|artist_latitude|
|gender|artist_longitude|
|itemInSession|artist_location|
|lastName|artist_name|
|length|song_id|
|level|title|
|location|duration|
|method|year|
|page|
|registration|
|sessionId|
|song|
|status|
|ts|
|userAgent|
|userId|

##### Fact Table

|*songplay*|*Reference Table*|
|:-|:-|
|songplay_id|PRIMARY KEY|
|**start_time**|time table|
|**user_id**|user table|
|level||
|**song_id**|song table|
|**artist_id**|artist table|
|session_id||
|location||
|user_agent||

##### Dimension  Tables

|*user*||*song*||*artist*||*time*||
|:-|:-|:-|:-|:-|:-|:-|:-|
|**user_id**|Primary KEY|**song_id**|PRIMARY KEY|**artist_id**|PRIMARY KEY|**start_time**|PRIMARY KEY|
|first_name||title||name||hour||
|last_name||artist_id||location||day||
|gender||year||latitude||week||
|level||duration||longitude||month||
|||||||year
|||||||weekday

### ETL
* Connect to the database in the cluster
* COPY to load data in staging tables
Example:
```
staging_songs_copy = (""" copy staging_songs 
    from 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={}'
    region 'us-west-2' compupdate off 
    JSON 'auto' truncatecolumns;
""").format(config.get('IAM_ROLE', 'ARN'))
```
* INSERT data in fact and Dim tables
Example: Query to insert data in `dimUser`
```
user_table_insert = ("""INSERT INTO dimUser(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId AS user_id,
                    firstName AS first_name,
                    lastName AS last_name,
                    gender,
                    level
    FROM staging_events
    WHERE user_id IS NOT NULL;
```

### Sparkify Analytical
* Connect to the database
* Explore data in the Udacity-dend Bucket
* Explore and check staging_songs and staging_events
* Queries
    * Explre dimTables and FactTables
    * Queries for the analytic team
    * Queries to play with Sortkey and Distkey

Example: The average number of sessions per week per user
```
query = """
    SELECT user_id, AVG(Events) AS AVG_sessionUserWeek
    FROM (SELECT t.week AS Week,
                 sp.user_id,
                 COUNT(*) AS Events    
        FROM factSongplay AS sp
        JOIN dimTime as t
        ON sp.start_time=t.start_time
        GROUP BY 1, 2) sub
    GROUP BY user_id    
    ORDER BY AVG_sessionUserWeek DESC
    LIMIT 5;
"""
pd.read_sql(query, conn_string).style.hide_index()
```

|user_id	|avg_sessionuserweek
|-|-|
|49	|8
|97	|8
|80	|7
|44	|5
|88 |5


## Web-links
[Table distribution by Blendo](https://www.blendo.co/amazon-redshift-guide-data-analyst/data-modeling-table-design/table-distribution-styles/)
[AWS Example of distribution Key](https://docs.aws.amazon.com/fr_fr/redshift/latest/dg/c_Distribution_examples.html)
[Selecting SortedKey](https://docs.aws.amazon.com/fr_fr/redshift/latest/dg/t_Sorting_data.html)
[AWS Example Selecting Sorted Key](https://docs.aws.amazon.com/fr_fr/redshift/latest/dg/t_Sorting_data-compare-sort-styles.html)
[How to load Data in Amazon Redshift](https://www.blendo.co/blog/how-to-load-data-from-mixpanel-to-redshift/
)
[Data Types](https://www.oreilly.com/library/view/high-performance-mysql/9781449332471/ch04.html)
[Catch boto3 exceptions](https://www.oreilly.com/library/view/high-performance-mysql/9781449332471/ch04.html)


