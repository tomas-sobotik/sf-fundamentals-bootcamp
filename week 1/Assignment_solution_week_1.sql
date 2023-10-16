/*
  03 UI Walktrough
  Basic DB objects creation
*/

--CITIBIKE database
CREATE DATABASE CITIBIKE;

--WORK schema
CREATE SCHEMA WORK;

--TRIPS table
create or replace table trips
(tripduration integer,
  starttime timestamp,
  stoptime timestamp,
  start_station_id integer,
  start_station_name string,
  start_station_latitude float,
  start_station_longitude float,
  end_station_id integer,
  end_station_name string,
  end_station_latitude float,
  end_station_longitude float,
  bikeid integer,
  membership_type string,
  usertype string,
  birth_year integer,
  gender integer);

---------------------------------------------------------------------------------

/* 05: Create first virtual warehouse */
use role SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH WITH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

/* Create a multicluster warehouse */
CREATE WAREHOUSE IF NOT EXISTS MULTI_COMPUTE_WH WITH 
  WAREHOUSE_SIZE = SMALL
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 3
  SCALING_POLICY = ECONOMY
  AUTO_SUSPEND = 180
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

---------------------------------------------------------------------------------
/*
06 Data loading principles
Create file format
*/

CREATE OR REPLACE FILE FORMAT FF_CSV
TYPE = CSV
FIELD_DELIMITER = ','
SKIP_HEADER = 0
FIELD_OPTIONALLY_ENCLOSED_BY = 
'"';

/* Create a stage */
CREATE OR REPLACE STAGE S_CITIBIKE_TRIPS
URL = 's3://snowflake-workshop-lab/citibike-trips-csv/'
FILE_FORMAT = ( FORMAT_NAME = FF_CSV );

list @s_citibike_trips;

---------------------------------------------------------------------------------
/* 
07 COPY command
LOADING FROM CSV file
*/

alter warehouse compute_wh set warehouse_size = large;


COPY into TRIPS from @S_CITIBIKE_TRIPS
ON_ERROR = SKIP_FILE;

alter warehouse compute_wh set warehouse_size = xsmall;

select count(*) from trips;

/* 
Working with PARQUET files
*/

--offloading data into the user stage in PARQUET format
  
copy into @~/parquet/ FROM (
    select  
        tripduration,
        starttime ,
        stoptime ,
        start_station_id ,
        start_station_name ,
        start_station_latitude ,
        start_station_longitude ,
        end_station_id ,
        end_station_name ,
        end_station_latitude ,
        end_station_longitude ,
        bikeid ,
        membership_type ,
        usertype ,
        birth_year ,
        gender 
    
    from trips 
    where starttime >= '2018-05-01' and starttime <= '2018-05-30'
)
FILE_FORMAT = (TYPE = PARQUET)
HEADER = true;

/*CREATE copy of TRIPS table for loading from parquet */
create or replace table trips_parquet like trips;
--OR complete table script
create or replace table trips_parquet
(tripduration integer,
  starttime timestamp,
  stoptime timestamp,
  start_station_id integer,
  start_station_name string,
  start_station_latitude float,
  start_station_longitude float,
  end_station_id integer,
  end_station_name string,
  end_station_latitude float,
  end_station_longitude float,
  bikeid integer,
  membership_type string,
  usertype string,
  birth_year integer,
  gender integer);

  --Import data from PARQUET file into the table
  COPY into trips_parquet (tripduration, starttime, stoptime, start_station_id, start_station_name,
                        start_station_latitude, start_station_longitude, end_station_id,
                        end_station_name, end_station_latitude, end_station_longitude,
                        bikeid, membership_type,usertype,birth_year, gender) 
from 
(
 select 
    $1:TRIPDURATION::integer,
    $1:STARTTIME::timestamp,
    $1:STOPTIME::timestamp,
    $1:START_STATION_ID::integer,
    $1:START_STATION_NAME::string,
    $1:START_STATION_LATITUDE::float,
    $1:START_STATION_LONGITUDE::float,
    $1:END_STATION_ID::integer,
    $1:END_STATION_NAME::string,
    $1:END_STATION_LATITUDE::float,
    $1:END_STATION_LONGITUDE::float,
    $1:BIKEID::integer,
    $1:MEMBERSHIP_TYPE::string,
    $1:USERTYPE::string,
    $1:BIRTH_YEAR::integer,
    $1:GENDER::integer
 from @~/parquet/
)
 file_format = (type = PARQUET)
;

select * from trips_parquet;

--USING INFER_SCHEMA to find out the parquet file schema
/*
Now Let's suppose we are about to import a new parquet file where we do not know its structure.
Use INFER_SCHEMA function to find out how the file schema looks like.

Use the parquet files exported in previous steps. They are available in your user stage. 

In order to use INFER_SCHEMA function, You need to have a file format with defined parquet type. 
This one will be then used in INFER_SCHEMA calling

Please create following file format
*/

create file format my_parquet_format
type = parquet;
create file format my_parquet_format
type = parquet;

select *
  from table(
    infer_schema(
      location=>'@~/parquet/'
      , file_format=>'my_parquet_format'
      )
    );

---------------------------------------------------------------------------------
