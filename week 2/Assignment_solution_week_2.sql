/*
08 Working with cache

*/


alter warehouse compute_wh set auto_suspend = 360;

alter warehouse compute_wh suspend;

select * from trips where start_station_id = 3171;

select end_station_id, end_station_name, count(*) from trips where start_station_id = 3171
group by end_station_id, end_station_name
order by count(*) desc;


alter warehouse compute_wh set auto_suspend = 60;

---------------------------------------------------------------------------------

/*
09 RBAC model

*/

use role securityadmin;

create or replace role ANALYST;
grant usage on database citibike to role analyst;
grant usage on schema citibike.public to role analyst;
grant select on table citibike.public.trips to role analyst;
grant usage on warehouse compute_wh to role analyst;

create or replace role DEVELOPER;
grant role ANALYST to role DEVELOPER;
grant all on database CITIBIKE to role developer;
grant all on schema CITIBIKE.PUBLIC to role developer;
grant role developer to role SYSADMIN;


use role analyst;
create table tmp as select * from trips where 1 = 0;

use role developer;

create table tmp as select * from trips where 1 = 0;
drop table tmp;

---------------------------------------------------------------------------------

/*
10 Storage related features

*/


update trips set start_station_name = 'Central Park S & 6 Ave TMP' where start_station_id = 2006; --308759

select distinct start_station_name from trips where start_station_id = 2006;

--using TIME TRAVEL to find initial value  
select distinct start_station_name from trips at (timestamp => dateadd('minutes', -10, current_timestamp) )
where start_station_id = 2006;

--another TIME TRAVEL syntax
select distinct start_station_name from trips at (offset => -60*10 )
where start_station_id = 2006;

--CLONE creation with TIME TRAVEL syntax
create table trips_cloned clone trips at (offset => -60*10 );

select distinct start_station_name from trips_cloned where start_station_id = 2006;

--option 1: fixing using timetravel
update trips set start_station_name = 
(
    select distinct start_station_name from trips at (offset => -60*10 ) where start_station_id = 2006
)
where start_station_id = 2006;

--option 2: fixing using cloned table
drop table trips;

alter table trips_cloned rename to trips;

---------------------------------------------------------------------------------

/*
11 Semi structured data
Exercise #1
*/

--Create a new table for holding the JSON data
create table json_sample (value variant);

--running the insert statements
insert into json_sample
select parse_json('
{"id":1,"first_name":"Madelena","last_name":"Bastiman","email":"mbastiman0@washington.edu","gender":"Female","ip_address":"47.136.171.159","language":"Bislama","city":"Lagunas","street":"Nevada","street_number":"2","phone":"+51 224 307 3778"}'); 
insert into json_sample
select parse_json('
{"id":2,"first_name":"Jasmine","last_name":"Hayth","email":"jhayth1@soup.io","gender":"Female","ip_address":"44.117.102.69","language":"Papiamento","city":"Sikeshu","street":"Dovetail","street_number":"7922","phone":"+86 710 521 7096"}');
insert into json_sample
select parse_json('
{"id":3,"first_name":"Doria","last_name":"Brownjohn","email":"dbrownjohn2@unblog.fr","gender":"Female","ip_address":"242.221.58.251","language":"Greek","city":"Trollhättan","street":"Dayton","street_number":"8348","phone":"+46 207 829 2153"}');
insert into json_sample
select parse_json('
{"id":4,"first_name":"Gaylor","last_name":"Enderson","email":"genderson3@istockphoto.com","gender":"Bigender","ip_address":"169.138.17.143","language":"Swahili","city":"Velizh","street":"Sugar","street_number":"1","phone":"+7 659 246 7831"}');
insert into json_sample
select parse_json('
{"id":5,"first_name":"Gaile","last_name":"Elcombe","email":"gelcombe4@japanpost.jp","gender":"Male","ip_address":"13.24.168.205","language":"Haitian Creole","city":"L’vovskiy","street":"Surrey","street_number":"3751","phone":"+7 641 563 0389"}');

--checking the table content
select * from json_sample;


--Because the JSON data has quite simple structure without nested elements and arrays, we can easily create a view on top of the JSON file with colon notation: 
create or replace view json_sample_view as
select
    value:id::integer as id,
    value:email::string as email,
    value:first_name::string as first_name,
    value:ip_address::string as ip_address,
    value:language::string as language,
    value:city::string as city,
    value:street::string as street,
    value:street_number::integer as street_number,
    value:phone::string as phone_number
   
from
    json_sample;
    

    
select * from json_sample_view;

----------------------------------------------------------------------------

/*
Exercise #2

Create a JSON structure from relational data - simple example
*/

select
    object_construct(
     'StartStationName', start_station_name,
     'day',  date_trunc('day',starttime),
     'userType', array_agg(distinct usertype)  over (partition by date_trunc('day',starttime), start_station_id),
     'tripDetails', object_construct
        (
            'endStationName', end_station_name,
            'duration', tripduration
        )   
   )

from trips 
where 
    date_trunc('day',starttime) between '2018-06-09' and '2018-06-10'
and start_station_id = 239
limit 100;  

--BONUS: create a JSON structure which will aggregate all the trips in station Willoughby St & Fleet St at June 9, 2018.

--solution with CTE
with individual_trips as (
    select object_construct(
        'duration', tripduration,
        'endStation', end_station_name,
        'userbirthYear', birth_year,
        'membershipType', membership_type,
        'userType', usertype
        ) t, 
        start_station_name,
        starttime
        
                        

from trips 
where 
date_trunc('day',starttime) between '2018-06-09' and '2018-06-10'
and start_station_id = 239 )

select
    object_construct(
     'stationName', start_station_name,
      'day',  date_trunc('day',starttime),
      'trips', array_agg(t) over (partition by start_station_name, date_trunc('day',starttime) ) 
    ) json
from individual_trips limit 100;

----------------------------------------------------------------------------

/*
Exercise #3

Flattening the JSON data
*/

--First -> create table with json data
create or replace table json_trips_per_station as  
with individual_trips as (
    select object_construct(
        'startStation', start_station_name,
        'duration', tripduration,
        'endStation', end_station_name,
        'membershipType', membership_type,
        'userDetails', object_construct(
            'userType', usertype,
            'userbirthYear', birth_year
        )
        
        ) t, 
        start_station_name,
        starttime
        
                        

from trips 
where 
date_trunc('day',starttime) between '2018-06-01' and '2018-06-07'
 )

select
    object_construct(
     'stationName', start_station_name,
      'day',  date_trunc('day',starttime),
      'trips', array_agg(t) over (partition by start_station_name, date_trunc('day',starttime) ) 
    ) json
from individual_trips;

--flattening the data
select 
t.json:day::timestamp start_time,
t.json:stationName::varchar start_station,
f.value:duration::number duration,
f.value:endStation::varchar end_station,
f.value:membershipType::varchar membership_Type,
f.value:userDetails:userType::varchar user_Type,
f.value:userDetails:userbirthYear::varchar user_Birth_Year
from 
json_trips_per_station t,
lateral flatten (input => t.json:trips) f
limit 10;

---------------------------------------------------------------------------------

/*
12 Data Governance
Dynamic Data Masking
*/
use role sysadmin;
select * from trips limit 100;

--policy creation

create or replace masking policy mask_pii as (val number) returns number ->
  case
    when current_role() in ('ANALYST', 'SYSADMIN', 'SECURITYADMIN', 'ACCOUNTADMIN') then val
    else 0000
end;

--applying masking policy to the birth_year and gender columns
alter table trips modify column birth_year set masking policy mask_pii;
alter table trips modify column gender set masking policy mask_pii;

--unsetting the policy
alter table trips modify column birth_year unset masking policy; 
alter table trips modify column gender unset masking policy; 

--changing role to DEVELOPER and checking the table again
use role developer;
select * from citibike.public.trips limit 100;

---------------------------------------------------------------------------------
/* EXERCISE 2
same example but policy is automatically applied based on TAG value
*/
--unset the policy first
alter table trips modify column birth_year unset masking policy; 
alter table trips modify column gender unset masking policy; 

--switch to accountadmin for tag creation
use role accountadmin;
create tag security_level;

--assign masking policy to the tag
alter tag security_level set masking policy mask_pii;

--assign tag to birth_year and gender columns
alter table trips modify column birth_year set tag security_level = 'pii';
alter table trips modify column gender set tag security_level = 'pii';


--try to query a table
select * from citibike.public.trips limit 100;

--change role to developer and query the table again
use role developer;
select * from citibike.public.trips limit 100;

---------------------------------------------------------------------------------

/*
13 Streams and Tasks
*/

--STREAMS
select * from trips limit 100;

select date_trunc('month',starttime), count(*) from trips group by 
date_trunc('month',starttime)
order by 1 desc;

--create table like trips and call it trips_monthly
create table trips_monthly like trips;

--create a stream on top of this new table to track changes 
create stream str_trips_monthly on table trips_monthly;

--check the stream;
show streams;

--check if stream has data
select SYSTEM$STREAM_HAS_DATA('str_trips_monthly');

--insert some data into table with stream
insert into trips_monthly select * from trips 
where date_trunc('month', starttime) = '2018-04-01T00:00:00Z';

--check if stream has data again
select SYSTEM$STREAM_HAS_DATA('str_trips_monthly');

--try to query a stream
select * from str_trips_monthly limit 1000;

--check what kind of actions stream contains
select distinct metadata$action, metadata$isupdate from str_trips_monthly;


--create a table for holding the result of the aggregation
create table fact_rides 
(
month timestamp_ntz,
number_of_rides number,
total_duration number,
avg_duration number
);

--consume the stream and insert aggregated data into the new table
insert into fact_rides 
select date_trunc('month', starttime), count(*), sum(tripduration), round(avg(tripduration),2) 
from str_trips_monthly
group by date_trunc('month', starttime);

--check the data
select * from fact_rides;

--check if the stream still has data
select SYSTEM$STREAM_HAS_DATA('str_trips_monthly');

---------------------------------------------------------------------------------
/* EXERCISE 2
TASK
*/

--first we need to grant executing task to sysadmin role
use role accountadmin;
grant execute task on account to role sysadmin;
use role sysadmin;

--create task
create or replace task t_rides_agg
warehouse = COMPUTE_WH
schedule = '1 minute'
comment = 'aggregating rides on monhtly basis'
when SYSTEM$STREAM_HAS_DATA('str_trips_monthly')
AS
insert into fact_rides 
select date_trunc('month', starttime), count(*), sum(tripduration), round(avg(tripduration),2) 
from str_trips_monthly
group by date_trunc('month', starttime);

--check the task definition
show tasks;

--resume task
alter task t_rides_agg resume;

--insert data into trips_monthly
insert into trips_monthly select * from trips 
where date_trunc('month', starttime) = '2018-05-01T00:00:00Z';

--check the fact table
select * from fact_rides;

--check the task history
select *
  from table(information_schema.task_history())
  order by scheduled_time desc;

---------------------------------------------------------------------------------

/* EXERCISE 3
Chaining the tasks to create a DAG
*/

--create a custom log table holding the last loaded month and system timestamp when it has been done
create or replace table log_fact_rides
(
max_loaded_month timestamp_ntz,
inserted_date timestamp_ntz
);

--we need to suspend the root task first
alter task t_rides_agg suspend;

--create a new task
create or replace task t_rides_log
warehouse = COMPUTE_WH
comment = 'Logging the last loaded month'
after T_RIDES_AGG
AS
insert into log_fact_rides select max(month), current_timestamp from 
fact_rides;

--check the tasks
show tasks;

--resume both tasks
alter task t_rides_log resume;
alter task t_rides_agg resume;

--insert new data into trips_monthly to trigger whole pipeline
insert into trips_monthly select * from trips 
where date_trunc('month', starttime) = '2018-06-01T00:00:00Z';

--check fact table
select * from fact_rides;

--check the log table
select * from log_fact_rides;