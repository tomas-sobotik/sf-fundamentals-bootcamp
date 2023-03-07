/*
14 Data Sharing
*/

use role sysadmin;

--creating a special role for acessing the the DB objects 
create database role citibike.share_provider1;

--grant usage on db, schema and table we want to share
grant usage on database citibike to database role share_provider1;
grant usage on schema citibike.public to database role share_provider1;
grant select on table citibike.public.trips_monthly to database role share_provider1;
grant select on table citibike.public.json_sample to database role share_provider1;

--create empty share
use role accountadmin;
create share s_monthly_trips;


--grant usage on database to include it into the share
grant usage on database citibike to share s_monthly_trips;

--grant role to share
grant database role share_provider1 to share s_monthly_trips;

--check the shares
show shares;

--adding consumer account to the share
alter share s_monthly_trips add accounts = xxxx;

/*
EXERCISE 2 - granting objects directly to share
*/

--creating empty share
create share s_json_data;

--grant usage on db and schema
grant usage on database citibike to share s_json_data;
grant usage on schema citibike.public to share s_json_data;
grant select on table citibike.public.json_sample to share s_json_data;
grant select on table citibike.public.json_trips_per_station to share s_json_data;

---------------------------------------------------------------------------------

/*
15 Data Marketplace
*/

select * from starbucks_locations.public.core_poi where lower(city) like 'new york'; 
--1095 Lexington Ave, --40.773446	-73.959778

select distinct start_station_name, start_station_latitude, start_station_longitude  from trips where lower(start_station_name) like '%lexington ave%'; 

select * from accuweather_sample.forecast.top_city_daily_metric where city_name like 'New York';

select * from accuweather_historical.historical.top_city_daily_imperial where city_name like 'New York'; 

---------------------------------------------------------------------------------
/*
16 Snowsight dashboard
*/

--used credits
select
    sum(credits_used)
from
    account_usage.metering_history
where
    start_time = :daterange;

--total executed jobs
select
    count(*) as number_of_jobs
from
    account_usage.query_history
where
    --start_time >= date_trunc(month, current_date);
    start_time = :daterange;


--billable storage
select
    avg(storage_bytes + stage_bytes + failsafe_bytes) / power(1024, 3) as billable_mb
from
    account_usage.storage_usage
where
    USAGE_DATE = current_date() -1;


--credits usage overtime
select
    start_time::date as usage_date,
    warehouse_name,
    sum(credits_used) as total_credits_used
from
    account_usage.warehouse_metering_history
where
    start_time = :daterange
group by
    1,
    2
order by
    2,
    1;

--execution time by query type
select
    query_type,
    warehouse_size,
    avg(execution_time) / 1000 as average_execution_time
from
    account_usage.query_history
where
    start_time = :daterange
group by
    1,
    2
order by
    3 desc;

  
  --total execution time by repeated queries
  select
    query_id,
    query_text,
    (execution_time / 60000) as exec_time
from
    account_usage.query_history
where
    execution_status = 'SUCCESS'
order by
    execution_time desc
limit
    25;


--compute and cloud services credits usage by warehouse
select
    warehouse_name,
    sum(credits_used_cloud_services) credits_used_cloud_services,
    sum(credits_used_compute) credits_used_compute,
    sum(credits_used) credits_used
from
    account_usage.warehouse_metering_history
where
    true
    and start_time = :daterange
group by
    1
order by
    2 desc
limit
    10;

  ---------------------------------------------------------------------------------

/*
17 Programmability - create UDF
*/
--give me age of the bike rider
create or replace function get_age(birth_year number) 
returns number
as
$$
select date_part('year', current_timestamp) - birth_year
$$
;


select birth_year, get_age(birth_year) from trips limit 200;

--is it weekend ride?
create or replace function weekend_ride_check(ride_date timestamp)
returns boolean
as
$$
    select decode( dayname(ride_date), 'Sat', True, 'Sun', True, False)
$$;

select weekend_ride_check(starttime), * from trips limit 200;

show user functions;

---------------------------------------------------------------------------------

/*
19 Serverless features
*/


--clean the fact table first
truncate table fact_rides;
truncate table log_fact_rides;

use role sysadmin;

--alter the root task to remove the warehouse
alter task t_rides_agg unset warehouse;

--check the task definition, now the warehouse attribute should be empty it means that task is serverless
show tasks;

--we need to grant execute managed task to sysadmin role
use role accountadmin;
grant EXECUTE MANAGED TASK on account to role SYSADMIN;
use role sysadmin;

--recreate the task to be a serverless
create or replace task t_rides_log
comment = 'Logging the last loaded month'
after T_RIDES_AGG
AS
insert into log_fact_rides select max(month), current_timestamp from 
fact_rides;

--check the task definition
show tasks;

--resume tasks
alter task T_RIDES_LOG resume;
alter task t_rides_agg resume;

--insert data into trips_monthly
insert into trips_monthly select * from trips 
where date_trunc('month', starttime) = '2018-06-01T00:00:00Z';

--check the fact table
select * from fact_rides;

--check the log table
select * from log_fact_rides;

--check the task history
select *
  from table(information_schema.task_history(
  TASK_NAME => 'T_RIDES_AGG'
  ))
  order by scheduled_time desc;
  
  
--suspend tasks
alter task T_RIDES_LOG suspend;
alter task t_rides_agg suspend;

--check what warehouse snowflake use for the task runs
use role accountadmin;
select * from snowflake.account_usage.query_history where query_id in ('01a977e4-0102-2a72-0000-cd550017f1fe'); -- you need to provide your own query_id