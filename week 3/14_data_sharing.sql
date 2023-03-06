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
