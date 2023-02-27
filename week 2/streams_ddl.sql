--create table like trips and call it trips_monthly
create table trips_monthly like trips;

--create a stream on top of this new table to track changes
create stream str_trips_monthly on table trips_monthly;

--insert some data into table with stream
insert into trips_monthly select * from trips
where date_trunc('month', starttime) = '2018-04-01T00:00:00Z';

--create a table for holding the result of the aggregation
create table fact_rides
(
month timestamp_ntz,
number_of_rides number,
total_duration number,
avg_duration number
);
