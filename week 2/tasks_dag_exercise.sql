--create a custom log table holding the last loaded month and system timestamp when it has been done
create or replace table log_fact_rides
(
max_loaded_month timestamp_ntz,
inserted_date timestamp_ntz
);

--we need to suspend the root task first
alter task t_rides_agg suspend;

--query for your task
insert into log_fact_rides select max(month), current_timestamp from
fact_rides;

--check the tasks
show tasks;

--insert new data into trips_monthly to trigger whole pipeline
insert into trips_monthly select * from trips
where date_trunc('month', starttime) = '2018-06-01T00:00:00Z';
