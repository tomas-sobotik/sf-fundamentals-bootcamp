*
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
select * from snowflake.account_usage.query_history where query_id in ('YOUR_QUERY_ID');
