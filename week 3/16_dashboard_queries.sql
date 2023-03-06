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

--billable storage
    select
        avg(storage_bytes + stage_bytes + failsafe_bytes) / power(1024, 3) as billable_mb
    from
        account_usage.storage_usage
    where
        USAGE_DATE = current_date() -1;

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
