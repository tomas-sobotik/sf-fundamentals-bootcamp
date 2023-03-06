/*
20 Performance optimization
*/


--filter the data based on date should lead to effective prunning
select * from trips where starttime between to_timestamp('01-01-2017', 'DD-MM-YYYY') and
to_timestamp('30-01-2017', 'DD-MM-YYYY');


--alter the warehouse to have more power
alter warehouse compute_wh set warehouse_size = xlarge;

--bad query with exploding joins, let it run for 2 mins
select * from trips a
join trips b on a.start_station_id = b.start_station_id;

--scaling warehouse back
alter warehouse compute_wh set warehouse_size = xsmall;

--complex query
select * from trips where (start_station_name, end_station_name) in (
select start_station_name, end_station_name from trips where start_station_name in
(
    select start_station_name from trips
        group by start_station_name
        order by count(*) desc
    limit 100

)
group by start_station_name, end_station_name
order by count(*) desc)
;
