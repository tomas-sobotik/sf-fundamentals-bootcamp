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
