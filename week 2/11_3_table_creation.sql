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
      --'trips', t
      'trips', array_agg(t) over (partition by start_station_name, date_trunc('day',starttime) )
    ) json
from individual_trips;
