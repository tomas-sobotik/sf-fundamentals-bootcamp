SELECT
  *
FROM
  TRIPS
WHERE
  date_trunc('day',starttime) between '2018-06-09' and ‘2018-06-10'
AND start_station_id = 239;
