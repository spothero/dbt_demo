  SELECT parking_spot_id,
      CASE
        WHEN parking_spot_id IN ( SELECT DISTINCT parking_spot_id FROM {{ source('sh_public','spothero_hours') }} WHERE (NOT deleted) AND start_time = '00:00:00' AND end_time = '00:00:00' AND start_dow = 0 AND end_dow = 0 ) THEN '24/2'

        WHEN parking_spot_id IN ( SELECT parking_spot_id FROM(
          SELECT  parking_spot_id,
            min(start_time) as min_start,
            max(start_time) as max_start,
            min(end_time) as min_ends,
            max(end_time) as max_ends,
            sum(start_dow) as start_dow_sum,
            sum(end_dow) as end_dow_sum
    FROM {{ source('sh_public','spothero_hours') }}
    WHERE (NOT deleted)
    GROUP BY 1)
    WHERE min_start = '00:00:00' AND max_start = '00:00:00' AND min_ends = '00:00:00' AND max_ends = '00:00:00' and start_dow_sum = '11' AND end_dow_sum = '6')
    THEN '24/2'

        WHEN parking_spot_id IN ( SELECT DISTINCT parking_spot_id FROM {{ source('sh_public','spothero_hours') }} WHERE (NOT deleted) AND (start_dow = '5' AND end_dow = '6' AND start_time = '00:00:00' AND end_time = '00:00:00'))
        AND parking_spot_id IN ( SELECT DISTINCT parking_spot_id FROM {{ source('sh_public','spothero_hours') }} WHERE (NOT deleted) AND (start_dow = '6' AND end_dow = '0' AND start_time = '00:00:00' AND end_time = '00:00:00'))
        THEN '24/2'

        WHEN parking_spot_id in (SELECT distinct parking_spot_id FROM {{ source('sh_public','spothero_hours') }} WHERE start_dow in (5,6)) THEN 'Weekends - Limited Hours'

      ELSE 'Weekends - Closed'
      END as weekend_hoo,

      CASE
        WHEN parking_spot_id IN ( SELECT DISTINCT parking_spot_id FROM {{ source('sh_public','spothero_hours') }} WHERE (NOT deleted) AND start_time = '00:00:00' AND end_time = '00:00:00' AND start_dow = 0 AND end_dow = 0 ) THEN '24/5'
        WHEN parking_spot_id IN ( SELECT parking_spot_id FROM(
          SELECT  parking_spot_id,
            min(start_time) as min_start,
            max(start_time) as max_start,
            min(end_time) as min_ends,
            max(end_time) as max_ends,
            sum(start_dow) as start_dow_sum,
            sum(end_dow) as end_dow_sum
    FROM {{ source('sh_public','spothero_hours') }}
    WHERE (NOT deleted)
    GROUP BY 1)
    WHERE min_start = '00:00:00' AND max_start = '00:00:00' AND min_ends = '00:00:00' AND max_ends = '00:00:00' and start_dow_sum = '10' AND end_dow_sum = '15')
    THEN '24/5'

        WHEN parking_spot_id IN ( SELECT DISTINCT parking_spot_id FROM {{ source('sh_public','spothero_hours') }} WHERE (NOT deleted) AND (start_dow = '0' AND end_dow = '1' AND start_time = '00:00:00' AND end_time = '00:00:00'))
        AND parking_spot_id IN ( SELECT DISTINCT parking_spot_id FROM {{ source('sh_public','spothero_hours') }} WHERE (NOT deleted) AND (start_dow = '1' AND end_dow = '2' AND start_time = '00:00:00' AND end_time = '00:00:00'))
        AND parking_spot_id IN ( SELECT DISTINCT parking_spot_id FROM {{ source('sh_public','spothero_hours') }} WHERE (NOT deleted) AND (start_dow = '2' AND end_dow = '3' AND start_time = '00:00:00' AND end_time = '00:00:00'))
        AND parking_spot_id IN ( SELECT DISTINCT parking_spot_id FROM {{ source('sh_public','spothero_hours') }} WHERE (NOT deleted) AND (start_dow = '3' AND end_dow = '4' AND start_time = '00:00:00' AND end_time = '00:00:00'))
        AND parking_spot_id IN ( SELECT DISTINCT parking_spot_id FROM {{ source('sh_public','spothero_hours') }} WHERE (NOT deleted) AND (start_dow = '4' AND end_dow = '5' AND start_time = '00:00:00' AND end_time = '00:00:00'))
        THEN '24/5'

        WHEN parking_spot_id in (SELECT distinct parking_spot_id FROM {{ source('sh_public','spothero_hours') }} WHERE start_dow in (0,1,2,3,4)) THEN 'Weekdays - Limited Hours'
      ELSE 'Weekdays - Closed'
      END as weekday_hoo


    FROM {{ source('sh_public','spothero_hours') }}
    WHERE (NOT deleted)
    GROUP BY 1,2,3