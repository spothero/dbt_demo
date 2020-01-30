{{ config(tags=["presto"]) }}

SELECT
  rental_presto.rental_id,
  COALESCE(rate_segment_by_requested_presto.rate_segment,
    (CASE WHEN DATE_FORMAT(rental_local_tz.starts_local_timezone ,'%W') NOT IN ('Saturday', 'Sunday') THEN
          (
          CASE WHEN (HOUR(rental_local_tz.starts_local_timezone) BETWEEN 5 AND 10) AND date_diff('hour', (rental_local_tz.starts_local_timezone), (rental_local_tz.ends_local_timezone)) <= 12 THEN 'Commuter'
            WHEN date_diff('hour', (rental_local_tz.starts_local_timezone), (rental_local_tz.ends_local_timezone)) BETWEEN 0 AND 3 THEN '3 Hrs'
            WHEN date_diff('hour', (rental_local_tz.starts_local_timezone), (rental_local_tz.ends_local_timezone)) BETWEEN 3 AND 6 THEN
              (CASE WHEN (HOUR(rental_local_tz.starts_local_timezone ) >= 16 OR HOUR(rental_local_tz.starts_local_timezone ) <= 4) THEN 'Night (6 Hrs)' ELSE '6 Hrs' END)
            WHEN date_diff('hour', (rental_local_tz.starts_local_timezone), (rental_local_tz.ends_local_timezone)) BETWEEN 6 AND 12 THEN
              (CASE WHEN (HOUR(rental_local_tz.starts_local_timezone ) >= 16 OR HOUR(rental_local_tz.starts_local_timezone ) <= 4) THEN 'Overnight (12 Hrs)' ELSE '12 Hrs' END)
            WHEN date_diff('hour', (rental_local_tz.starts_local_timezone), (rental_local_tz.ends_local_timezone)) > 12 THEN '24 Hrs'
            ELSE NULL END
          )
      WHEN DATE_FORMAT(rental_local_tz.starts_local_timezone ,'%W') IN ('Saturday', 'Sunday') THEN
          (
          CASE WHEN date_diff('hour', (rental_local_tz.starts_local_timezone), (rental_local_tz.ends_local_timezone)) BETWEEN 0 AND 12 THEN 'Weekend (12 Hrs)'
          ELSE 'Weekend (24 hrs)' END
           )
      ELSE NULL
      END)) AS mod_rate_segment
FROM /*redshift.pipegen.pg_rentals_past_eight_weeks */ {{ ref('pg_rentals_past_eight_weeks') }} AS rental_presto
LEFT JOIN 
/*	hive_emr.pipegen.pg_rental_local_timezone_presto */ {{ ref('pg_rental_local_timezone_presto') }}  AS rental_local_tz ON rental_presto.rental_id = rental_local_tz.rental_id
LEFT JOIN 
/*	hive_emr.pipegen.pg_rate_segment_by_requested_presto */ {{ ref('pg_rate_segment_by_requested_presto') }} AS rate_segment_by_requested_presto ON rental_presto.rental_id = rate_segment_by_requested_presto.rental_id
WHERE (((rental_local_tz.created_local_timezone ) >= ((DATE_ADD('week', -4, DATE_TRUNC('DAY', DATE_ADD('day', (0 - MOD((DAY_OF_WEEK(CAST(NOW() AS DATE)) % 7) - 1 + 7, 7)), CAST(NOW() AS DATE)))))) AND (rental_local_tz.created_local_timezone ) < ((DATE_ADD('week', 4, DATE_ADD('week', -4, DATE_TRUNC('DAY', DATE_ADD('day', (0 - MOD((DAY_OF_WEEK(CAST(NOW() AS DATE)) % 7) - 1 + 7, 7)), CAST(NOW() AS DATE)))))))))
GROUP BY 1,2