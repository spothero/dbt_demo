{{ config(tags=["presto"]) }}

SELECT
  rental_presto.rental_id,
  CASE WHEN DATE_FORMAT( true_search_presto.requested_time_starts  ,'%W') NOT IN ('Saturday', 'Sunday') THEN
      (
      CASE WHEN (HOUR( true_search_presto.requested_time_starts ) BETWEEN 5 AND 10) AND date_diff('hour', (true_search_presto.requested_time_starts ), ( true_search_presto.requested_time_ends )) <= 12 THEN 'Commuter'
        WHEN date_diff('hour', ( true_search_presto.requested_time_starts ), ( true_search_presto.requested_time_ends )) BETWEEN 0 AND 3 THEN '3 Hrs'
        WHEN date_diff('hour', (true_search_presto.requested_time_starts ), ( true_search_presto.requested_time_ends )) BETWEEN 3 AND 6 THEN
          (CASE WHEN (HOUR( true_search_presto.requested_time_starts  ) >= 16 OR HOUR( true_search_presto.requested_time_starts  ) <= 4) THEN 'Night (6 Hrs)' ELSE '6 Hrs' END)
        WHEN date_diff('hour', ( true_search_presto.requested_time_starts ), ( true_search_presto.requested_time_ends )) BETWEEN 6 AND 12 THEN
          (CASE WHEN (HOUR( true_search_presto.requested_time_starts  ) >= 16 OR HOUR( true_search_presto.requested_time_starts  ) <= 4) THEN 'Overnight (12 Hrs)' ELSE '12 Hrs' END)
        WHEN date_diff('hour', ( true_search_presto.requested_time_starts ), ( true_search_presto.requested_time_ends )) > 12 THEN '24 Hrs'
        ELSE NULL END
      )
  WHEN DATE_FORMAT( true_search_presto.requested_time_starts  ,'%W') IN ('Saturday', 'Sunday') THEN
      (
      CASE WHEN date_diff('hour', ( true_search_presto.requested_time_starts ), ( true_search_presto.requested_time_ends )) BETWEEN 0 AND 12 THEN 'Weekend (12 Hrs)'
      ELSE 'Weekend (24 hrs)' END
       )
  ELSE NULL
  END AS rate_segment
FROM  {{ ref('pg_true_search') }} AS true_search_presto
INNER JOIN {{ ref('pg_rentals_past_eight_weeks') }} AS rental_presto ON true_search_presto.search_uuid = rental_presto.search_id AND true_search_presto.facility_id = rental_presto.parking_spot_id
WHERE (true_search_presto.search_type = 'TRANSIENT')
GROUP BY 1,2