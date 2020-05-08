  SELECT
    cast(requested_time_starts as TIMESTAMP) as requested_time_starts,
    search_source,
    experiment_or_control,
    requested_time_duration_tier,
    facility_id,
    AVG(price_offered) as average_price_offered,
    SUM(rental_price) AS total_gross_revenue_for_facility,
    COUNT(search_uuid) AS search_count,
    COUNT(
      CASE
        WHEN rental_id IS NOT NULL THEN search_uuid
        ELSE NULL
      END
    ) AS search_converted_count
  FROM (
    SELECT DISTINCT
      searches.search_uuid,
      searches.facility_id,
      searches.source as search_source,
      1.0 * searches.price / 100 as price_offered,
      CASE
        WHEN searches.rule_group_id = cast(facility_rate_recommendation.rate_rule_group_id AS varchar) THEN 'Experiment'
        ELSE 'Control'
      END AS experiment_or_control,
      CASE
        WHEN date_diff('hour', requested_time_starts, requested_time_ends) < 4 then '<= 3 hours'
        WHEN date_diff('hour', requested_time_starts, requested_time_ends) < 7 then '4-6 hours'
        WHEN date_diff('hour', requested_time_starts, requested_time_ends) < 13 then '7-12 hours'
        WHEN date_diff('hour', requested_time_starts, requested_time_ends) < 25 then '13-24 hours'
        ELSE '>= 25 hours'
      END AS requested_time_duration_tier,
      COALESCE(rental.price, 0) as rental_price,
      rental.rental_id,
      DATE_FORMAT(
        searches.requested_time_starts AT TIME ZONE 'UTC',
        '%Y-%m-%d %H:00:00'
      ) AS requested_time_starts
    FROM hive.search_tracking.searches_spark AS searches
    JOIN redshift.daphne_public.facility_rate_recommendation
    	ON searches.facility_id = facility_rate_recommendation.facility_id
  	LEFT JOIN redshift.sh_public.rental AS rental
        ON UPPER(searches.search_uuid) = UPPER(rental.search_id)
          AND searches.facility_id = rental.parking_spot_id
          AND rental.reservation_status = 'valid'
          AND rental.payment_status = 'success'
    WHERE
      searches.occurred_utc >= CAST(
        CONCAT(
          FORMAT_DATETIME(TIMESTAMP '2020-01-01', 'yyyy-MM-dd HH:mm:ss.SSS'), ' America/Chicago'
        ) AS TIMESTAMP
      )
      AND searches.processed >= CAST(
      CONCAT(
          FORMAT_DATETIME(TIMESTAMP '2020-01-01', 'yyyy-MM-dd HH:mm:ss.SSS'), ' America/Chicago'
        ) AS TIMESTAMP
      )
      AND searches.search_type = 'TRANSIENT'
  )
  GROUP BY 1,2,3,4,5