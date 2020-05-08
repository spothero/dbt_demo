  SELECT
        parking_spot.parking_spot_id,
        COUNT(DISTINCT
                CASE WHEN (transient_inventory_status.is_unavailable = 'f') = 'no' and transient_inventory_status.unavailable_reason = 'sold out'
                    THEN cast(convert_timezone('UTC',spothero_city.timezone,transient_inventory_status.starts) as date)  ELSE NULL END)
                AS "p4w_distinct_days_sold_out",
        COUNT(DISTINCT
                CASE WHEN (transient_inventory_status.is_unavailable = 'f') = 'no' and transient_inventory_status.unavailable_reason = 'sold out' AND ((TRIM(TO_CHAR(convert_timezone('UTC',spothero_city.timezone,transient_inventory_status.starts), 'Day')) IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday')))
                    THEN cast(convert_timezone('UTC',spothero_city.timezone,transient_inventory_status.starts) as date) ELSE NULL END)
                AS "p4w_distinct_days_sold_out_weekday",
        COUNT(DISTINCT
                CASE WHEN (transient_inventory_status.is_unavailable = 'f') = 'no' and transient_inventory_status.unavailable_reason = 'sold out' AND ((TRIM(TO_CHAR(convert_timezone('UTC',spothero_city.timezone,transient_inventory_status.starts), 'Day')) IN ('Saturday', 'Sunday')))
                    THEN cast(convert_timezone('UTC',spothero_city.timezone,transient_inventory_status.starts) as date) ELSE NULL END)
                AS "p4w_distinct_days_sold_out_weekend",
        COUNT(DISTINCT
                CASE WHEN (transient_inventory_status.is_unavailable = 'f') = 'no' and transient_inventory_status.unavailable_reason = 'sold out'
                   THEN TO_CHAR(DATE_TRUNC('week', convert_timezone('UTC',spothero_city.timezone,transient_inventory_status.starts )), 'YYYY-MM-DD')  ELSE NULL END)
                AS "p4w_distinct_weeks_sold_out",
        1.0 * (COUNT(
                CASE WHEN (transient_inventory_status.is_unavailable = 'f') = 'no' and transient_inventory_status.unavailable_reason = 'sold out'
                THEN 1 ELSE NULL END)) / 2
                AS "p4w_total_hours_sold_out",
      1.0 * (COUNT(CASE WHEN (transient_inventory_status.is_unavailable = 'f') = 'no' and transient_inventory_status.unavailable_reason = 'sold out'
       THEN 1 ELSE NULL END)) / (COUNT(DISTINCT transient_inventory_status.id )) AS "p4w_percent_time_sold_out",
      case when((1.0 * (COUNT(CASE WHEN (transient_inventory_status.is_unavailable = 'f') = 'no' and transient_inventory_status.unavailable_reason = 'sold out'
       THEN 1 ELSE NULL END)) / 2)>0) THEN TRUE ELSE FALSE END as p4w_did_sell_out
      ,MAX((transient_inventory_status.transient_total - transient_inventory_status.transient_available)) AS p4w_max_inventory_sold
      ,MAX(CASE WHEN ((transient_inventory_status.is_unavailable = 'f') = 'no' and transient_inventory_status.unavailable_reason = 'sold out') THEN (transient_inventory_status.transient_total - transient_inventory_status.transient_available) ELSE NULL END) AS p4w_max_inventory_sold_when_sold_out
      ,MAX(transient_inventory_status.transient_total) AS p4w_max_inventory_cap
      ,CASE WHEN MAX(transient_inventory_status.transient_total) = MIN(transient_inventory_status.transient_total) THEN FALSE Else TRUE END as p4w_had_inventory_rules_or_changes
      FROM {{ source('sh_public','spothero_inventoryavailabilitystatus') }}  AS transient_inventory_status
      LEFT JOIN {{ source('sh_public','parking_spot') }}  AS parking_spot ON transient_inventory_status.spot_id = parking_spot.parking_spot_id
      LEFT JOIN {{ source('sh_public','spothero_city') }} ON spothero_city.spothero_city_id = parking_spot.spothero_city_id
      WHERE ((((convert_timezone('UTC',spothero_city.timezone,transient_inventory_status.starts) ) >= ((DATEADD(week,-4, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) )))
          AND (convert_timezone('UTC',spothero_city.timezone,transient_inventory_status.starts) ) < ((DATEADD(week,4, DATEADD(week,-4, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ) )))))) AND (NOT COALESCE(transient_inventory_status.is_cnp , FALSE))
      GROUP BY 1