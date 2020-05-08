SELECT
        parking_spot.parking_spot_id,
        COUNT(DISTINCT
                CASE WHEN (transient_inventory_status.is_unavailable = 'f') = 'no' and transient_inventory_status.unavailable_reason = 'sold out'
                    THEN cast(CONVERT_TIMEZONE('UTC', 'America/Chicago', transient_inventory_status.starts) as date)  ELSE NULL END)
                AS "p8w_distinct_days_sold_out",
        COUNT(DISTINCT
                CASE WHEN (transient_inventory_status.is_unavailable = 'f') = 'no' and transient_inventory_status.unavailable_reason = 'sold out'
                   THEN TO_CHAR(DATE_TRUNC('week', CONVERT_TIMEZONE('UTC', 'America/Chicago', transient_inventory_status.starts )), 'YYYY-MM-DD')  ELSE NULL END)
                AS "p8w_distinct_weeks_sold_out",
        1.0 * (COUNT(
                CASE WHEN (transient_inventory_status.is_unavailable = 'f') = 'no' and transient_inventory_status.unavailable_reason = 'sold out'
                THEN 1 ELSE NULL END)) / 2
                AS "p8w_total_hours_sold_out",
      1.0 * (COUNT(CASE WHEN (transient_inventory_status.is_unavailable = 'f') = 'no' and transient_inventory_status.unavailable_reason = 'sold out'
       THEN 1 ELSE NULL END)) / (COUNT(DISTINCT transient_inventory_status.id )) AS "p8w_percent_time_sold_out",
      case when((1.0 * (COUNT(CASE WHEN (transient_inventory_status.is_unavailable = 'f') = 'no' and transient_inventory_status.unavailable_reason = 'sold out'
       THEN 1 ELSE NULL END)) / 2)>0) THEN TRUE ELSE FALSE END as p8w_did_sell_out
      ,CASE WHEN MAX(transient_inventory_status.transient_total) = MIN(transient_inventory_status.transient_total) THEN FALSE Else TRUE END as p8w_had_inventory_rules_or_changes
      FROM {{ source('sh_public','spothero_inventoryavailabilitystatus') }}  AS transient_inventory_status
      LEFT JOIN {{ source('sh_public','parking_spot') }}  AS parking_spot ON transient_inventory_status.spot_id = parking_spot.parking_spot_id
      WHERE ((((transient_inventory_status.starts ) >= ((CONVERT_TIMEZONE('America/Chicago', 'UTC', DATEADD(week,-8, DATE_TRUNC('week', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE()))) )))) AND (transient_inventory_status.starts ) < ((CONVERT_TIMEZONE('America/Chicago', 'UTC', DATEADD(week,8, DATEADD(week,-8, DATE_TRUNC('week', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE()))) ) ))))))) AND (NOT COALESCE(transient_inventory_status.is_cnp , FALSE))
      group by 1