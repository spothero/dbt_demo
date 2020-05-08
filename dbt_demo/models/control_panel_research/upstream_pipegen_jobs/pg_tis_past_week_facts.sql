  SELECT
    parking_spot.parking_spot_id,
    AVG(transient_inventory_status.transient_total) as avg_total_inventory,
    AVG(CASE WHEN DATE_PART(hour, convert_timezone ('UTC', spothero_city.timezone, transient_inventory_status.starts))::integer >=7 AND DATE_PART(hour, convert_timezone ('UTC', spothero_city.timezone, transient_inventory_status.starts))::integer < 19 AND MOD(EXTRACT(DOW FROM convert_timezone ('UTC', spothero_city.timezone, transient_inventory_status.starts))::integer - 1 + 7, 7) NOT IN (5,6) THEN transient_inventory_status.transient_total ELSE NULL END) as avg_weekday_daytime_total_inventory,
    AVG(CASE WHEN MOD(EXTRACT(DOW FROM convert_timezone ('UTC', spothero_city.timezone, transient_inventory_status.starts))::integer - 1 + 7, 7) IN (5,6) THEN transient_inventory_status.transient_total ELSE NULL END) as avg_weekend_total_inventory,
    COUNT(DISTINCT CASE WHEN (transient_inventory_status.is_unavailable = 'f') = 'no' and transient_inventory_status.unavailable_reason = 'sold out' THEN cast(CONVERT_TIMEZONE('UTC', 'America/Chicago', transient_inventory_status.starts) as date)  ELSE NULL END) AS distinct_days_sold_out,
    1.0 * (COUNT(CASE WHEN (transient_inventory_status.is_unavailable = 'f') = 'no' and transient_inventory_status.unavailable_reason = 'sold out' THEN 1 ELSE NULL END)) / 2  AS total_hours_sold_out,
    1.0 * (COUNT(CASE WHEN (transient_inventory_status.is_unavailable = 'f') = 'no' and transient_inventory_status.unavailable_reason = 'sold out' THEN 1 ELSE NULL END)) / (COUNT(DISTINCT transient_inventory_status.id )) AS percent_time_sold_out,
    ((1.0 * (COUNT(CASE WHEN (transient_inventory_status.is_unavailable = 'f') = 'no' and transient_inventory_status.unavailable_reason = 'sold out' THEN 1 ELSE NULL END)) / 2)>0) as did_sell_out,
    MAX(transient_inventory_status.transient_total) = MIN(transient_inventory_status.transient_total) AS had_inventory_rules_or_changes
  FROM {{ source('sh_public','spothero_inventoryavailabilitystatus') }}  AS transient_inventory_status
  LEFT JOIN {{ source('sh_public','parking_spot') }}  AS parking_spot ON transient_inventory_status.spot_id = parking_spot.parking_spot_id
  LEFT JOIN {{ source('sh_public','spothero_city') }}  AS spothero_city ON parking_spot.spothero_city_id = spothero_city.spothero_city_id
  WHERE (((convert_timezone ('UTC', spothero_city.timezone, transient_inventory_status.starts) ) >= ((DATEADD(day,-7, DATE_TRUNC('day',GETDATE()) ))) AND (convert_timezone ('UTC', spothero_city.timezone, transient_inventory_status.starts) ) < ((DATEADD(day,7, DATEADD(day,-7, DATE_TRUNC('day',GETDATE()) ) ))))) AND (NOT COALESCE(transient_inventory_status.is_cnp , FALSE))
  GROUP BY 1