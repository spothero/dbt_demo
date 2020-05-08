  SELECT
    parking_spot.parking_spot_id
  FROM sh_public.parking_spot
  INNER JOIN
    (SELECT
      event_rental_rule.parking_spot_id AS parking_spot_id
    FROM {{ source('sh_public','event_rental_rule') }}
    LEFT JOIN {{ source('sh_public','event') }} ON event_rental_rule.event_id = event.event_id
    WHERE event_rental_rule.rule_status = 'enabled' AND (DATEDIFF(day,event.starts,GETDATE()) < 0 OR event_rental_rule.destination_id IS NOT NULL)
    UNION
    SELECT
      facility_id AS parking_spot_id
    FROM {{ source('sh_public','tiered_event_rate_key') }}
    INNER JOIN {{ source('sh_public','tiered_event_rate') }} ON tiered_event_rate_key.id = tiered_event_rate.tiered_event_rate_key_id
      AND NOT tiered_event_rate.deleted
    ) event_rates ON event_rates.parking_spot_id = parking_spot.parking_spot_id
  GROUP BY 1