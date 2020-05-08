  SELECT
      distinct parking_spot.parking_spot_id  AS parking_spot_id
    FROM {{ source('sh_public','monthly_rental_rule') }}  AS monthly_rates
    LEFT JOIN {{ ref('pg_monthly_inventory') }} AS monthly_inventory ON monthly_inventory.rule_id = monthly_rates.rule_id
    LEFT JOIN {{ source('sh_public','parking_spot') }}  AS parking_spot ON monthly_rates.parking_spot_id = parking_spot.parking_spot_id
    WHERE
      monthly_inventory.total_monthly_inventory > 0 AND (monthly_rates.rule_status = 'enabled')