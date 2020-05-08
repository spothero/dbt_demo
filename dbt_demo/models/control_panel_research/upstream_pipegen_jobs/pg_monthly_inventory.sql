    WITH a as (
      SELECT
       monthly_rental_rule.rule_id,
       monthly_rental_rule.max_quantity,
       CASE WHEN monthly_rental_rule.recurrable  THEN 'Recurrable' ELSE 'Nonrecurrable' END AS rate_type,
       COUNT(DISTINCT CASE WHEN rental_facts.is_good_rental AND ((((convert_timezone ('UTC', spothero_city.timezone, rental.starts) ) >= ((DATE_TRUNC('month', DATE_TRUNC('day',convert_timezone ('UTC', spothero_city.timezone, GETDATE()))))) AND (convert_timezone ('UTC', spothero_city.timezone, rental.starts) ) < ((DATEADD(month,1, DATE_TRUNC('month', DATE_TRUNC('day',convert_timezone ('UTC', spothero_city.timezone, GETDATE()))) )))))) THEN rental.rental_id  ELSE NULL END) AS mtd_rental_count,
       MAX(CASE WHEN rental_facts.is_good_rental THEN convert_timezone ('UTC', spothero_city.timezone, rental.created)  ELSE NULL END) AS latest_booking
      FROM {{ source('sh_public','monthly_rental_rule') }}
      LEFT join {{ ref('pg_rentals') }}   AS rental ON monthly_rental_rule.rule_id = rental.rule_id
      LEFT JOIN {{ ref('pg_rental_facts') }}  AS rental_facts ON rental.rental_id = rental_facts.rental_id
      LEFT JOIN {{ source('sh_public','parking_spot') }}  AS parking_spot ON monthly_rental_rule.parking_spot_id = parking_spot.parking_spot_id
      LEFT join {{ source('sh_public','spothero_city') }} ON parking_spot.spothero_city_id = spothero_city.spothero_city_id
      WHERE (monthly_rental_rule.rule_status = 'enabled') AND (parking_spot.status = 'on_sales_allowed')
      GROUP BY 1,2,3
      )
      SELECT *,
       CASE WHEN rate_type = 'Nonrecurrable' THEN max_quantity + mtd_rental_count else max_quantity end as total_monthly_inventory,
       CASE WHEN rate_type = 'Nonrecurrable' THEN max_quantity
            WHEN max_quantity-mtd_rental_count>=0 THEN max_quantity-mtd_rental_count
            ELSE 0
       end as remaining_inventory
      FROM a