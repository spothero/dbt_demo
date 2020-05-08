with p5w as (
      SELECT rental.parking_spot_id
            ,COALESCE(SUM(CASE WHEN rental_facts.is_good_rental THEN rental.price ELSE NULL END), 0) AS total_gross_revenue
            ,COUNT(CASE WHEN rental_facts.is_good_rental THEN 1 ELSE NULL END) AS rental_count
      from {{ ref('pg_rentals') }} as rental
      LEFT JOIN {{ ref('pg_rental_facts') }} as rental_facts ON rental.rental_id = rental_facts.rental_id
      LEFT join {{ source('sh_public','parking_spot') }} ON rental.parking_spot_id = parking_spot.parking_spot_id
      LEFT join {{ source('sh_public','spothero_city') }} ON parking_spot.spothero_city_id = spothero_city.spothero_city_id
      WHERE ((((convert_timezone ('UTC', spothero_city.timezone, rental.created)) >= ((DATEADD(week,-5, DATE_TRUNC('week', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE()))) )))
           AND (convert_timezone ('UTC', spothero_city.timezone, rental.created)) < ((DATEADD(week,5, DATEADD(week,-5, DATE_TRUNC('week', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE()))) ) ))))))
           AND rental_facts.is_good_rental
      GROUP BY 1
    )
, pw as (
      SELECT rental.parking_spot_id
            ,COALESCE(SUM(CASE WHEN rental_facts.is_good_rental THEN rental.price ELSE NULL END), 0) AS total_gross_revenue
            ,COUNT(CASE WHEN rental_facts.is_good_rental THEN 1 ELSE NULL END) AS rental_count
      from {{ ref('pg_rentals') }} as rental
      LEFT JOIN {{ ref('pg_rental_facts') }} as rental_facts ON rental.rental_id = rental_facts.rental_id
      LEFT join {{ source('sh_public','parking_spot') }} ON rental.parking_spot_id = parking_spot.parking_spot_id
      LEFT join {{ source('sh_public','spothero_city') }} ON parking_spot.spothero_city_id = spothero_city.spothero_city_id
      WHERE ((((convert_timezone ('UTC', spothero_city.timezone, rental.created)) >= ((DATEADD(week,-1, DATE_TRUNC('week', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE()))) )))
           AND (convert_timezone ('UTC', spothero_city.timezone, rental.created)) < ((DATEADD(week,1, DATEADD(week,-1, DATE_TRUNC('week', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE()))) ) ))))))
           AND rental_facts.is_good_rental
      GROUP BY 1
    )
SELECT
p5w.parking_spot_id
,COALESCE(p5w.rental_count,0) as p5w_rental_count
,COALESCE(pw.rental_count,0) as pw_rental_count
,1.00*((COALESCE(p5w.rental_count,0) - COALESCE(pw.rental_count,0)))/4 as previous4w_avg_rental_count
,((1.00*(COALESCE(pw.rental_count,0)))/nullif((1.00*(1.00*((COALESCE(p5w.rental_count,0) - COALESCE(pw.rental_count,0)))/4)),0))-1 as percent_change_pw_previous4w_rental_count
FROM p5w
LEFT JOIN pw on pw.parking_spot_id = p5w.parking_spot_id