  SELECT rental.parking_spot_id
      ,COALESCE(SUM(CASE WHEN rental_facts.is_good_rental THEN rental.price ELSE NULL END), 0) AS p48w_total_gross_revenue
      ,COALESCE(SUM(CASE WHEN rental_facts.is_good_rental AND (NOT rental_facts.is_event_rate_rental OR rental_facts.is_event_rate_rental IS NULL) THEN rental.price ELSE NULL END), 0) AS p48w_total_gross_revenue_no_event
      ,COUNT(CASE WHEN rental_facts.is_good_rental THEN 1 ELSE NULL END) AS p48w_rental_count
      ,COUNT(CASE WHEN rental_facts.rental_sequence_number = 1 THEN 1 ELSE NULL END) AS p48w_new_users
      ,COUNT(CASE WHEN rental_facts.facility_rental_sequence_number = 1 THEN 1 ELSE NULL END) AS p48w_new_facility_users
      ,COUNT(DISTINCT CASE WHEN rental_facts.facility_rental_sequence_number > 1 THEN rental_facts.renter_id ELSE NULL END) AS p48w_repeat_facility_users
  from {{ ref('pg_rentals') }} as rental
  LEFT JOIN {{ ref('pg_rental_facts') }} as rental_facts ON rental.rental_id = rental_facts.rental_id
  LEFT join {{ source('sh_public','parking_spot') }} ON rental.parking_spot_id = parking_spot.parking_spot_id
  LEFT join {{ source('sh_public','spothero_city') }} ON parking_spot.spothero_city_id = spothero_city.spothero_city_id
  LEFT JOIN {{ source('sfdc','opportunity') }} AS sfdc_opportunity ON sfdc_opportunity.spot_id_c = parking_spot.parking_spot_id
  WHERE (((convert_timezone ('UTC', spothero_city.timezone, rental.created)) >= (DATEADD(week,-8, DATE_TRUNC('week', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE()))) ))))
     AND (((convert_timezone ('UTC', spothero_city.timezone, rental.created)) < (DATEADD(week,-4, DATE_TRUNC('week', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE()))) ))))
     AND rental_facts.transient_or_monthly = 'Transient'
  GROUP BY 1