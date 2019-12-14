SELECT
  rental.renter_id, first_purchase
  ,COUNT(CASE WHEN (rental.reservation_status = 'valid' OR rental.reservation_status = 'recurrence') AND (rental.created::date - first_purchase::date)<=7 THEN 1 ELSE NULL END) AS "First_7_Day_Rental_Count"
  ,COUNT(CASE WHEN (rental.reservation_status = 'valid' OR rental.reservation_status = 'recurrence') AND (rental.created::date - first_purchase::date)<=14 THEN 1 ELSE NULL END) AS "First_14_Day_Rental_Count"
  ,COUNT(CASE WHEN (rental.reservation_status = 'valid' OR rental.reservation_status = 'recurrence') AND (rental.created::date - first_purchase::date)<=30 THEN 1 ELSE NULL END) AS "First_30_Day_Rental_Count"
  ,COUNT(CASE WHEN (rental.reservation_status = 'valid' OR rental.reservation_status = 'recurrence') AND (rental.created::date - first_purchase::date)<=60 THEN 1 ELSE NULL END) AS "First_60_Day_Rental_Count"
  ,COUNT(CASE WHEN (rental.reservation_status = 'valid' OR rental.reservation_status = 'recurrence') AND (rental.created::date - first_purchase::date)<=90 THEN 1 ELSE NULL END) AS "First_90_Day_Rental_Count"
  ,COUNT(CASE WHEN (rental.reservation_status = 'valid' OR rental.reservation_status = 'recurrence') AND (rental.created::date - first_purchase::date)<=183 THEN 1 ELSE NULL END) AS "First_183_Day_Rental_Count"
  ,COUNT(CASE WHEN (rental.reservation_status = 'valid' OR rental.reservation_status = 'recurrence') AND (rental.created::date - first_purchase::date)<=365 THEN 1 ELSE NULL END) AS "First_365_Day_Rental_Count"
  ,COUNT(DISTINCT CASE WHEN (rental.reservation_status = 'valid' OR rental.reservation_status = 'recurrence') AND (rental.created::date - first_purchase::date)<=365 THEN parking_spot_id ELSE NULL END) AS "First_365_Day_Parking_Spot_Count"
  ,CAST(COALESCE(SUM(CASE WHEN (rental.reservation_status = 'valid' OR rental.reservation_status = 'recurrence') AND (rental.created::date - first_purchase::date)<=30 THEN rental.price ELSE NULL END), 0) as decimal(10,2)) AS "First_30_Day_GMV"
  ,CAST(COALESCE(SUM(CASE WHEN (rental.reservation_status = 'valid' OR rental.reservation_status = 'recurrence') AND (rental.created::date - first_purchase::date)<=60 THEN rental.price ELSE NULL END), 0) as decimal(10,2)) AS "First_60_Day_GMV"
  ,CAST(COALESCE(SUM(CASE WHEN (rental.reservation_status = 'valid' OR rental.reservation_status = 'recurrence') AND (rental.created::date - first_purchase::date)<=90 THEN rental.price ELSE NULL END), 0) as decimal(10,2)) AS "First_90_Day_GMV"
  ,CAST(COALESCE(SUM(CASE WHEN (rental.reservation_status = 'valid' OR rental.reservation_status = 'recurrence') AND (rental.created::date - first_purchase::date)<=365 THEN rental.price ELSE NULL END), 0) as decimal(10,2)) AS "First_365_Day_GMV"
FROM {{ ref('pg_rentals') }} as rental
JOIN {{ source('sh_public','spothero_user') }} on spothero_user.id = rental.renter_id
LEFT JOIN (
  SELECT rental.renter_id as renters_id, min(rental.created) as first_purchase 
  FROM {{ ref('pg_rentals') }} as rental 
  WHERE reservation_status='valid' or reservation_status='recurrence'
  GROUP BY 1
) first_purchase_table on first_purchase_table.renters_id = rental.renter_id
GROUP BY 1,2