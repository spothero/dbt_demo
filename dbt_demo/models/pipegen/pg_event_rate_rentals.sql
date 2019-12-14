   SELECT
    rental.rental_id
   FROM {{ ref('pg_rentals') }} AS rental 
   WHERE EXISTS(
     SELECT rental.rental_id
     FROM {{ source('sh_public','spothero_reservation_breakdown') }} spothero_reservation_breakdown
     WHERE spothero_reservation_breakdown.rule_type = 'event' 
     AND spothero_reservation_breakdown.rental_id IS NOT NULL 
     and rental.rental_id = spothero_reservation_breakdown.rental_id 
   ) 