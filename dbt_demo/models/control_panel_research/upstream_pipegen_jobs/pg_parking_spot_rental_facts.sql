    SELECT rental.parking_spot_id
          ,min(created) AS first_sale
          ,max(created) AS last_sale
    from {{ ref('pg_rentals') }} as rental
    LEFT JOIN {{ ref('pg_rental_facts') }} rental_facts ON rental_facts.rental_id = rental.rental_id
    WHERE rental_facts.is_good_rental
    GROUP BY 1