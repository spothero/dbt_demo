  SELECT
          parking_spot.parking_spot_id  AS parking_spot_id,
          COUNT(DISTINCT transient_rates.id ) AS enabled_rates
        FROM {{ source('sh_public','ratings_ratingrule') }}  AS transient_rates
        LEFT JOIN {{ source('sh_public','parking_spot') }}  AS parking_spot ON parking_spot.parking_spot_id = transient_rates.parking_spot_id
        LEFT JOIN {{ source('sfdc','opportunity') }}  AS sfdc_opportunity ON parking_spot.parking_spot_id = sfdc_opportunity.spot_id_c and not sfdc_opportunity.is_deleted
        LEFT JOIN {{ source('sh_public','ratings_ratingrulegroup') }}  AS ratings_ratingrulegroup ON transient_rates.rating_rule_group_id = ratings_ratingrulegroup.id
        LEFT JOIN {{ ref('pg_spothero_reservation_breakdown') }}  AS spothero_reservation_breakdown ON spothero_reservation_breakdown.rule_id = transient_rates.id AND spothero_reservation_breakdown.rule_type != 'event'
        LEFT JOIN {{ ref('pg_rentals') }}  AS rental ON rental.rental_id = spothero_reservation_breakdown.rental_id

        WHERE ((transient_rates.starts  < (DATEADD(minute,0, DATE_TRUNC('minute', CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE())) )))) AND ((transient_rates.ends  >= (DATEADD(minute,0, DATE_TRUNC('minute', CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE())) )) OR transient_rates.ends  IS NULL)) AND (transient_rates.rule_status = 0) AND (ratings_ratingrulegroup.status = 'enabled')
        GROUP BY 1