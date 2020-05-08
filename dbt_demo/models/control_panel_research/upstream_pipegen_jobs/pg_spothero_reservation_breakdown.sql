    SELECT parking_spot.parking_spot_id
    from {{ source('sh_public','parking_spot') }}
    INNER JOIN {{ source('sh_public','monthly_rental_rule') }}
    ON monthly_rental_rule.parking_spot_id = parking_spot.parking_spot_id
    AND monthly_rental_rule.rule_status = 'enabled'
    GROUP BY 1