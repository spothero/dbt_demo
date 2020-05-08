  SELECT parking_spot.parking_spot_id
       from {{ source('sh_public','parking_spot') }}
       INNER JOIN {{ source('sh_public','ratings_ratingrule') }}
         ON ratings_ratingrule.parking_spot_id = parking_spot.parking_spot_id
         AND ratings_ratingrule.rule_status = '0'
        GROUP BY 1