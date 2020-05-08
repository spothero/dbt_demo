    SELECT DISTINCT * 
    FROM {{ source('sh_public','reviews_reviewresponse') }}
    WHERE reservation_id IN (
        SELECT
            reservation_id
        FROM {{ source('sh_public','reviews_reviewresponse') }}
        GROUP BY 1
        HAVING min(star_rating) = max(star_rating)
    )