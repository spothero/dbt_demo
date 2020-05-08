  SELECT
    parking_spot.parking_spot_id
    ,AVG(1.00*reviews_reviewresponse.star_rating) as average_star_rating
    ,AVG(CASE WHEN (((reviews_reviewresponse.created ) >= ((CONVERT_TIMEZONE('America/Chicago', 'UTC', DATEADD(week,-4, DATE_TRUNC('week', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE()))) )))) AND (reviews_reviewresponse.created ) < ((CONVERT_TIMEZONE('America/Chicago', 'UTC', DATEADD(week,4, DATEADD(week,-4, DATE_TRUNC('week', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE()))) ) )))))) THEN 1.00*reviews_reviewresponse.star_rating ELSE NULL END) as average_star_rating_p4w
  FROM {{ ref('pg_reviews_reviewresponse') }} as reviews_reviewresponse
  INNER JOIN {{ ref('pg_rentals') }}   AS rental ON reviews_reviewresponse.reservation_id  = rental.rental_id
  INNER JOIN {{ source('sh_public','parking_spot') }}  AS parking_spot ON rental.parking_spot_id = parking_spot.parking_spot_id
  GROUP BY 1