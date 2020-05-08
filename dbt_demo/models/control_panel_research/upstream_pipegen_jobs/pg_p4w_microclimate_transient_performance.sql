  WITH microclimate_data AS (
      SELECT
         parking_spot.parking_spot_id  AS parking_spot_id,
         comp_parking_spots.parking_spot_id  AS comp_parking_spot_id,
         COALESCE(COALESCE(CAST( ( SUM(DISTINCT (CAST(FLOOR(COALESCE(CASE WHEN rental_facts.is_good_rental  THEN rental.price  ELSE NULL END,0)*(CAST(1000000 AS DOUBLE PRECISION)*1.0)) AS DECIMAL(38,0))) + CAST(STRTOL(LEFT(MD5(CAST(CASE WHEN rental_facts.is_good_rental  THEN rental.rental_id  ELSE NULL END AS VARCHAR)),15),16) AS DECIMAL(38,0))* 1.0e8 + CAST(STRTOL(RIGHT(MD5(CAST(CASE WHEN rental_facts.is_good_rental  THEN rental.rental_id  ELSE NULL END AS VARCHAR)),15),16) AS DECIMAL(38,0)) ) - SUM(DISTINCT CAST(STRTOL(LEFT(MD5(CAST(CASE WHEN rental_facts.is_good_rental  THEN rental.rental_id  ELSE NULL END AS VARCHAR)),15),16) AS DECIMAL(38,0))* 1.0e8 + CAST(STRTOL(RIGHT(MD5(CAST(CASE WHEN rental_facts.is_good_rental  THEN rental.rental_id  ELSE NULL END AS VARCHAR)),15),16) AS DECIMAL(38,0))) )  AS DOUBLE PRECISION) / CAST((CAST(1000000 AS DOUBLE PRECISION)) AS DOUBLE PRECISION), 0), 0) AS "total_p4w_transient_gross_revenue"
      FROM {{ ref('pg_spot_microclimate') }} AS spot_microclimate
      LEFT JOIN {{ source('sh_public','parking_spot') }}  AS parking_spot ON spot_microclimate.origin_parking_spot_id = parking_spot.parking_spot_id
      LEFT JOIN {{ source('sh_public','parking_spot') }}  AS comp_parking_spots ON spot_microclimate.comp_parking_spot_id = comp_parking_spots.parking_spot_id
      LEFT JOIN {{ ref('pg_rentals') }}  AS rental ON comp_parking_spots.parking_spot_id = rental.parking_spot_id
      LEFT JOIN {{ ref('pg_rental_facts') }}  AS rental_facts ON rental.rental_id = rental_facts.rental_id

      WHERE (spot_microclimate.distance_miles  <= 0.25) AND (comp_parking_spots.status = 'on_sales_allowed')
        AND ((((rental.created ) >= ((CONVERT_TIMEZONE('America/Chicago', 'UTC', DATEADD(week,-4, DATE_TRUNC('week', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE()))) )))) AND (rental.created ) < ((CONVERT_TIMEZONE('America/Chicago', 'UTC', DATEADD(week,4, DATEADD(week,-4, DATE_TRUNC('week', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE()))) ) )))))))
          AND (rental_facts.transient_or_monthly = 'Transient')
      GROUP BY 1,2
      )

      SELECT
        parking_spot_id
        ,AVG(total_p4w_transient_gross_revenue) total_microclimate_average
        ,AVG(CASE WHEN QUARTILE = 1 THEN total_p4w_transient_gross_revenue ELSE NULL END) upper_microclimate_average
        ,AVG(CASE WHEN QUARTILE = 2 THEN total_p4w_transient_gross_revenue ELSE NULL END) upper_mid_microclimate_average
        ,AVG(CASE WHEN QUARTILE = 3 THEN total_p4w_transient_gross_revenue ELSE NULL END) lower_mid_microclimate_average
        ,AVG(CASE WHEN QUARTILE = 4 THEN total_p4w_transient_gross_revenue ELSE NULL END) lower_microclimate_average
      FROM
      (
        SELECT
        parking_spot_id,
        comp_parking_spot_id,
        total_p4w_transient_gross_revenue,
        NTILE(4) OVER(PARTITION BY parking_spot_id ORDER BY total_p4w_transient_gross_revenue DESC) AS Quartile
        FROM microclimate_data
        GROUP BY 1,2,3
      )
      GROUP BY 1