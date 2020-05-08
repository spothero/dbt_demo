   SELECT
        rental.parking_spot_id  AS parking_spot_id,
        1000.00 * (COUNT(DISTINCT CASE WHEN (hero_tag_c.sipp_c = 'TRUE') THEN sfdc_case.rental_id_c  ELSE NULL END)) / nullif((COUNT(DISTINCT rental.rental_id )),0) AS p48w_sipp
      FROM {{ source('sfdc','case') }}  AS sfdc_case
      FULL OUTER JOIN {{ ref('pg_rentals') }} AS rental ON rental.rental_id = sfdc_case.rental_id_c
      LEFT JOIN {{ source('sfdc','hero_tag_c') }}  AS hero_tag_c ON sfdc_case.hero_tag_c = hero_tag_c.id
      WHERE
      (((rental.starts ) >= ((DATEADD(week,-8, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) AND (rental.starts ) < ((DATEADD(week,4, DATEADD(week,-8, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ) )))))
      GROUP BY 1