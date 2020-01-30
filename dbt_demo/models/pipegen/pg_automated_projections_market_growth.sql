{{ config(tags=["redshift"]) }}


WITH revenue AS (
    SELECT
      reporting_market.id  AS reporting_market,
      TO_CHAR(DATE_TRUNC('month', rental.created ), 'YYYY-MM') AS created_month,
      COALESCE(SUM(CASE WHEN rental_facts.is_good_rental  THEN rental.price  ELSE NULL END), 0) AS total_gross_revenue,
      COUNT(DISTINCT rental.parking_spot_id ) AS parking_spot_count
    FROM {{ source('sh_public','parking_spot') }}  AS parking_spot
    LEFT JOIN {{ source('sfdc','opportunity') }}  AS sfdc_opportunity ON parking_spot.parking_spot_id = sfdc_opportunity.spot_id_c and not sfdc_opportunity.is_deleted
    LEFT JOIN {{ source('sfdc','reporting_neighborhood_c') }}  AS reporting_neighborhood ON sfdc_opportunity.reporting_neighborhood_geography_c = reporting_neighborhood.id
    LEFT JOIN {{ source('sfdc','reporting_city_c') }}  AS reporting_city ON reporting_neighborhood.reporting_city_c = reporting_city.id
    LEFT JOIN {{ source('sfdc','market_c') }}  AS reporting_market ON reporting_city.reporting_market_c = reporting_market.id
    LEFT JOIN {{ ref('pg_rentals') }}  AS rental ON rental.parking_spot_id = parking_spot.parking_spot_id
    LEFT JOIN {{ ref('pg_rental_facts') }}  AS rental_facts ON rental.rental_id = rental_facts.rental_id

    WHERE ((rental.created ) >= ((DATEADD(month,-24, DATE_TRUNC('month', DATE_TRUNC('day',GETDATE())) ))) AND (rental.created ) < ((DATEADD(month,24, DATEADD(month,-24, DATE_TRUNC('month', DATE_TRUNC('day',GETDATE()))))))) AND (NOT COALESCE(rental.partner_id = 45 , FALSE))
    GROUP BY 1,2
)

,CAGR_calculation AS (
    SELECT
      reporting_market
      ,created_month
      ,total_gross_revenue
      ,parking_spot_count
      ,(LAG(total_gross_revenue,12) OVER (PARTITION BY reporting_market ORDER BY created_month)) / NULLIF((LAG(parking_spot_count,12) OVER (PARTITION BY reporting_market ORDER BY created_month)),0) AS year_prior
      ,((total_gross_revenue / NULLIF(parking_spot_count,0))/NULLIF((year_prior),0))^(1.0/11.0) - 1.0 AS CAGR_value
FROM revenue
)

,ranking_table AS (
    SELECT
      reporting_market
      ,CAGR_value
      ,NTILE(4) OVER (PARTITION BY reporting_market ORDER BY CAGR_value DESC) RANKING
    FROM CAGR_calculation
    WHERE CAGR_value IS NOT NULL
    GROUP BY 1,2
)

,avg_values AS (
    SELECT
      reporting_market AS reporting_market_id
      ,CASE WHEN AVG(CAGR_value) < 0.0 THEN 0.01 WHEN AVG(CAGR_value) > 0.1 THEN 0.1 ELSE AVG(CAGR_value) END market_upper_average
    FROM ranking_table
    WHERE RANKING = 1
    GROUP BY 1
)

SELECT
    sfdc_opportunity.id opportunity_id
    ,COALESCE(market_upper_average,0.04) market_upper_average
FROM {{ source('sfdc','opportunity') }}  AS sfdc_opportunity
LEFT JOIN {{ source('sfdc','reporting_neighborhood_c') }}  AS reporting_neighborhood ON sfdc_opportunity.reporting_neighborhood_geography_c = reporting_neighborhood.id
LEFT JOIN {{ source('sfdc','reporting_city_c') }}  AS reporting_city ON reporting_neighborhood.reporting_city_c = reporting_city.id
LEFT JOIN {{ source('sfdc','market_c') }}  AS reporting_market ON reporting_city.reporting_market_c = reporting_market.id
LEFT JOIN avg_values ON avg_values.reporting_market_id = reporting_market.id
WHERE NOT sfdc_opportunity.is_deleted