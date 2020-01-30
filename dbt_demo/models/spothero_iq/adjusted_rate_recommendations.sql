{{ config(tags=["presto","spothero_iq"]) }}

    WITH commuter_price_full AS (
      SELECT
      *,
      SUM(CASE WHEN full_rate_segment = 'Commuter' THEN price ELSE NULL END) OVER (PARTITION BY origin_opportunity_id) AS "Commuter_Price"
      FROM {{ ref('pg_competitive_rate_segment_performance_presto') }} --hive_emr.pipegen.pg_competitive_rate_segment_performance_presto
    )

    ,full_rate_suite AS (
      SELECT
        *,
        SUM(CASE WHEN full_rate_segment = 'Weekend (24 hrs)' THEN
          (CASE WHEN (COALESCE(price,0) >= 2*COALESCE("Weekend_12_Hrs_Price",0) OR price IS NULL) THEN "Weekend_12_Hrs_Price" * 1.2 ELSE price END)
            ELSE NULL END) OVER (PARTITION BY origin_opportunity_id) AS "Weekend_24_Hrs_Price"
      FROM (
        SELECT
          *,
          SUM(CASE WHEN full_rate_segment = 'Weekend (12 Hrs)' THEN
            (CASE WHEN (COALESCE(price,0) >= 1.5*COALESCE("Price_12_Hrs",0) OR price IS NULL) THEN "Price_12_Hrs" * 0.8 ELSE price END)
              ELSE NULL END) OVER (PARTITION BY origin_opportunity_id) AS "Weekend_12_Hrs_Price"
        FROM (
          SELECT
            *,
            SUM(CASE WHEN full_rate_segment = 'Night (6 Hrs)' THEN
              (CASE WHEN (COALESCE(price,0) >= 0.9*COALESCE("Overnight_12_Hrs_Price",0) OR price IS NULL) THEN "Overnight_12_Hrs_Price" * 0.8 ELSE price END)
                ELSE NULL END) OVER (PARTITION BY origin_opportunity_id) AS "Night_6_Hrs_Price"
          FROM (
            SELECT
              *,
              SUM(CASE WHEN full_rate_segment = 'Overnight (12 Hrs)' THEN
                (CASE WHEN (COALESCE(price,0) >= 0.8*COALESCE("Price_12_Hrs",0) OR price IS NULL) THEN "Price_12_Hrs" * 0.75 ELSE price END)
                  ELSE NULL END) OVER (PARTITION BY origin_opportunity_id) AS "Overnight_12_Hrs_Price"
            FROM (
              SELECT
                *,
                SUM(CASE WHEN full_rate_segment = '3 Hrs' THEN
                  (CASE WHEN (COALESCE(price,0) >= 0.9*COALESCE("Price_6_Hrs",0) OR COALESCE(price,0)*2 <= COALESCE("Price_6_Hrs",0) OR price IS NULL) THEN "Price_6_Hrs" * 0.8 ELSE price END)
                    ELSE NULL END) OVER (PARTITION BY origin_opportunity_id) AS "Price_3_Hrs"
              FROM (
                SELECT
                  *,
                  SUM(CASE WHEN full_rate_segment = '6 Hrs' THEN
                    (CASE WHEN (COALESCE(price,0) >= 0.9*COALESCE("Price_12_Hrs",0) OR COALESCE(price,0)*2 <= COALESCE("Price_12_Hrs",0) OR price IS NULL) THEN "Price_12_Hrs" * 0.8 ELSE price END)
                      ELSE NULL END) OVER (PARTITION BY origin_opportunity_id) AS "Price_6_Hrs"
                FROM (
                  SELECT
                    *,
                    SUM(CASE WHEN full_rate_segment = '24 Hrs' THEN
                      (CASE WHEN (COALESCE(price,0) <= COALESCE("Price_12_Hrs",0) OR COALESCE(price,0) >= 2*COALESCE("Price_12_Hrs",0) OR price IS NULL) THEN "Price_12_Hrs" * 1.2 ELSE price END)
                        ELSE NULL END) OVER (PARTITION BY origin_opportunity_id) AS "Price_24_Hrs"
                  FROM (
                    SELECT
                      *,
                      SUM(CASE WHEN full_rate_segment = '12 Hrs' THEN
                        (CASE WHEN (COALESCE(price,0) < 1.05*COALESCE("Commuter_Price",0) OR price IS NULL) THEN "Commuter_Price" * 1.05 ELSE price END)
                          ELSE NULL END) OVER (PARTITION BY origin_opportunity_id) AS "Price_12_Hrs"
                    FROM commuter_price_full
                  )
                )
              )
            )
          )
        )
      )
    )

    SELECT
      origin_opportunity_id
      ,rate_segment
      ,price AS unadjusted_price
      ,CASE WHEN full_rate_segment = '3 Hrs' THEN "Price_3_Hrs"
        WHEN full_rate_segment = '6 Hrs' THEN "Price_6_Hrs"
        WHEN full_rate_segment = '12 Hrs' THEN "Price_12_Hrs"
        WHEN full_rate_segment = '24 Hrs' THEN "Price_24_Hrs"
        WHEN full_rate_segment = 'Commuter' THEN "Commuter_Price"
        WHEN full_rate_segment = 'Overnight (12 Hrs)' THEN "Overnight_12_Hrs_Price"
        WHEN full_rate_segment = 'Night (6 Hrs)' THEN "Night_6_Hrs_Price"
        WHEN full_rate_segment = 'Weekend (12 Hrs)' THEN "Weekend_12_Hrs_Price"
        WHEN full_rate_segment = 'Weekend (24 hrs)' THEN "Weekend_24_Hrs_Price"
      ELSE NULL END adjusted_price
    FROM full_rate_suite