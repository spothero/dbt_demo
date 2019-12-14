  with x as (
              SELECT
                y.*
                ,FIRST_VALUE(cad_exchange_rate) OVER(ORDER BY day desc ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as most_recent
              FROM (
                    SELECT
                      DATE(forex.forex_exchange_date) AS day
                      ,AVG(forex.USD_CAD ) AS cad_exchange_rate
                    FROM (
                          SELECT cast(forex.exchange_date as timestamp) as forex_exchange_date, usd_cad
                          FROM {{ source('exchangerate','forex') }}  AS forex
                          UNION ALL
                          SELECT
                          cast(forex_real.exchange_date as timestamp) AS forex_exchange_date, forex_real.CAD *(1/forex_real.USD) AS usd_cad
                          FROM {{ source('exchangerate','forex_real') }}  AS forex_real
                          ) as forex
                    GROUP BY 1
                    ) y
            )
  SELECT day, cad_exchange_rate FROM x 
  UNION 
  SELECT date as day ,z.most_recent as cad_exchange_rate
  FROM {{ source('spothero_csv', 'date_series') }}
  CROSS JOIN (SELECT MAX(day) as max_day, MAX(most_recent) as most_recent from x) z
  WHERE date > max_day AND date < DATE_ADD('day',7,CURRENT_DATE)