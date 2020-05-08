  with z as (
          with y as (
          with x as (
          SELECT
            transient_inventory_status.spot_id
            ,transient_inventory_status.starts
            ,transient_inventory_status.ends
            ,transient_inventory_status.transient_available
            ,CASE WHEN (starts - LAG(starts) OVER (PARTITION by spot_id ORDER by starts)) = '0 years 0 mons 0 days 0 hours 30 mins 0.00 secs' THEN TRUE ELSE FALSE END as no_30_min_gap


          FROM {{ source('sh_public','spothero_inventoryavailabilitystatus') }}  AS transient_inventory_status
          LEFT JOIN {{ source('sfdc','opportunity') }}  AS sfdc_opportunity ON transient_inventory_status.spot_id = sfdc_opportunity.spot_id_c
          LEFT JOIN {{ source('sfdc','reporting_neighborhood_c') }}  AS reporting_neighborhood ON sfdc_opportunity.reporting_neighborhood_geography_c = reporting_neighborhood.id
          LEFT JOIN {{ source('sfdc','reporting_city_c') }}  AS reporting_city ON reporting_neighborhood.reporting_city_c = reporting_city.id
          LEFT JOIN {{ source('sfdc','market_c') }}  AS reporting_market ON reporting_city.reporting_market_c = reporting_market.id
          LEFT JOIN {{ source('sh_public','parking_spot') }}  AS parking_spot ON transient_inventory_status.spot_id = parking_spot.parking_spot_id
          LEFT JOIN {{ source('sh_public','spothero_city') }}  AS spothero_city ON parking_spot.spothero_city_id = spothero_city.spothero_city_id


          WHERE (((CONVERT_TIMEZONE('UTC', spothero_city.timezone, transient_inventory_status.starts ) >= ((DATEADD(week,-1, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) AND CONVERT_TIMEZONE('UTC', spothero_city.timezone, transient_inventory_status.starts ) < ((DATEADD(week,1, DATEADD(week,-1, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ) )))))) AND (NOT COALESCE(transient_inventory_status.is_cnp , FALSE)) and transient_available > 0 and (NOT is_unavailable OR (unavailable_reason in ('')) OR unavailable_reason = 'outside operation hours') AND reporting_neighborhood.is_airport_parking_c AND NOT COALESCE(parking_spot.actual_spot_id IS NOT NULL , FALSE)

          )

          SELECT
              x.*
           
           ,(row_number() over (order by spot_id, starts) - row_number() over (partition by no_30_min_gap order by spot_id, starts)) as uninterrupted_span_occurence


          FROM x
          
          )

          SELECT
              spot_id
              ,uninterrupted_span_occurence
              ,(COUNT(starts) + 1)/2 as uninterrupted_hours
              ,AVG(transient_available) as avg_stalls_available
              ,MIN(transient_available) as min_stalls_available
          FROM y
          WHERE  no_30_min_gap
          GROUP BY 1,2
          )

          SELECT
                spot_id as parking_spot_id
                ,SUM(uninterrupted_hours/120 * avg_stalls_available) as est_p1w_avg_airport_stalls_available
                ,SUM(uninterrupted_hours/120 * min_stalls_available) as est_p1w_min_airport_stalls_available
          FROM z
          WHERE uninterrupted_hours >= 120
          GROUP BY 1