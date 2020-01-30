{{ config(tags=["redshift"]) }}

WITH full_geo_list AS (
    SELECT
        destination_id
        ,location_lat
        ,location_lon
        ,reporting_region_name
        ,reporting_market_name
        ,reporting_city_names
        ,reporting_neighborhood_names,
        FIRST_VALUE(reporting_region_name) OVER (PARTITION BY destination_id ORDER BY region_count DESC ROWS UNBOUNDED PRECEDING) AS reporting_region_name_summary,
        FIRST_VALUE(reporting_market_name) OVER (PARTITION BY destination_id ORDER BY market_count DESC ROWS UNBOUNDED PRECEDING) AS reporting_market_name_summary,
        FIRST_VALUE(reporting_city_names) OVER (PARTITION BY destination_id ORDER BY city_count DESC ROWS UNBOUNDED PRECEDING) AS reporting_city_names_summary,
        FIRST_VALUE(reporting_neighborhood_names) OVER (PARTITION BY destination_id ORDER BY neighborhood_count DESC ROWS UNBOUNDED PRECEDING) AS reporting_neighborhood_names_summary
    FROM (
      SELECT
        destination.destination_id,
        destination.location_lat,
        destination.location_lon,
        sfdc_opportunity.id,
        reporting_market.region_c AS reporting_region_name,
        reporting_market.name AS reporting_market_name,
        reporting_city.name AS reporting_city_names,
        reporting_neighborhood.name AS reporting_neighborhood_names,
        COUNT(sfdc_opportunity.id) OVER (PARTITION BY destination.destination_id,reporting_market.region_c) region_count,
        COUNT(sfdc_opportunity.id) OVER (PARTITION BY destination.destination_id,reporting_market.name) market_count,
        COUNT(sfdc_opportunity.id) OVER (PARTITION BY destination.destination_id,reporting_city.name) city_count,
        COUNT(sfdc_opportunity.id) OVER (PARTITION BY destination.destination_id,reporting_neighborhood.name) neighborhood_count
      FROM {{ ref('pg_parent_destination') }} AS parent_destination
      INNER JOIN {{ source('sh_public','destination') }} AS destination ON destination.destination_id = parent_destination.parent_destination_id
      INNER JOIN {{ ref('pg_destination_microclimate') }} AS destination_microclimate ON destination.destination_id = destination_microclimate.destination_id
      INNER JOIN {{ source('sfdc','opportunity') }} AS sfdc_opportunity ON destination_microclimate.opportunity_id = sfdc_opportunity.id
      INNER JOIN {{ source('sfdc','reporting_neighborhood_c') }}  AS reporting_neighborhood ON sfdc_opportunity.reporting_neighborhood_geography_c = reporting_neighborhood.id
      INNER JOIN {{ source('sfdc','reporting_city_c') }}  AS reporting_city ON reporting_neighborhood.reporting_city_c = reporting_city.id
      INNER JOIN sfdc.market_c  AS reporting_market ON reporting_city.reporting_market_c = reporting_market.id
      GROUP BY 1,2,3,4,5,6,7,8
    )
  )

  SELECT
    destination_id
    ,location_lat
    ,location_lon
    ,reporting_region_name_summary reporting_region_name
    ,reporting_market_name_summary reporting_market_name
    ,reporting_city_names_summary reporting_city_name
    ,reporting_neighborhood_names_summary reporting_neighborhood_name
  FROM full_geo_list
  GROUP BY 1,2,3,4,5,6,7