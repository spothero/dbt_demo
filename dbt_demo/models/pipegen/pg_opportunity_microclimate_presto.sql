{{ config(tags=["presto"]) }}

SELECT
 origin.id AS origin_opportunity_id,
 comp.id AS comp_opportunity_id,
 CASE WHEN origin.id = comp.id THEN 'Target Facility' ELSE 'Microclimate' END AS is_origin,
 (ACOS(SIN(RADIANS(origin.lat_long_latitude_s)) * SIN(RADIANS(comp.lat_long_latitude_s)) + COS(RADIANS(origin.lat_long_latitude_s)) * COS(RADIANS(comp.lat_long_latitude_s)) * COS(RADIANS(comp.lat_long_longitude_s - origin.lat_long_longitude_s))) * 6371) / 1.60934 AS distance_miles
FROM {{ source('sfdc','opportunity') }} AS origin
CROSS JOIN {{ source('sfdc','opportunity') }} AS comp
WHERE (ACOS(SIN(RADIANS(origin.lat_long_latitude_s)) * SIN(RADIANS(comp.lat_long_latitude_s)) + COS(RADIANS(origin.lat_long_latitude_s)) * COS(RADIANS(comp.lat_long_latitude_s)) * COS(RADIANS(comp.lat_long_longitude_s - origin.lat_long_longitude_s))) * 6371) <= 1.60934 OR origin.id = comp.id
GROUP BY 1,2,3,4