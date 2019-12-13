SELECT
  origin_parking_spot.parking_spot_id AS origin_parking_spot_id
  ,origin_opportunity.id origin_opportunity_id
  ,comp_parking_spot.parking_spot_id AS comp_parking_spot_id
  ,comp_opportunity.id comp_opportunity_id
  ,origin_opportunity.lat_long_latitude_s AS origin_location_lat
  ,origin_opportunity.lat_long_longitude_s AS origin_location_lon
  ,CASE WHEN origin_opportunity.id = comp_opportunity.id THEN 0 ELSE ROUND(ACOS(SIN(RADIANS(origin_opportunity.lat_long_latitude_s)) * SIN(RADIANS(comp_opportunity.lat_long_latitude_s)) + COS(RADIANS(origin_opportunity.lat_long_latitude_s)) * COS(RADIANS(comp_opportunity.lat_long_latitude_s)) * COS(RADIANS(comp_opportunity.lat_long_longitude_s - origin_opportunity.lat_long_longitude_s))) * 6371/1.60934, 2) END AS distance_miles
FROM {{ source('sfdc', 'opportunity') }} origin_opportunity
LEFT JOIN {{ source('sh_public', 'parking_spot') }} origin_parking_spot ON origin_parking_spot.parking_spot_id = origin_opportunity.spot_id_c AND NOT origin_opportunity.is_deleted
CROSS JOIN {{ source('sfdc', 'opportunity') }} comp_opportunity
LEFT JOIN {{ source('sh_public', 'parking_spot') }} comp_parking_spot ON comp_parking_spot.parking_spot_id = comp_opportunity.spot_id_c AND NOT comp_opportunity.is_deleted
WHERE (ACOS(SIN(RADIANS(origin_opportunity.lat_long_latitude_s)) * SIN(RADIANS(comp_opportunity.lat_long_latitude_s)) + COS(RADIANS(origin_opportunity.lat_long_latitude_s)) * COS(RADIANS(comp_opportunity.lat_long_latitude_s)) * COS(RADIANS(comp_opportunity.lat_long_longitude_s - origin_opportunity.lat_long_longitude_s))) * 6371 <= 1.60934 OR origin_opportunity.id = comp_opportunity.id)
GROUP BY 1,2,3,4,5,6,7