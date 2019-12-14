SELECT
  parking_spot.parking_spot_id
  ,origin.id AS opportunity_id
  ,destination.destination_id AS destination_id
  ,destination.title AS destination_title
  ,CASE WHEN destination.seatgeek_id IS NOT NULL THEN CONCAT('SG',destination.seatgeek_id) ELSE CONCAT('SH',destination.destination_id) END AS aggregated_destination_id
  ,(ACOS(SIN(RADIANS(origin.lat_long_latitude_s)) * SIN(RADIANS(destination.location_lat)) + COS(RADIANS(origin.lat_long_latitude_s)) * COS(RADIANS(destination.location_lat)) * COS(RADIANS(destination.location_lon - origin.lat_long_longitude_s))) * 6371) / 1.60934 distance_miles
  ,ROW_NUMBER() OVER (PARTITION BY opportunity_id ORDER BY distance_miles) opportunity_surrounding_destinations_rank
  ,ROW_NUMBER() OVER (PARTITION BY destination_id ORDER BY distance_miles) destination_surrounding_opportunities_rank
FROM {{ source('sfdc', 'opportunity') }} origin
LEFT JOIN {{ source('sh_public', 'parking_spot') }} ON parking_spot.parking_spot_id = origin.spot_id_c
CROSS JOIN {{ source('sh_public', 'destination') }} destination
WHERE (NOT origin.is_deleted) AND (ACOS(SIN(RADIANS(origin.lat_long_latitude_s)) * SIN(RADIANS(destination.location_lat)) + COS(RADIANS(origin.lat_long_latitude_s)) * COS(RADIANS(destination.location_lat)) * COS(RADIANS(destination.location_lon - origin.lat_long_longitude_s))) * 6371 <= 3.21869)
GROUP BY 1,2,3,4,5,6