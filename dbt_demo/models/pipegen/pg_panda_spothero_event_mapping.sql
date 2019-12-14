WITH sh_event_local_tz AS (
  SELECT 
    event.event_id
    ,parent_event.parent_event_id
    ,convert_timezone('UTC', spothero_city.timezone, event.starts)||'-'||destination.seatgeek_id AS sh_start_sg_venue_key
    ,convert_timezone('UTC', spothero_city.timezone, event.starts)||'-'||event.title AS sh_start_title_key
  FROM {{ source('sh_public','event') }} event
  LEFT JOIN {{ source('sh_public','destination') }} destination ON event.destination_id = destination.destination_id
  LEFT JOIN {{ source('sh_public','spothero_city') }} spothero_city ON destination.spothero_city_id = spothero_city.spothero_city_id
  LEFT JOIN {{ ref('pg_parent_event') }} parent_event ON parent_event.sh_event_id = event.event_id
  WHERE event.seatgeek_id NOT IN (SELECT DISTINCT panda_event.nseatgeekeventid FROM parkingpanda_dbo.seo_event panda_event)
  GROUP BY 1,2,3,4
)

SELECT 
  panda_event.idseo_event panda_event_id
  ,MAX(COALESCE(sg_match_parent_event.parent_event_id,venue_match_parent_event.parent_event_id,title_starts_match_parent_event.parent_event_id)) AS sh_parent_event_id
  ,MAX(CASE WHEN sg_match_parent_event.parent_event_id IS NOT NULL THEN 'SG ID Match' 
    WHEN venue_match_parent_event.parent_event_id IS NOT NULL THEN 'SG Venue-Starts Match'
    WHEN title_starts_match_parent_event.parent_event_id IS NOT NULL THEN 'Title-Starts Match'
  ELSE 'No Match' END) AS category
FROM {{ source('parkingpanda_dbo' ,'seo_event') }} panda_event
LEFT JOIN {{ source('parkingpanda_dbo' ,'seo') }} panda_venue ON panda_event.idseo = panda_venue.idseo
LEFT JOIN {{ ref('pg_parent_event') }} sg_match_parent_event ON sg_match_parent_event.seatgeek_id = panda_event.nseatgeekeventid
LEFT JOIN sh_event_local_tz AS venue_match_parent_event ON panda_event.dstart||'-'||panda_venue.nseatgeekid = venue_match_parent_event.sh_start_sg_venue_key
LEFT JOIN sh_event_local_tz AS title_starts_match_parent_event ON panda_event.dstart||'-'||panda_event.cdescription = title_starts_match_parent_event.sh_start_title_key
GROUP BY 1