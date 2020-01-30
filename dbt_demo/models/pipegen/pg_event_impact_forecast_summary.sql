{{ config(tags=["redshift"]) }}

SELECT
  base_event_impact.*
  ,parent_event.parent_event_id
  ,event.starts event_starts
  ,parent_destination.parent_destination_id
FROM {{ source('event','sg_event_impact') }} AS base_event_impact
INNER JOIN (
    SELECT event_id,max(prediction_date) AS latest_date
    FROM {{ source('event','sg_event_impact') }}
    GROUP BY 1
) AS  current_event_impact ON base_event_impact.prediction_date = current_event_impact.latest_date AND base_event_impact.event_id = current_event_impact.event_id
LEFT JOIN {{ ref('pg_parent_event') }} parent_event ON parent_event.seatgeek_id = base_event_impact.event_id
LEFT JOIN {{ source('sh_public','event') }} ON event.event_id = parent_event.parent_event_id
LEFT JOIN {{ ref('pg_parent_destination') }} parent_destination ON event.destination_id = parent_destination.sh_destination_id