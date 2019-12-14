 WITH title_starts_summary AS (
  SELECT
    concat_key AS title_starts
    ,title_starts_summary_sub.sh_event_id AS sh_event_id
    ,count_ids
  FROM {{ source('sh_public','event') }} event
  INNER JOIN (
    SELECT
      event.title||event.starts concat_key,
      MAX(event_id) AS sh_event_id,
      COUNT(DISTINCT event_id) AS count_ids
    FROM {{ source('sh_public','event') }}
    WHERE NOT deleted AND seatgeek_id IS NULL
    GROUP BY 1
  ) title_starts_summary_sub ON event.title||event.starts = title_starts_summary_sub.concat_key
  WHERE NOT deleted AND seatgeek_id IS NULL
  GROUP BY 1,2,3
)

,seo_url_summary AS (
  SELECT 
    event.seo_url,
    MAX(event.event_id) sh_event_id,
    COUNT(event_id) count_seo_url
  FROM title_starts_summary
  INNER JOIN {{ source('sh_public','event') }} event ON title_starts_summary.sh_event_id = event.event_id
  WHERE count_ids = 1
  GROUP BY 1
)

SELECT
  summarized_event_table.spothero_ref_event_id parent_event_id,
  COALESCE(event_sg_fan.event_id,event_title_start_fan.event_id,event_seo_url_fan.event_id) sh_event_id,
  summarized_event_table.seatgeek_id,
  summarized_event_table.category
FROM (
  SELECT 
    sh_event_id AS spothero_ref_event_id
    ,CAST(NULL AS INT) sh_event_id
    ,seatgeek_id
    ,NULL seo_url
    ,NULL title_starts
    ,CASE WHEN count_sg > 1 THEN 'SeatGeek Event - duplicate' ELSE 'SeatGeek Event - non-duplicate' END AS category
  FROM
  (
    SELECT 
      seatgeek_id
      ,MAX(event_id) sh_event_id
      ,COUNT(event_id) count_sg
    FROM {{ source('sh_public','event') }}
    WHERE seatgeek_id IS NOT NULL AND NOT deleted
    GROUP BY 1
  )
  UNION ALL
  SELECT
    sh_event_id AS spothero_ref_event_id
    ,sh_event_id AS sh_event_id
    ,CAST(NULL AS INT) AS seatgeek_id
    ,seo_url
    ,NULL AS title_starts
    ,CASE WHEN count_seo_url > 1 THEN 'Manual Event - seo_url duplicate' ELSE 'Manual Event - non-duplicate' END AS category
  FROM seo_url_summary
  UNION ALL
  SELECT
    sh_event_id AS spothero_ref_event_id
    ,sh_event_id AS sh_event_id
    ,CAST(NULL AS INT) AS seatgeek_id
    ,NULL AS seo_url
    ,title_starts
    ,'Manual Event - Title, Starts duplicate' AS category
  FROM title_starts_summary
  WHERE count_ids > 1
) summarized_event_table
LEFT JOIN {{ source('sh_public','event') }} event_sg_fan ON summarized_event_table.seatgeek_id = event_sg_fan.seatgeek_id AND event_sg_fan.seatgeek_id IS NOT NULL AND NOT event_sg_fan.deleted
LEFT JOIN (
  SELECT
    event.event_id
    ,seo_url
  FROM {{ source('sh_public','event') }}
  LEFT JOIN title_starts_summary ON title_starts_summary.title_starts = event.title||event.starts
  WHERE (count_ids = 1 OR count_ids IS NULL) AND event.seatgeek_id IS NULL AND (NOT event.deleted)
) event_seo_url_fan ON summarized_event_table.seo_url = event_seo_url_fan.seo_url
LEFT JOIN (
  SELECT
    event.event_id
    ,event.title||event.starts title_starts
  FROM {{ source('sh_public','event') }} event
  LEFT JOIN title_starts_summary ON title_starts_summary.title_starts = event.title||event.starts
  WHERE (count_ids > 1) AND event.seatgeek_id IS NULL AND (NOT event.deleted)
) event_title_start_fan ON summarized_event_table.title_starts = event_title_start_fan.title_starts
GROUP BY 1,2,3,4