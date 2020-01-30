{{ config(tags=["presto"]) }}

SELECT
  rental_id,
  CAST(FORMAT_DATETIME(
                  CASE
                    WHEN timezone = 'America/Chicago' THEN starts AT TIME ZONE 'America/Chicago'
                    WHEN timezone = 'America/Los_Angeles' THEN starts  AT TIME ZONE 'America/Los_Angeles'
                    WHEN timezone = 'America/New_York' THEN starts  AT TIME ZONE 'America/New_York'
                    WHEN timezone = 'America/Phoenix' THEN starts AT TIME ZONE 'America/Phoenix'
                    WHEN timezone = 'America/Denver' THEN starts AT TIME ZONE 'America/Denver'
                    ELSE NULL
                  END
                  ,'yyyy-MM-dd HH:mm:ss.SSS'
                  ) AS TIMESTAMP) as starts_local_timezone,
  CAST(FORMAT_DATETIME(
                  CASE
                    WHEN timezone = 'America/Chicago' THEN created AT TIME ZONE 'America/Chicago'
                    WHEN timezone = 'America/Los_Angeles' THEN created  AT TIME ZONE 'America/Los_Angeles'
                    WHEN timezone = 'America/New_York' THEN created  AT TIME ZONE 'America/New_York'
                    WHEN timezone = 'America/Phoenix' THEN created AT TIME ZONE 'America/Phoenix'
                    WHEN timezone = 'America/Denver' THEN created AT TIME ZONE 'America/Denver'
                    ELSE NULL
                  END
                  ,'yyyy-MM-dd HH:mm:ss.SSS'
                  ) AS TIMESTAMP) as created_local_timezone,
  CAST(FORMAT_DATETIME(
                  CASE
                    WHEN timezone = 'America/Chicago' THEN ends AT TIME ZONE 'America/Chicago'
                    WHEN timezone = 'America/Los_Angeles' THEN ends  AT TIME ZONE 'America/Los_Angeles'
                    WHEN timezone = 'America/New_York' THEN ends  AT TIME ZONE 'America/New_York'
                    WHEN timezone = 'America/Phoenix' THEN ends AT TIME ZONE 'America/Phoenix'
                    WHEN timezone = 'America/Denver' THEN ends AT TIME ZONE 'America/Denver'
                    ELSE NULL
                  END
                  ,'yyyy-MM-dd HH:mm:ss.SSS'
                  ) AS TIMESTAMP) as ends_local_timezone
FROM {{ ref('pg_rentals_past_eight_weeks') }}