{{ config(tags=["redshift"]) }}

WITH y as (
SELECT * FROM {{ ref('pg_parking_spot_historical_status_summary') }}
)
SELECT
  date_series.date as date
  ,y.facility_id
  ,y.status
FROM {{ source('spothero_csv','date_series') }}
LEFT JOIN y on (y.status_start_date <= date_series.date and y.status_end_date > date_series.date) OR (y.status_start_date <= date_series.date and y.status_end_date IS NULL)
WHERE date_series.date <= DATEADD(day,1, DATE(GETDATE())) 
