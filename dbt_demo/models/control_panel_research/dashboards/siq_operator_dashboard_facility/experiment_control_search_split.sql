SELECT
	rates_experiments."experiment_or_control"  AS "rates_experiments.searches_parquet_presto_millennium_park_experiment_algorithm",
	COALESCE(SUM((rates_experiments."search_count") ), 0) AS "rates_experiments.search_count"
FROM {{ ref('pg_price_experiment') }}   AS rates_experiments

WHERE ((((rates_experiments."requested_time_starts" ) >= ((DATEADD(week,-4, DATE_TRUNC('week', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE()))) ))) AND (rates_experiments."requested_time_starts" ) < ((DATEADD(week,4, DATEADD(week,-4, DATE_TRUNC('week', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE()))) ) )))))) AND ((rates_experiments."requested_time_starts"  < (DATEADD(day,0, DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE())) )))) AND ((TRIM(TO_CHAR(rates_experiments."requested_time_starts" , 'Day')) NOT IN ('Saturday', 'Sunday') OR (TRIM(TO_CHAR(rates_experiments."requested_time_starts" , 'Day'))) IS NULL)) AND (rates_experiments.facility_id  = 2840)
GROUP BY 1
ORDER BY 1 DESC
LIMIT 500