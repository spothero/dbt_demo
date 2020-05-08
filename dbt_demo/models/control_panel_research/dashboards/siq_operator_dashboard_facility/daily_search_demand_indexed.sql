SELECT
	DATE(rates_experiments."requested_time_starts" ) AS "rates_experiments.requested_time_starts_date_date",
	COALESCE(SUM((rates_experiments."search_count") ), 0) AS "rates_experiments.search_count"
FROM {{ ref('pg_price_experiment') }}   AS rates_experiments

WHERE ((((rates_experiments."requested_time_starts" ) >= ((DATEADD(week,-4, DATE_TRUNC('week', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE()))) ))) AND (rates_experiments."requested_time_starts" ) < ((DATEADD(week,4, DATEADD(week,-4, DATE_TRUNC('week', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE()))) ) )))))) AND ((rates_experiments."requested_time_starts"  < (DATEADD(day,0, DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE())) )))) AND (rates_experiments.facility_id  = 2840)
GROUP BY 1
ORDER BY 1 DESC
LIMIT 500