SELECT
	rates_experiments.title  AS "rates_experiments.parking_spot_title",
	rates_experiments.facility_id  AS "rates_experiments.facility_id",
	rates_experiments.algorithm  AS "rates_experiments.algorithm"
FROM {{ ref('pg_price_experiment') }}   AS rates_experiments

WHERE ((((rates_experiments."requested_time_starts" ) >= ((DATEADD(week,-4, DATE_TRUNC('week', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE()))) ))) AND (rates_experiments."requested_time_starts" ) < ((DATEADD(week,4, DATEADD(week,-4, DATE_TRUNC('week', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE()))) ) )))))) AND ((rates_experiments."requested_time_starts"  < (DATEADD(day,0, DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE())) )))) AND ((TRIM(TO_CHAR(rates_experiments."requested_time_starts" , 'Day')) NOT IN ('Saturday', 'Sunday') OR (TRIM(TO_CHAR(rates_experiments."requested_time_starts" , 'Day'))) IS NULL)) AND (rates_experiments.facility_id  = 2840)
GROUP BY 1,2,3
ORDER BY 1
LIMIT 500