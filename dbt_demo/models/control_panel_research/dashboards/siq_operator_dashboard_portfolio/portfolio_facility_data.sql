{{ config(tags="spotheroiq_operator_dashboard_portfolio") }}

SELECT * FROM (
SELECT *, DENSE_RANK() OVER (ORDER BY z___min_rank) as z___pivot_row_rank, RANK() OVER (PARTITION BY z__pivot_col_rank ORDER BY z___min_rank) as z__pivot_col_ordering, CASE WHEN z___min_rank = z___rank THEN 1 ELSE 0 END AS z__is_highest_ranked_cell FROM (
SELECT *, MIN(z___rank) OVER (PARTITION BY "rates_experiments.parking_spot_title","rates_experiments.facility_id","rates_experiments.algorithm") as z___min_rank FROM (
SELECT *, RANK() OVER (ORDER BY CASE WHEN z__pivot_col_rank=1 THEN 1 ELSE 2 END, CASE WHEN z__pivot_col_rank=1 THEN "rates_experiments.search_count" ELSE NULL END DESC NULLS LAST, "rates_experiments.search_count" DESC, z__pivot_col_rank, "rates_experiments.parking_spot_title", "rates_experiments.facility_id", "rates_experiments.algorithm") AS z___rank FROM (
SELECT *, DENSE_RANK() OVER (ORDER BY "rates_experiments.searches_parquet_presto_millennium_park_experiment_algorithm" DESC NULLS LAST) AS z__pivot_col_rank FROM (
SELECT
	rates_experiments."experiment_or_control"  AS "rates_experiments.searches_parquet_presto_millennium_park_experiment_algorithm",
	rates_experiments.title  AS "rates_experiments.parking_spot_title",
	rates_experiments.facility_id  AS "rates_experiments.facility_id",
	rates_experiments.algorithm  AS "rates_experiments.algorithm",
	COALESCE(SUM((rates_experiments."search_count") ), 0) AS "rates_experiments.search_count"
FROM {{ ref('pg_price_experiment') }}   AS rates_experiments

WHERE ((((rates_experiments."requested_time_starts" ) >= ((DATEADD(week,-4, DATE_TRUNC('week', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE()))) ))) AND (rates_experiments."requested_time_starts" ) < ((DATEADD(week,4, DATEADD(week,-4, DATE_TRUNC('week', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE()))) ) )))))) AND (rates_experiments.facility_id  IN (2840,1406,12247,2191,2189,2916,2187,2186,8006,8010,1886,14354,1879,1893,1877,1881,5873,10283,1887,7289,2993,14443,11607,13838,6111,11858,7946,7995))
GROUP BY 1,2,3,4) ww
) bb WHERE z__pivot_col_rank <= 16384
) aa
) xx
) zz
 WHERE (z__pivot_col_rank <= 50 OR z__is_highest_ranked_cell = 1) AND (z___pivot_row_rank <= 500 OR z__pivot_col_ordering = 1) ORDER BY z___pivot_row_rank