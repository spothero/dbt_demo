-- raw sql results do not include filled-in values for 'transient_inventory_status.starts_week'


SELECT * FROM (
SELECT *, DENSE_RANK() OVER (ORDER BY z___min_rank) as z___pivot_row_rank, RANK() OVER (PARTITION BY z__pivot_col_rank ORDER BY z___min_rank) as z__pivot_col_ordering, CASE WHEN z___min_rank = z___rank THEN 1 ELSE 0 END AS z__is_highest_ranked_cell FROM (
SELECT *, MIN(z___rank) OVER (PARTITION BY "transient_inventory_status.starts_day_of_week_index","transient_inventory_status.starts_day_of_week","transient_inventory_status.starts_hour_of_day") as z___min_rank FROM (
SELECT *, RANK() OVER (ORDER BY "transient_inventory_status.starts_day_of_week_index" ASC, "transient_inventory_status.starts_hour_of_day" ASC, z__pivot_col_rank, "transient_inventory_status.starts_day_of_week") AS z___rank FROM (
SELECT *, DENSE_RANK() OVER (ORDER BY "transient_inventory_status.starts_week" NULLS LAST) AS z__pivot_col_rank FROM (
SELECT
	TO_CHAR(DATE_TRUNC('week', CONVERT_TIMEZONE('UTC', 'America/Chicago', transient_inventory_status.starts )), 'YYYY-MM-DD') AS "transient_inventory_status.starts_week",
	MOD(EXTRACT(DOW FROM CONVERT_TIMEZONE('UTC', 'America/Chicago', transient_inventory_status.starts ))::integer - 1 + 7, 7) AS "transient_inventory_status.starts_day_of_week_index",
	TRIM(TO_CHAR(CONVERT_TIMEZONE('UTC', 'America/Chicago', transient_inventory_status.starts ), 'Day')) AS "transient_inventory_status.starts_day_of_week",
	DATE_PART(hour, CONVERT_TIMEZONE('UTC', 'America/Chicago', transient_inventory_status.starts ))::integer AS "transient_inventory_status.starts_hour_of_day",
	AVG((transient_inventory_status.transient_total - transient_inventory_status.transient_available) ) AS "transient_inventory_status.average_inventory_sold",
	AVG(transient_inventory_status.transient_total ) AS "transient_inventory_status.average_inventory_total"
FROM {{ source( 'sh_public','spothero_inventoryavailabilitystatus') }}  AS transient_inventory_status
LEFT JOIN {{ source( 'sh_public','parking_spot') }}  AS parking_spot ON transient_inventory_status.spot_id = parking_spot.parking_spot_id

WHERE ((((transient_inventory_status.starts ) >= ((CONVERT_TIMEZONE('America/Chicago', 'UTC', DATEADD(week,-3, DATE_TRUNC('week', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE()))) )))) AND (transient_inventory_status.starts ) < ((CONVERT_TIMEZONE('America/Chicago', 'UTC', DATEADD(week,4, DATEADD(week,-3, DATE_TRUNC('week', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE()))) ) ))))))) AND (NOT COALESCE(transient_inventory_status.is_cnp , FALSE)) AND (parking_spot.parking_spot_id  = 2840)
GROUP BY DATE_TRUNC('week', CONVERT_TIMEZONE('UTC', 'America/Chicago', transient_inventory_status.starts )),2,3,4) ww
) bb WHERE z__pivot_col_rank <= 16384
) aa
) xx
) zz
 WHERE (z__pivot_col_rank <= 50 OR z__is_highest_ranked_cell = 1) AND (z___pivot_row_rank <= 500 OR z__pivot_col_ordering = 1) ORDER BY z___pivot_row_rank