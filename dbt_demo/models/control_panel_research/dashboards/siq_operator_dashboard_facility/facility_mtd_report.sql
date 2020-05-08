SELECT * FROM (
SELECT *, DENSE_RANK() OVER (ORDER BY z___min_rank) as z___pivot_row_rank, RANK() OVER (PARTITION BY z__pivot_col_rank ORDER BY z___min_rank) as z__pivot_col_ordering, CASE WHEN z___min_rank = z___rank THEN 1 ELSE 0 END AS z__is_highest_ranked_cell FROM (
SELECT *, MIN(z___rank) OVER (PARTITION BY "sfdc_opportunity.id","parking_spot.title","parking_spot.parking_spot_id","parking_spot_facts.location_lat","parking_spot_facts.location_lon","parking_spot_facts.street_address_pw","parking_spot_facts.city_address_pw","parking_spot_facts.location_lat_pw","parking_spot_facts.location_lon_pw") as z___min_rank FROM (
SELECT *, RANK() OVER (ORDER BY "parking_spot.title" DESC, z__pivot_col_rank, "sfdc_opportunity.id", "parking_spot.parking_spot_id", "parking_spot_facts.location_lat", "parking_spot_facts.location_lon", "parking_spot_facts.street_address_pw", "parking_spot_facts.city_address_pw", "parking_spot_facts.location_lat_pw", "parking_spot_facts.location_lon_pw") AS z___rank FROM (
SELECT *, DENSE_RANK() OVER (ORDER BY "accounting_export_line.effective_month" NULLS LAST, "accounting_export_line.before_mtd_effective" DESC NULLS LAST) AS z__pivot_col_rank FROM (
SELECT
	TO_CHAR(DATE_TRUNC('month', accounting_export_line.effective_date ), 'YYYY-MM') AS "accounting_export_line.effective_month",
	CASE WHEN EXTRACT(DAY FROM accounting_export_line.effective_date::timestamp) < EXTRACT(DAY FROM CONVERT_TIMEZONE('UTC', 'America/Chicago',GETDATE()))  THEN 'Yes' ELSE 'No' END
 AS "accounting_export_line.before_mtd_effective",
	sfdc_opportunity.id  AS "sfdc_opportunity.id",
	parking_spot.title  AS "parking_spot.title",
	parking_spot.parking_spot_id  AS "parking_spot.parking_spot_id",
	parking_spot_facts.location_lat  AS "parking_spot_facts.location_lat",
	parking_spot_facts.location_lon  AS "parking_spot_facts.location_lon",
	REPLACE(parking_spot_facts.street_address ,' ','-') AS "parking_spot_facts.street_address_pw",
	REPLACE(parking_spot_facts.city_address ,' ','-') AS "parking_spot_facts.city_address_pw",
	RPAD(LEFT(CAST(parking_spot_facts.location_lat AS text),10),10,0) AS "parking_spot_facts.location_lat_pw",
	RPAD(LEFT(CAST(parking_spot_facts.location_lon AS text),10),10,0) AS "parking_spot_facts.location_lon_pw",
	COALESCE(SUM((CASE WHEN spothero_city.currency_type = 'cad' THEN accounting_export_line.remit/pg_currency_exchange_rate.cad_exchange_rate ELSE accounting_export_line.remit END) + (CASE WHEN spothero_city.currency_type = 'cad' THEN accounting_export_line.manual_adjustment_remit/pg_currency_exchange_rate.cad_exchange_rate ELSE accounting_export_line.manual_adjustment_remit END) + (CASE WHEN spothero_city.currency_type = 'cad' THEN accounting_export_line.refund_remit/pg_currency_exchange_rate.cad_exchange_rate ELSE accounting_export_line.refund_remit END) ), 0) AS "accounting_export_line.total_remit_no_filter"
FROM {{ source('controlpanel_public','line_item') }}  AS accounting_export_line
LEFT JOIN {{ source('sh_public','parking_spot') }}  AS parking_spot ON accounting_export_line.facility_id = parking_spot.parking_spot_id
LEFT JOIN {{ ref('pg_parking_spot_facts') }}  AS parking_spot_facts ON parking_spot.parking_spot_id = parking_spot_facts.parking_spot_id
LEFT JOIN {{ source('sfdc','opportunity') }}  AS sfdc_opportunity ON parking_spot.parking_spot_id = sfdc_opportunity.spot_id_c
LEFT JOIN {{ source('sh_public','spothero_city') }}  AS spothero_city ON parking_spot.spothero_city_id = spothero_city.spothero_city_id
LEFT JOIN {{ ref('pg_rentals') }}  AS rental ON accounting_export_line.reservation_id = rental.rental_id
LEFT JOIN {{ ref('pg_currency_exchange_rate') }}  AS pg_currency_exchange_rate ON (DATE(CONVERT_TIMEZONE('UTC', 'America/Chicago', rental.created ))) = (DATE(pg_currency_exchange_rate.day ))

WHERE ((((accounting_export_line.effective_date ) >= ((DATEADD(month,-1, DATE_TRUNC('month', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE()))) ))) AND (accounting_export_line.effective_date ) < ((DATEADD(month,2, DATEADD(month,-1, DATE_TRUNC('month', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE()))) ) )))))) AND (parking_spot.parking_spot_id  = 2840) AND (NOT accounting_export_line._fivetran_deleted OR accounting_export_line._fivetran_deleted IS NULL)
GROUP BY DATE_TRUNC('month', accounting_export_line.effective_date ),2,3,4,5,6,7,8,9,10,11) ww
) bb WHERE z__pivot_col_rank <= 16384
) aa
) xx
) zz
 WHERE (z__pivot_col_rank <= 50 OR z__is_highest_ranked_cell = 1) AND (z___pivot_row_rank <= 500 OR z__pivot_col_ordering = 1) ORDER BY z___pivot_row_rank