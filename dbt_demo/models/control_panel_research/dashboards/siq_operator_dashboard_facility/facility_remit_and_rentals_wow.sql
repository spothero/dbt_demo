-- raw sql results do not include filled-in values for 'accounting_export_line.effective_week'


SELECT
	TO_CHAR(DATE_TRUNC('week', accounting_export_line.effective_date ), 'YYYY-MM-DD') AS "accounting_export_line.effective_week",
	COUNT(DISTINCT CASE WHEN (accounting_export_line.action_type = 'PURCHASE') AND rental_facts.is_good_rental THEN accounting_export_line.reservation_id  ELSE NULL END) AS "accounting_export_line.reservation_count",
	COALESCE(SUM((CASE WHEN spothero_city.currency_type = 'cad' THEN accounting_export_line.remit/pg_currency_exchange_rate.cad_exchange_rate ELSE accounting_export_line.remit END) + (CASE WHEN spothero_city.currency_type = 'cad' THEN accounting_export_line.manual_adjustment_remit/pg_currency_exchange_rate.cad_exchange_rate ELSE accounting_export_line.manual_adjustment_remit END) + (CASE WHEN spothero_city.currency_type = 'cad' THEN accounting_export_line.refund_remit/pg_currency_exchange_rate.cad_exchange_rate ELSE accounting_export_line.refund_remit END) ), 0) AS "accounting_export_line.total_remit_no_filter"
FROM {{ source('controlpanel_public','line_item') }}  AS accounting_export_line
LEFT JOIN {{ source('sh_public','parking_spot') }}  AS parking_spot ON accounting_export_line.facility_id = parking_spot.parking_spot_id
LEFT JOIN {{ source('sh_public','spothero_city') }}  AS spothero_city ON parking_spot.spothero_city_id = spothero_city.spothero_city_id
LEFT JOIN {{ ref('pg_rentals') }}  AS rental ON accounting_export_line.reservation_id = rental.rental_id
LEFT JOIN {{ ref('pg_currency_exchange_rate') }}  AS pg_currency_exchange_rate ON (DATE(CONVERT_TIMEZONE('UTC', 'America/Chicago', rental.created ))) = (DATE(pg_currency_exchange_rate.day ))
LEFT JOIN {{ ref('pg_rental_facts') }}  AS rental_facts ON accounting_export_line.reservation_id = rental_facts.rental_id

WHERE ((((accounting_export_line.effective_date ) >= ((DATEADD(week,-4, DATE_TRUNC('week', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE()))) ))) AND (accounting_export_line.effective_date ) < ((DATEADD(week,4, DATEADD(week,-4, DATE_TRUNC('week', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE()))) ) )))))) AND (parking_spot.parking_spot_id  = 2840) AND (NOT accounting_export_line._fivetran_deleted OR accounting_export_line._fivetran_deleted IS NULL)
GROUP BY DATE_TRUNC('week', accounting_export_line.effective_date )
ORDER BY 1 DESC
LIMIT 500