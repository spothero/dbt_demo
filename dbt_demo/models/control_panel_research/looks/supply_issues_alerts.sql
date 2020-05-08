SELECT
	sfdc_opportunity.id  AS "sfdc_opportunity.id",
	rental.rental_id  AS "rental.rental_id",
	TO_CHAR(CONVERT_TIMEZONE('UTC', 'America/Chicago', CASE
          WHEN sfdc_case.desk_created_date_c IS NOT NULL then sfdc_case.desk_created_date_c
          ELSE sfdc_case.created_date
          END), 'YYYY-MM-DD HH24:MI:SS') AS "sfdc_case.created_time",
	TO_CHAR(CONVERT_TIMEZONE('UTC', 'America/Chicago', rental.starts ), 'YYYY-MM-DD HH24:MI:SS') AS "rental.starts_time",
	TO_CHAR(CONVERT_TIMEZONE('UTC', 'America/Chicago', rental.ends), 'YYYY-MM-DD HH24:MI:SS') AS "rental.ends_time",
	parking_spot.title  AS "parking_spot.title",
	parking_spot.parking_spot_id  AS "parking_spot.parking_spot_id",
	parking_spot_facts.location_lat  AS "parking_spot_facts.location_lat",
	parking_spot_facts.location_lon  AS "parking_spot_facts.location_lon",
	REPLACE(parking_spot_facts.street_address ,' ','-') AS "parking_spot_facts.street_address_pw",
	REPLACE(parking_spot_facts.city_address ,' ','-') AS "parking_spot_facts.city_address_pw",
	RPAD(LEFT(CAST(parking_spot_facts.location_lat AS text),10),10,0) AS "parking_spot_facts.location_lat_pw",
	RPAD(LEFT(CAST(parking_spot_facts.location_lon AS text),10),10,0) AS "parking_spot_facts.location_lon_pw",
	CASE
WHEN rental_facts.rental_segment IN ('Commuter', 'Short-term Weekday', 'Weekday PM','Weekend Day','Multiday','Other','Airport')  THEN '0'
WHEN rental_facts.rental_segment IN ('Event')  THEN '1'
WHEN rental_facts.rental_segment IN ('Monthly Recurrable', 'Monthly Nonrecurrable')  THEN '2'

END AS "rental_facts.rental_segment_rollup_event_transient_monthly__sort_",
	CASE
WHEN rental_facts.rental_segment IN ('Commuter', 'Short-term Weekday', 'Weekday PM','Weekend Day','Multiday','Other','Airport')  THEN 'Regular Transient'
WHEN rental_facts.rental_segment IN ('Event')  THEN 'Event'
WHEN rental_facts.rental_segment IN ('Monthly Recurrable', 'Monthly Nonrecurrable')  THEN 'Monthly'

END AS "rental_facts.rental_segment_rollup_event_transient_monthly",
	company.name  AS "company.name",
	hero_tag_c.hero_tag_type_c || ' - ' || hero_tag_c.hero_tag_category_c || ' - ' || hero_tag_c.name  AS "sfdc_case.full_tag"
FROM {{ source('sfdc','case') }}  AS sfdc_case
FULL OUTER JOIN pipegen.pg_rentals  AS rental ON sfdc_case.rental_id_c = rental.rental_id
LEFT JOIN {{ ref('pg_rental_facts') }}  AS rental_facts ON rental.rental_id = rental_facts.rental_id
LEFT JOIN {{ source('sh_public','parking_spot') }}  AS parking_spot ON rental.parking_spot_id = parking_spot.parking_spot_id
LEFT JOIN {{ ref('pg_parking_spot_facts') }}  AS parking_spot_facts ON parking_spot_facts.parking_spot_id = parking_spot.parking_spot_id
LEFT JOIN {{ source('sh_public','company') }}  AS company ON parking_spot.company_id = company.company_id
LEFT JOIN {{ source('sfdc','opportunity') }}  AS sfdc_opportunity ON parking_spot.parking_spot_id = sfdc_opportunity.spot_id_c
LEFT JOIN {{ source('sfdc','reporting_neighborhood_c') }}  AS reporting_neighborhood ON sfdc_opportunity.reporting_neighborhood_geography_c = reporting_neighborhood.id
LEFT JOIN {{ source('sfdc','reporting_city_c') }}  AS reporting_city ON reporting_neighborhood.reporting_city_c = reporting_city.id
LEFT JOIN {{ source('sfdc','market_c') }}  AS reporting_market ON reporting_city.reporting_market_c = reporting_market.id
LEFT JOIN {{ source('sfdc','hero_tag_c') }}  AS hero_tag_c ON sfdc_case.hero_tag_c = hero_tag_c.id

WHERE ((((CASE
          WHEN sfdc_case.desk_created_date_c IS NOT NULL then sfdc_case.desk_created_date_c
          ELSE sfdc_case.created_date
          END) >= ((CONVERT_TIMEZONE('America/Chicago', 'UTC', DATEADD(week,-3, DATE_TRUNC('week', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE()))) )))) AND (CASE
          WHEN sfdc_case.desk_created_date_c IS NOT NULL then sfdc_case.desk_created_date_c
          ELSE sfdc_case.created_date
          END) < ((CONVERT_TIMEZONE('America/Chicago', 'UTC', DATEADD(week,4, DATEADD(week,-3, DATE_TRUNC('week', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/Chicago', GETDATE()))) ) ))))))) AND ((parking_spot.title  IN ('Millennium Park Garage', '55 E Monroe St. - Garage', 'Greenway Self Park - Garage', '900 N Michigan Ave. - Garage', '2515 N Clark St. - Lurie Children''s Hospital - Garage', '401 N LOWER Michigan Ave. - Valet', 'Hilton Chicago Garage', 'Westin Michigan Ave. - Valet'))) AND ((company.name  IN ('SP+', 'SP+ - Millennium Park Garages'))) AND (reporting_market.name = 'Chicago') AND ((rental.reservation_status  IN ('valid', 'recurrence', 'cancelled'))) AND hero_tag_c.sipp_c AND ((hero_tag_c.hero_tag_type_c || ' - ' || hero_tag_c.hero_tag_category_c || ' - ' || hero_tag_c.name  IN ('Issue - Attendant - Wasn''t Present - Did not park', 'Issue - Attendant - Wasn''t Present - Paid to exit', 'Issue - Attendant - Not Accepted - Other', 'Issue - Attendant - Not Accepted - Doesn''t work with SH', 'Issue - Attendant - Not Accepted - Incorrect Rate', 'Issue - Facility - Closed', 'Issue - Facility - Lot Full - No Spots')))
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
ORDER BY 3 DESC
LIMIT 500