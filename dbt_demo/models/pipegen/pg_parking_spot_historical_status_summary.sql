{{ config(tags=["redshift"]) }}

WITH x as (
    SELECT
        facility_id
        ,status_change_date
        ,last_new_value
        ,ROW_NUMBER() OVER (PARTITION by facility_id ORDER BY status_change_date) as row
    FROM (
SELECT distinct facility_id
,DATE(convert_timezone ('UTC', spothero_city.timezone,  change_date)) as status_change_date
,LAST_VALUE(new_status) OVER (PARTITION by facility_id, DATE(convert_timezone ('UTC', spothero_city.timezone, change_date)) ORDER BY change_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_new_value
FROM {{ source('core','logs_facility_status') }}
LEFT JOIN {{ source('sh_public','parking_spot') }} on parking_spot.parking_spot_id = logs_facility_status.facility_id
LEFT JOIN {{ source('sh_public','spothero_city') }}  AS spothero_city ON parking_spot.spothero_city_id = spothero_city.spothero_city_id
)
)
SELECT
x1.facility_id
,x1.status_change_date as status_start_date
,x2.status_change_date as status_end_date
,x1.last_new_value as status
FROM
x x1
LEFT JOIN x x2 on x2.facility_id = x1.facility_id AND x2.row - 1 = x1.row