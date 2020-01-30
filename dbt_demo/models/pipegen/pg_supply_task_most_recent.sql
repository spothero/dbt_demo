{{ config(tags=["redshift"]) }}

SELECT *
FROM {{ source('sfdc','supply_task_c') }}
INNER JOIN (SELECT (supply_task_c.location_c || supply_task_c.type_c || supply_task_c.sub_type_c) as concat_key, MAX(created_date) as max_created FROM {{ source('sfdc','supply_task_c') }} WHERE NOT is_deleted AND (NOT status_c = 'Archived' OR status_c IS NULL)  GROUP BY 1 ) a on a.concat_key = (supply_task_c.location_c || supply_task_c.type_c || supply_task_c.sub_type_c) and a.max_created = supply_task_c.created_date
WHERE (NOT supply_task_c.is_deleted AND (NOT status_c = 'Archived' OR status_c IS NULL))