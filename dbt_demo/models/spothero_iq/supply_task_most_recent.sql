{{ config(tags=["presto","spothero_iq"]) }}

select * from {{ ref('pg_supply_task_most_recent') }}