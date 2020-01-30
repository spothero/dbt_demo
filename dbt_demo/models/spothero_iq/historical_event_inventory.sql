{{ config(tags=["redshift","spothero_iq"]) }}

select * from {{ ref('pg_historical_event_inventory') }}