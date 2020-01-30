{{ config(tags=["redshift","spothero_iq"]) }}

select * from {{ ref('pg_event_impact_forecast_summary') }}