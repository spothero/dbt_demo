{{ config(tags=["dashboard","spothero_iq"]) }}

select * from {{ ref('daphne_spothero_lookup') }}

union

select * from {{ ref('destination') }}

union

select * from {{ ref('event') }}

union

select * from {{ ref('event_destination_mapping_table') }}

union

select * from {{ ref('event_impact_forecast_summary') }}

union

select * from {{ ref('event_type_rollup') }}

union

select * from {{ ref('historical_event_inventory') }}

union

select * from {{ ref('tiered_event_rate') }}

union

select * from {{ ref('sfdc_user_ae') }}
