{{ config(tags=["dashboard","spothero_iq"]) }}

select * from {{ ref('adjusted_rate_recommendations') }}

union

select * from {{ ref('automated_projections_output_summarized') }}

union

select * from {{ ref('competitive_rate_segment_performance') }}

union

select * from {{ ref('daphne_spothero_lookup_presto') }}

union

select * from {{ ref('reporting_market_presto') }}

union

select * from {{ ref('sfdc_user_ae_presto') }}

union

select * from {{ ref('supply_task_most_recent') }}