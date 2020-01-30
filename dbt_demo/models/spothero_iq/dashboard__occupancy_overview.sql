{{ config(tags=["dashboard","spothero_iq"]) }}

select * from {{ ref('daphne_spothero_lookup') }}

union

select * from {{ ref('date_time_placeholder') }}

union

select * from {{ ref('facility') }}

union

select * from {{ ref('occupancy_hourly') }}

union

select * from {{ ref('occupancy_hourly_for_placeholder') }}

union

select * from {{ ref('transient_inventory_status_hourly') }}
