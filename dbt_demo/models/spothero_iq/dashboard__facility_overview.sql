{{ config(tags=["dashboard","spothero_iq"]) }}

select * from {{ ref('entry_and_exit_counts') }}

union

select * from {{ ref('facility') }}

union

select * from {{ ref('occupancy_hourly') }}

union

select * from {{ ref('parking_transactions') }}

union

select * from {{ ref('search_hourly') }}

union

select * from {{ ref('tickets_view') }}

union

select * from {{ ref('transient_inventory_status_hourly') }}