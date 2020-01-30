{{ config(tags=["dashboard","spothero_iq"]) }}

select * from {{ ref('daily_facility_pacing') }}

union

select * from {{ ref('daphne_spothero_lookup') }}