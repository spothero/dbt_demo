{{ config(tags=["redshift","spothero_iq","looker_pdt"]) }}

select * from {{ ref('daily_facility_pacing') }}