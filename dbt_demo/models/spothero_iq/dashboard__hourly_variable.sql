{{ config(tags=["dashboard","spothero_iq"]) }}

select * from {{ ref('hourly_variable') }}