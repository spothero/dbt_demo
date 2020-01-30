{{ config(tags=["redshift","spothero_iq"]) }}

select * from {{ source('sh_public','tiered_event_rate') }}