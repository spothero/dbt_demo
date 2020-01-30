{{ config(tags=["redshift","spothero_iq"]) }}

select * from {{ source('sfdc','contact') }}