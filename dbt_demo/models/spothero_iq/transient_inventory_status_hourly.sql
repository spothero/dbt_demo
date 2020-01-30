{{ config(tags=["redshift","spothero_iq"]) }}

select * from {{ source('sh_public','spothero_inventoryavailabilitystatus') }}