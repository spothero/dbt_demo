{{ config(tags=["redshift","spothero_iq"]) }}

select * from {{ source('daphne_public','facility') }}