{{ config(tags=["redshift","spothero_iq","looker_pdt"]) }}

select * from {{ source('daphne_public','parking_transactions') }}