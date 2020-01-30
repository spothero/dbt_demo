{{ config(tags=["presto","spothero_iq","looker_pdt"]) }}

select * from {{ source('daphne_public','parking_transactions') }}
select * from {{ source('sh_public','parking_spot') }}