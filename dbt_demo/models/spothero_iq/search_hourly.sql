{{ config(tags=["presto","spothero_iq","looker_pdt"]) }}

select * from {{ ref('pg_true_search') }} 

union 

select * from {{ source('daphne_public','parking_transactions') }}