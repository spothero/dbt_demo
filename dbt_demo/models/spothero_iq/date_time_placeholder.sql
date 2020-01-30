{{ config(tags=["redshift","spothero_iq","looker_pdt"]) }}

select * from {{ source('spothero_csv','date_series') }}

union

select * from {{ ref('daphne_spothero_lookup') }}
