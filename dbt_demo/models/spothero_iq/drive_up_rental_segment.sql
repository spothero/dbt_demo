{{ config(tags=["redshift","spothero_iq","looker_pdt"]) }}

	
select * from {{ source('daphne_public','parking_transactions' ) }}

union

select * from {{ source('sh_public','parking_spot' ) }}

union

select * from {{ source('sh_public','spothero_city' ) }}