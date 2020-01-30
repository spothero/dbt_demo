{{ config(tags=["redshift","spothero_iq","looker_pdt"]) }}

select * from {{ source('daphne_public','parking_transactions') }}

union

select * from {{ ref('pg_rental_facts') }}

union

select * from {{ ref('pg_rentals') }}

union

select * from {{ ref('pg_spot_microclimate') }}

union

select * from {{ source('sh_public','parking_spot') }}

union

select * from {{ source('sh_public','spothero_city') }}

union

select * from {{ source('spothero_csv','daphne_fake_budget_test_11_21_2019') }}

union

select * from {{ source('spothero_csv','date_series') }}