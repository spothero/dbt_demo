{{ config(tags=["dashboard","spothero_iq"]) }}

select * from {{ ref('daily_facility_pacing') }}

union

select * from {{ ref('daphne_pacing_facts') }}

union

select * from {{ ref('daphne_spothero_lookup') }}

union

select * from {{ ref('sfdc_account') }}

union

select * from {{ ref('sfdc_contact') }}

union

select * from {{ ref('sfdc_opportunity') }}

union

select * from {{ ref('sfdc_user_ae') }}
