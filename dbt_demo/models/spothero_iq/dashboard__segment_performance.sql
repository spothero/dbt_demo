{{ config(tags=["dashboard","spothero_iq"]) }}

select * from {{ ref('drive_up_product_segments') }}

union

select * from {{ ref('drive_up_rental_segment') }}

union

select * from {{ ref('parking_transactions_redshift') }}