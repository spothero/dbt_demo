{{ config(tags=["presto","spothero_iq"]) }}

select * from {{ source('sfdc','market_c')}}