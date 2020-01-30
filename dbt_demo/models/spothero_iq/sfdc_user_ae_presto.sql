{{ config(tags=["presto","spothero_iq"]) }}

select * from {{ source('sfdc','user') }}
