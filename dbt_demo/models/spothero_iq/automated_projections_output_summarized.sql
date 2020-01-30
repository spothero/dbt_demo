{{ config(tags=["presto","spothero_iq"]) }}

select * from {{ ref('pg_automated_projections_market_growth') }}