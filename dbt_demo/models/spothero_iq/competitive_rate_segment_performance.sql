{{ config(tags=["presto","spothero_iq"]) }}

select * from {{ ref('pg_competitive_rate_segment_performance_presto') }}