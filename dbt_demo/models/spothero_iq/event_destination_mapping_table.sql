{{ config(tags=["redshift","spothero_iq"]) }}

select * from {{ ref('pg_destination_geography') }}
union
select * from {{ ref('pg_destination_microclimate') }}
union
select * from {{ ref('pg_parent_destination') }}
union
select * from {{ ref('pg_parent_event') }}
union
select * from {{ source('sfdc','opportunity') }}
union
select * from {{ source('sh_public','destination') }}
union
select * from {{ source('sh_public','event') }}
union
select * from {{ source('sh_public','parking_spot') }}
union
select * from {{ ref('daphne_spothero_lookup') }}