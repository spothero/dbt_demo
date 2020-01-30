{{ config(tags=["redshift","spothero_iq","looker_pdt"]) }}


select * from {{ ref('pg_parent_event') }}
union
select * from {{ source('sh_public','event_event_types') }}
union
select * from {{ source('sh_public','spothero_eventtype') }}

