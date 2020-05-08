  SELECT
         DISTINCT object_id_int as parking_spot_id
        FROM {{ source('sh_public','reversion_version') }}
        WHERE content_type_id = 28
        and json_extract_path_text(
            json_extract_array_element_text(serialized_data,0,true),'fields','integration_slugs',true) ilike '%clickandpark%'