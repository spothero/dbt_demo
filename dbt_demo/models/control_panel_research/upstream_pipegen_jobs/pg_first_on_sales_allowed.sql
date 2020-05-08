  SELECT parking_spot_id, min(date_created) as first_on_sales_allowed
  FROM (SELECT rr.date_created
  ,rv.revision_id
  ,object_id_int as parking_spot_id
  ,json_extract_path_text(json_extract_array_element_text(serialized_data,0),'fields','status') as status
  FROM {{ source('sh_public','reversion_version') }} as rv
  INNER JOIN {{ source('sh_public','reversion_revision') }} as rr on rr.id = rv.revision_id
  LEFT join {{ source('sh_public','parking_spot') }} as ps on ps.parking_spot_id = rv.object_id_int
  WHERE content_type_id = 28
  AND ps.actual_spot_id is null
  ) as foo
  WHERE status = 'on_sales_allowed'
  GROUP BY 1