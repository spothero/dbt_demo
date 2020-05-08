    SELECT a.parking_spot_id
          ,a.location_lat
          ,a.location_lon
          ,a.street_address
          ,a.city
    FROM {{ source('sh_public','spothero_spotaddress') }} a
    INNER JOIN (SELECT id
                      ,row_number() over(partition by parking_spot_id ORDER BY id) as row
                FROM {{ source('sh_public','spothero_spotaddress') }} rank) b
      ON a.id = b.id AND b.row = 1