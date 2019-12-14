SELECT 
  rental.*
  ,spothero_city.timezone 
FROM 
	{{ source('sh_public','rental') }} AS rental
INNER JOIN 
	{{ source('sh_public','parking_spot') }} as parking_spot on parking_spot.parking_spot_id = rental.parking_spot_id
INNER JOIN 
	{{ source('sh_public','spothero_city') }} as spothero_city on spothero_city.spothero_city_id = parking_spot.spothero_city_id
WHERE (((rental.created) >= ((DATEADD(week,-8, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ))) AND (rental.created ) < ((DATEADD(week,8, DATEADD(week,-8, DATE_TRUNC('week', DATE_TRUNC('day',GETDATE())) ) )))))