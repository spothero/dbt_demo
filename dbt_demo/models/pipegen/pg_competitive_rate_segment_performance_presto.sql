{{ config(tags=["presto"]) }}


    WITH comp_data AS (
      SELECT
        opportunity_microclimate_presto.origin_opportunity_id  AS origin_opportunity_id,
        rate_segment_roll_up_presto.mod_rate_segment  AS rate_segment,
        AVG(CASE WHEN opportunity_microclimate_presto.is_origin ='Target Facility' THEN (
          CASE WHEN (rate_segment_roll_up_presto.mod_rate_segment != '24 Hrs' OR rate_segment_roll_up_presto.mod_rate_segment != 'Weekend (24 hrs)') THEN rental_presto.price
          ELSE (rental_presto.price / NULLIF(rental_presto.rental_length_hours,0))*24 END)
        ELSE NULL END) AS average_price_origin,
        AVG(CASE WHEN NOT opportunity_microclimate_presto.is_origin ='Target Facility' AND opportunity_microclimate_presto.distance_miles <= 0.25 THEN (
          CASE WHEN (rate_segment_roll_up_presto.mod_rate_segment != '24 Hrs' OR rate_segment_roll_up_presto.mod_rate_segment != 'Weekend (24 hrs)') THEN rental_presto.price
          ELSE (rental_presto.price / NULLIF(rental_presto.rental_length_hours,0))*24 END)
        ELSE NULL END) AS "average_price_0_25_mile_comp",
        AVG(CASE WHEN NOT opportunity_microclimate_presto.is_origin ='Target Facility' AND opportunity_microclimate_presto.distance_miles <= 0.5 THEN (
          CASE WHEN (rate_segment_roll_up_presto.mod_rate_segment != '24 Hrs' OR rate_segment_roll_up_presto.mod_rate_segment != 'Weekend (24 hrs)') THEN rental_presto.price
          ELSE (rental_presto.price / NULLIF(rental_presto.rental_length_hours,0))*24 END)
        ELSE NULL END) AS "average_price_0_5_mile_comp",
        AVG(CASE WHEN NOT opportunity_microclimate_presto.is_origin ='Target Facility' AND opportunity_microclimate_presto.distance_miles <= 1.0 THEN (
          CASE WHEN (rate_segment_roll_up_presto.mod_rate_segment != '24 Hrs' OR rate_segment_roll_up_presto.mod_rate_segment != 'Weekend (24 hrs)') THEN rental_presto.price
          ELSE (rental_presto.price / NULLIF(rental_presto.rental_length_hours,0))*24 END)
        ELSE NULL END) AS "average_price_1_mile_comp",
        SUM(CASE WHEN opportunity_microclimate_presto.is_origin ='Target Facility' THEN rental_presto.price ELSE NULL END) AS total_gmv_origin,
        SUM(CASE WHEN NOT opportunity_microclimate_presto.is_origin ='Target Facility' AND opportunity_microclimate_presto.distance_miles <= 0.25 THEN rental_presto.price ELSE NULL END) AS "total_gmv_0_25_mile_comp",
        SUM(CASE WHEN NOT opportunity_microclimate_presto.is_origin ='Target Facility' AND opportunity_microclimate_presto.distance_miles <= 0.50 THEN rental_presto.price ELSE NULL END) AS "total_gmv_0_5_mile_comp",
        SUM(CASE WHEN NOT opportunity_microclimate_presto.is_origin ='Target Facility' AND opportunity_microclimate_presto.distance_miles <= 1.00 THEN rental_presto.price ELSE NULL END) AS "total_gmv_1_mile_comp",
        COUNT(DISTINCT CASE WHEN NOT opportunity_microclimate_presto.is_origin ='Target Facility' AND opportunity_microclimate_presto.distance_miles <= 0.25 THEN rental_presto.parking_spot_id ELSE NULL END) AS "parking_spot_count_0_25_mile",
        COUNT(DISTINCT CASE WHEN NOT opportunity_microclimate_presto.is_origin ='Target Facility' AND opportunity_microclimate_presto.distance_miles <= 0.50 THEN rental_presto.parking_spot_id ELSE NULL END) AS "parking_spot_count_0_5_mile",
        COUNT(DISTINCT CASE WHEN NOT opportunity_microclimate_presto.is_origin ='Target Facility' AND opportunity_microclimate_presto.distance_miles <= 1.0 THEN rental_presto.parking_spot_id ELSE NULL END) AS "parking_spot_count_1_mile"
      FROM /*hive_emr.pipegen.pg_opportunity_microclimate_presto*/ {{ ref('pg_opportunity_microclimate_presto') }} AS opportunity_microclimate_presto
      INNER JOIN {{ source('sfdc', 'opportunity') }} /*redshift.sfdc.opportunity*/ AS comp_sfdc_opportunity_presto ON opportunity_microclimate_presto.comp_opportunity_id = comp_sfdc_opportunity_presto.id
      INNER JOIN (
        SELECT
          rental_presto.rental_id,
          rental_presto.price,
          rental_presto.parking_spot_id,
          rental_facts_presto.is_event_rate_rental,
          rental_facts_presto.rental_segment,
          rental_facts_presto.rental_length_hours
        FROM /*redshift.pipegen.pg_rentals_past_eight_weeks */ {{ ref('pg_rentals_past_eight_weeks') }} rental_presto
        INNER JOIN /*redshift.pipegen.pg_rental_facts */ {{ ref('pg_rental_facts') }} AS rental_facts_presto ON rental_presto.rental_id = rental_facts_presto.rental_id
        WHERE ((((rental_presto.created ) >= ((DATE_ADD('week', -4, DATE_TRUNC('DAY', DATE_ADD('day', (0 - MOD((DAY_OF_WEEK(CAST(NOW() AS DATE)) % 7) - 1 + 7, 7)), CAST(NOW() AS DATE)))))) AND (rental_presto.created ) < ((DATE_ADD('week', 4, DATE_ADD('week', -4, DATE_TRUNC('DAY', DATE_ADD('day', (0 - MOD((DAY_OF_WEEK(CAST(NOW() AS DATE)) % 7) - 1 + 7, 7)), CAST(NOW() AS DATE)))))))))) AND (rental_presto.payment_status = 'success') AND (rental_presto.reservation_status = 'valid') AND (NOT COALESCE(rental_facts_presto.is_event_rate_rental , FALSE)) AND NOT (rental_facts_presto.rental_segment IN ('Monthly Nonrecurrable', 'Monthly Recurrable', 'Event') OR rental_facts_presto.rental_segment IS NULL)
      ) AS rental_presto ON comp_sfdc_opportunity_presto.spot_id_c = CAST(rental_presto.parking_spot_id AS varchar) AND (NOT comp_sfdc_opportunity_presto.is_deleted)
      INNER JOIN 
      	/*hive_emr.pipegen.pg_rate_segment_roll_up_presto */ {{ ref('pg_rate_segment_roll_up_presto') }} AS rate_segment_roll_up_presto ON rental_presto.rental_id = rate_segment_roll_up_presto.rental_id
      WHERE (opportunity_microclimate_presto.distance_miles <= 1.00) OR opportunity_microclimate_presto.origin_opportunity_id = opportunity_microclimate_presto.comp_opportunity_id
      GROUP BY 1,2
    )

    SELECT
      comp_data.*
      ,full_rate_segment.rate_segment full_rate_segment
      ,CASE WHEN "parking_spot_count_0_25_mile" > 3 AND (((COALESCE(average_price_origin,0) - "average_price_0_25_mile_comp")/NULLIF("average_price_0_25_mile_comp",0)) > 0.1) AND COALESCE(total_gmv_origin,0) < "total_gmv_0_25_mile_comp" AND "total_gmv_0_25_mile_comp" >= 500 THEN "average_price_0_25_mile_comp"
        WHEN "parking_spot_count_0_5_mile" > 3 AND (((COALESCE(average_price_origin,0) - "average_price_0_5_mile_comp")/NULLIF("average_price_0_5_mile_comp",0)) > 0.1) AND COALESCE(total_gmv_origin,0) < "total_gmv_0_5_mile_comp" AND "total_gmv_0_5_mile_comp" >= 500 THEN "average_price_0_5_mile_comp"
        WHEN "parking_spot_count_1_mile" > 3 AND (((COALESCE(average_price_origin,0) - "average_price_1_mile_comp")/NULLIF("average_price_1_mile_comp",0)) > 0.1) AND COALESCE(total_gmv_origin,0) < "total_gmv_1_mile_comp" AND "total_gmv_1_mile_comp" >= 500 THEN "average_price_1_mile_comp"
      ELSE COALESCE(average_price_origin,"average_price_0_25_mile_comp","average_price_0_5_mile_comp","average_price_1_mile_comp",0) END AS price
      ,CASE WHEN "parking_spot_count_0_25_mile" > 3 THEN '0.25-mile'
        WHEN "parking_spot_count_0_5_mile" > 3 THEN '0.5-mile'
        WHEN "parking_spot_count_1_mile" > 3 THEN '1-mile'
      ELSE 'NONE' END AS selected_distance
    FROM comp_data
    LEFT JOIN (
      SELECT '6 Hrs' AS rate_segment UNION ALL
      SELECT 'Commuter' AS rate_segment UNION ALL
      SELECT 'Night (6 Hrs)' AS rate_segment UNION ALL
      SELECT 'Weekend (24 hrs)' AS rate_segment UNION ALL
      SELECT '12 Hrs' AS rate_segment UNION ALL
      SELECT 'Weekend (12 Hrs)' AS rate_segment UNION ALL
      SELECT 'Overnight (12 Hrs)' AS rate_segment UNION ALL
      SELECT '24 Hrs' AS rate_segment UNION ALL
      SELECT '3 Hrs' AS rate_segment
    ) AS full_rate_segment ON comp_data.rate_segment = full_rate_segment.rate_segment