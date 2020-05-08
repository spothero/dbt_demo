  select parking_spot.parking_spot_id as parking_spot_id
        ,spothero_city.timezone as timezone
        ,parking_spot_latlng.location_lat as location_lat
        ,parking_spot_latlng.location_lon as location_lon
        ,parking_spot_latlng.street_address as street_address
        ,parking_spot_latlng.city as city_address
        ,CONVERT_TIMEZONE('UTC', 'America/Chicago', first_on_sales_allowed.first_on_sales_allowed ) as first_on_sales_allowed
        ,parking_spot_rental_facts.first_sale as first_sale
        ,parking_spot_rental_facts.last_sale as last_sale
        ,has_transient_rates.parking_spot_id IS NOT NULL AS has_transient_rates
        ,has_event_rates.parking_spot_id IS NOT NULL AS has_event_rates
        ,has_monthly_rates.parking_spot_id IS NOT NULL AS has_monthly_rates
        ,has_monthly_inventory.parking_spot_id IS NOT NULL AS has_monthly_inventory
        ,p4w_rental_facts.p4w_total_gross_revenue
        ,p4w_rental_facts.p4w_total_gross_revenue_no_event
        ,p4w_rental_facts.p4w_rental_count
        ,p4w_rental_facts_monthly.parking_spot_id IS NOT NULL as p4w_active_location_monthly
        ,p4w_rental_facts_monthly.p4w_rental_count as p4w_rental_count_monthly
        ,p4w_rental_facts_monthly.p4w_total_gross_revenue as p4w_total_gross_revenue_monthly
        ,p90d_rental_facts_monthly.parking_spot_id IS NOT NULL as p90d_active_location_monthly
        ,p90d_rental_facts_monthly.p90d_rental_count as p90d_rental_count_monthly
        ,p90d_rental_facts_monthly.p90d_total_gross_revenue as p90d_total_gross_revenue_monthly
        ,p4w_rental_facts.p4w_new_users
        ,p4w_rental_facts.p4w_new_facility_users
        ,p4w_rental_facts.p4w_repeat_facility_users
        ,1.0*p4w_sipp.p4w_sipp AS p4w_sipp
        ,p4w_rental_facts.parking_spot_id IS NOT NULL AS p4w_active_location
        ,p48w_rental_facts.p48w_total_gross_revenue
        ,p48w_rental_facts.p48w_total_gross_revenue_no_event
        ,p48w_rental_facts.p48w_rental_count
        ,p48w_rental_facts.p48w_new_users
        ,p48w_rental_facts.p48w_new_facility_users
        ,p48w_rental_facts.p48w_repeat_facility_users
        ,p48w_rental_facts.parking_spot_id IS NOT NULL AS p48w_active_location
        ,1.0*p48w_sipp.p48w_sipp AS p48w_sipp
        ,last_year_rental_facts.ly_total_gross_revenue
        ,last_year_rental_facts.ly_rental_count
        ,last_year_rental_facts.ly_new_users
        ,last_year_rental_facts.ly_new_facility_users
        ,last_year_rental_facts.ly_repeat_facility_users
        ,last_year_rental_facts.parking_spot_id IS NOT NULL AS ly_active_location
        ,pw_vs_previous4w.previous4w_avg_rental_count
        ,pw_vs_previous4w.percent_change_pw_previous4w_rental_count
        ,pw_vs_previous4w.pw_rental_count
        ,transient_inventory_status_facts.total_hours_sold_out p1w_total_hours_sold_out
        ,transient_inventory_status_facts.percent_time_sold_out p1w_percent_time_sold_out
        ,transient_inventory_status_facts.did_sell_out p1w_did_sell_out
        ,transient_inventory_status_facts.distinct_days_sold_out p1w_distinct_days_sold_out
        ,transient_inventory_status_facts.avg_total_inventory p1w_avg_total_inventory
        ,transient_inventory_status_facts.avg_weekday_daytime_total_inventory p1w_avg_77_weekday_total_inventory
        ,transient_inventory_status_facts.avg_weekend_total_inventory p1w_avg_weekend_total_inventory
        ,transient_inventory_status_facts.had_inventory_rules_or_changes p1w_had_inventory_rules_or_changes
        ,transient_inventory_status_facts_p8w.p8w_total_hours_sold_out
        ,transient_inventory_status_facts_p8w.p8w_percent_time_sold_out
        ,transient_inventory_status_facts_p8w.p8w_did_sell_out
        ,transient_inventory_status_facts_p8w.p8w_distinct_days_sold_out
        ,transient_inventory_status_facts_p8w.p8w_distinct_weeks_sold_out
        ,transient_inventory_status_facts_p8w.p8w_had_inventory_rules_or_changes
        ,transient_inventory_status_facts_p4w.p4w_total_hours_sold_out
        ,transient_inventory_status_facts_p4w.p4w_percent_time_sold_out
        ,transient_inventory_status_facts_p4w.p4w_did_sell_out
        ,transient_inventory_status_facts_p4w.p4w_distinct_days_sold_out
        ,transient_inventory_status_facts_p4w.p4w_distinct_weeks_sold_out
        ,transient_inventory_status_facts_p4w.p4w_distinct_days_sold_out_weekday
        ,transient_inventory_status_facts_p4w.p4w_distinct_days_sold_out_weekend
        ,transient_inventory_status_facts_p4w.p4w_max_inventory_sold
        ,transient_inventory_status_facts_p4w.p4w_max_inventory_sold_when_sold_out
        ,transient_inventory_status_facts_p4w.p4w_max_inventory_cap
        ,transient_inventory_status_facts_p4w.p4w_had_inventory_rules_or_changes
        ,commuter_stall_availability.est_p1w_avg_commuter_stalls_available
        ,commuter_stall_availability.est_p1w_min_commuter_stalls_available
        ,commuter_stall_availability_p4w.est_p4w_avg_commuter_stalls_available
        ,commuter_stall_availability_p4w.est_p4w_min_commuter_stalls_available
        ,airport_stall_availability.est_p1w_avg_airport_stalls_available
        ,airport_stall_availability.est_p1w_min_airport_stalls_available
        ,weekend_morning_stall_availability.est_p1w_avg_weekend_morning_stalls_available
        ,weekend_morning_stall_availability.est_p1w_min_weekend_morning_stalls_available
        ,weekend_evening_stall_availability.est_p1w_avg_weekend_evening_stalls_available
        ,weekend_evening_stall_availability.est_p1w_min_weekend_evening_stalls_available
        ,hoo_facts.weekday_hoo
        ,hoo_facts.weekend_hoo
        ,star_rating_facts.average_star_rating
        ,star_rating_facts.average_star_rating_p4w
        ,p4w_microclimate_transient_performance.total_microclimate_average AS p4w_microclimate_transient_gross_revenue
        ,p4w_microclimate_transient_performance.upper_microclimate_average AS upper_p4w_microclimate_transient_gross_revenue
        ,p4w_microclimate_transient_performance.upper_mid_microclimate_average AS upper_mid_p4w_microclimate_transient_gross_revenue
        ,p4w_microclimate_transient_performance.lower_mid_microclimate_average AS lower_mid_p4w_microclimate_transient_gross_revenue
        ,p4w_microclimate_transient_performance.lower_microclimate_average AS lower_p4w_microclimate_transient_gross_revenue
        ,p48w_rental_facts_monthly.parking_spot_id IS NOT NULL AS p48w_active_location_monthly
        ,cnp_historical.parking_spot_id IS NOT NULL as was_cnp_facility
        ,parking_spot.spothero_owns_images AS spothero_owns_images
        ,live_enabled_rates.enabled_rates AS live_enabled_rates
  from {{ source('sh_public','parking_spot') }}
  left join {{ source('sh_public','spothero_city') }} ON parking_spot.spothero_city_id = spothero_city.spothero_city_id
  left join {{ ref('pg_parking_spot_latlng') }}  parking_spot_latlng ON parking_spot.parking_spot_id = parking_spot_latlng.parking_spot_id
  left join {{ ref('pg_p4w_rental_facts') }}  p4w_rental_facts ON parking_spot.parking_spot_id = p4w_rental_facts.parking_spot_id
  left join {{ ref('pg_p4w_rental_facts_monthly') }}  p4w_rental_facts_monthly ON parking_spot.parking_spot_id = p4w_rental_facts_monthly.parking_spot_id
  left join {{ ref('pg_p90d_rental_facts_monthly') }}  p90d_rental_facts_monthly ON parking_spot.parking_spot_id = p90d_rental_facts_monthly.parking_spot_id
  left join {{ ref('pg_p4w_sipp') }}  p4w_sipp ON parking_spot.parking_spot_id = p4w_sipp.parking_spot_id
  left join {{ ref('pg_p48w_rental_facts') }}  p48w_rental_facts ON parking_spot.parking_spot_id = p48w_rental_facts.parking_spot_id
  left join {{ ref('pg_p48w_rental_facts_monthly') }}  p48w_rental_facts_monthly ON parking_spot.parking_spot_id = p48w_rental_facts_monthly.parking_spot_id
  left join {{ ref('pg_p48w_sipp') }}  p48w_sipp ON parking_spot.parking_spot_id = p48w_sipp.parking_spot_id
  left join {{ ref('pg_last_year_rental_facts') }}  last_year_rental_facts ON parking_spot.parking_spot_id = last_year_rental_facts.parking_spot_id
  left join {{ ref('pg_has_transient_rates') }}  has_transient_rates ON parking_spot.parking_spot_id = has_transient_rates.parking_spot_id
  left join {{ ref('pg_has_event_rates') }}  has_event_rates ON parking_spot.parking_spot_id = has_event_rates.parking_spot_id
  left join {{ ref('pg_has_monthly_rates') }}  has_monthly_rates ON parking_spot.parking_spot_id = has_monthly_rates.parking_spot_id
  left join {{ ref('pg_has_monthly_inventory') }}  has_monthly_inventory ON parking_spot.parking_spot_id = has_monthly_inventory.parking_spot_id
  left join {{ ref('pg_first_on_sales_allowed') }}  first_on_sales_allowed ON parking_spot.parking_spot_id = first_on_sales_allowed.parking_spot_id
  left join {{ ref('pg_parking_spot_rental_facts') }}  parking_spot_rental_facts ON parking_spot.parking_spot_id = parking_spot_rental_facts.parking_spot_id
  left join {{ ref('pg_pw_vs_previous4w') }}  pw_vs_previous4w ON parking_spot.parking_spot_id = pw_vs_previous4w.parking_spot_id
  left join {{ ref('pg_tis_past_week_facts') }}  transient_inventory_status_facts on parking_spot.parking_spot_id = transient_inventory_status_facts.parking_spot_id
  left join {{ ref('pg_transient_inventory_status_facts_p8w') }}  transient_inventory_status_facts_p8w on parking_spot.parking_spot_id = transient_inventory_status_facts_p8w.parking_spot_id
  left join {{ ref('pg_transient_inventory_status_facts_p4') }}  transient_inventory_status_facts_p4w on parking_spot.parking_spot_id = transient_inventory_status_facts_p4w.parking_spot_id
  left join {{ ref('pg_commuter_stall_availability') }}  commuter_stall_availability on parking_spot.parking_spot_id = commuter_stall_availability.parking_spot_id
  left join {{ ref('pg_commuter_stall_availability_p4w') }}  commuter_stall_availability_p4w on parking_spot.parking_spot_id = commuter_stall_availability_p4w.parking_spot_id
  left join {{ ref('pg_airport_stall_availability') }}  airport_stall_availability on parking_spot.parking_spot_id = airport_stall_availability.parking_spot_id
  left join {{ ref('pg_weekend_morning_stall_availability') }} weekend_morning_stall_availability on parking_spot.parking_spot_id = weekend_morning_stall_availability.parking_spot_id
  left join {{ ref('pg_weekend_evening_stall_availability') }}  weekend_evening_stall_availability on parking_spot.parking_spot_id = weekend_evening_stall_availability.parking_spot_id
  left join {{ ref('pg_hoo_facts') }}  hoo_facts on parking_spot.parking_spot_id = hoo_facts.parking_spot_id
  left join {{ ref('pg_star_rating_facts') }}  star_rating_facts on parking_spot.parking_spot_id = star_rating_facts.parking_spot_id
  left join {{ ref('pg_p4w_microclimate_transient_performance') }}  p4w_microclimate_transient_performance on parking_spot.parking_spot_id = p4w_microclimate_transient_performance.parking_spot_id
  left join {{ ref('pg_cnp_historical') }}  cnp_historical on parking_spot.parking_spot_id = cnp_historical.parking_spot_id
  left join {{ ref('pg_live_enabled_rates') }}  live_enabled_rates on parking_spot.parking_spot_id = live_enabled_rates.parking_spot_id
