{{ config(tags=["redshift"]) }}


  WITH historical_status AS (
      SELECT
        DATE(parking_spot_historical_status.date) status_date,
        parking_spot_historical_status.facility_id,
        parking_spot_historical_status.status
      FROM {{ ref('pg_parking_spot_historical_status') }} AS parking_spot_historical_status
      WHERE status = 'on_sales_allowed' AND facility_id IN (SELECT DISTINCT parking_spot_id FROM sh_public.rental)
      GROUP BY 1,2,3
    )

    ,date_month_table AS (
      SELECT DATE_TRUNC('month',date_series.date) date_month
      FROM {{ source('spothero_csv','date_series') }} date_series
      WHERE date_series.date >= DATEADD(month,-24,GETDATE()) AND date_series.date <= GETDATE()
    )

    ,event_list AS (
      SELECT
        parent_event.parent_event_id,
        parent_event.starts,
        parent_event.ends,
        destination_microclimate.parking_spot_id,
        date_month_table.date_month,
        destination_microclimate.parking_spot_id||'-'||date_month_table.date_month concat_key,
        MIN(destination_microclimate.distance_miles) distance_miles
      FROM (
        SELECT 
        parent_event_id
        ,event.starts
        ,event.ends
        ,event.destination_id
        ,DATE_TRUNC('month',event.starts) starts_month
        ,DATE_TRUNC('month',event.ends) ends_month
        FROM {{ ref('pg_parent_event') }} parent_event
        INNER JOIN {{ source('sh_public','event') }} event ON event.event_id = parent_event.parent_event_id
        WHERE event.starts >= DATEADD(month,-24,GETDATE()) AND event.starts <= GETDATE()
        GROUP BY 1,2,3,4
      ) parent_event 
      LEFT JOIN date_month_table date_month_table ON date_month_table.date_month BETWEEN parent_event.starts_month AND parent_event.ends_month
      INNER JOIN {{ ref('pg_destination_microclimate') }} destination_microclimate ON destination_microclimate.destination_id = parent_event.destination_id 
      INNER JOIN historical_status ON historical_status.facility_id = destination_microclimate.parking_spot_id AND historical_status.status_date = DATE(parent_event.starts)
      WHERE destination_microclimate.distance_miles <= 2
      GROUP BY 1,2,3,4,5,6
    )

    SELECT
      event_list.parking_spot_id,
      event_list.parent_event_id,
      event_list.distance_miles,
      MAX(transient_inventory_status.price) displayed_event_price,
      MAX(CASE WHEN transient_inventory_status.unavailable_reason = 'blacked out' OR transient_inventory_status.unavailable_reason = 'sold out' OR transient_inventory_status.transient_available < 0 THEN (transient_inventory_status.transient_total - transient_inventory_status.transient_available) ELSE transient_inventory_status.transient_total END) AS max_inventory_total,
      MAX(transient_inventory_status.transient_total - transient_inventory_status.transient_available) AS max_inventory_occupied
    FROM event_list 
    INNER JOIN (
      SELECT 
        spot_id||'-'||DATE_TRUNC('month',starts) concat_key
        ,starts
        ,price
        ,unavailable_reason
        ,transient_available
        ,transient_total
      FROM {{ source('sh_public','spothero_inventoryavailabilitystatus') }} AS transient_inventory_status
      INNER JOIN historical_status ON historical_status.facility_id = transient_inventory_status.spot_id AND historical_status.status_date = DATE(transient_inventory_status.starts)
      WHERE transient_inventory_status.starts >= DATEADD(month,-24,GETDATE()) AND transient_inventory_status.starts <= GETDATE()
      GROUP BY 1,2,3,4,5,6
    ) AS transient_inventory_status ON event_list.concat_key = transient_inventory_status.concat_key AND transient_inventory_status.starts BETWEEN event_list.starts AND event_list.ends
    GROUP BY 1,2,3