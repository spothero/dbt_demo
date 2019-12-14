SELECT
     rental.rental_id
     ,rental.parking_spot_id
     ,starts
     ,ends
     ,renter_id
     ,CASE WHEN spothero_city.currency_type = 'cad' THEN rental.price/currency_exchange_rate.cad_exchange_rate ELSE price END AS checkout_price
     ,rental.price AS local_checkout_price
     ,CASE WHEN blanket_fee_base IS NOT NULL THEN gmv_true ELSE price END as price
     ,rental.created
     ,rental_source_title
     ,payment_type_title
     ,rule_id
     ,rental_rule_type_title
     ,qrcode_uuid
     ,event_id
     ,checked_in
     ,rental.last_updated
     ,CASE WHEN spothero_city.currency_type = 'cad' THEN rental.remit_amount/currency_exchange_rate.cad_exchange_rate ELSE rental.remit_amount END AS remit_amount
     ,rental.remit_amount as local_remit_amount
     ,reconciled
     ,rpr_id
     ,parking_spot_alias_id
     ,rental.status
     ,rental.phone_number
     ,entry_timestamp
     ,exit_timestamp
     ,barcode_content
     ,CASE WHEN spothero_city.currency_type = 'cad' THEN rental.payment_processor_fee/currency_exchange_rate.cad_exchange_rate ELSE rental.payment_processor_fee END AS payment_processor_fee
     ,rental.payment_processor_fee as local_payment_processor_fee
     ,dea_migrated
     ,subscription_id
     ,CASE WHEN spothero_city.currency_type = 'cad' THEN rental.spothero_credit_used/currency_exchange_rate.cad_exchange_rate ELSE rental.spothero_credit_used END AS spothero_credit_used
     ,rental.spothero_credit_used as local_spothero_credit_used
     ,cancellation_source
     ,discount_promocode_redemption_id
     ,sms_parking_pass
     ,short_url
     ,refund_timestamp
     ,cancellation_reason_for_operator
     ,rental_source_application
     ,rental_source_device_category
     ,rental_source_operating_system
     ,display_id
     ,reservation_status
     ,payment_status
     ,license_plate_unknown
     ,rental.remit_scheme_id
     ,affiliate_id
     ,vendor_rental_id
     ,CASE WHEN spothero_city.currency_type = 'cad' THEN rental.discount_amount/currency_exchange_rate.cad_exchange_rate ELSE rental.discount_amount END AS discount_amount
     ,rental.discount_amount as discount_amount_local
     ,cancellation_reason
     ,rule_trail
     ,remit_trail
     ,version
     ,search_id
     ,vehicle_info_id
     ,license_plate
     ,cancellation_reason_category
     ,action_id
     ,test_reservation
     ,sent_to_desk
     ,referral_source
     ,CASE WHEN spothero_city.currency_type = 'cad' THEN rental.facility_fee_amount/currency_exchange_rate.cad_exchange_rate ELSE rental.facility_fee_amount END AS facility_fee_amount
     ,rental.facility_fee_amount as local_facility_fee_amount
     ,facility_fee_id
     ,partner_id
     ,profile_id
     ,rental_source_referrer
     ,memo
     ,taxes_visible_to_operator
     ,vehicle_color
     ,license_plate_state
     ,null as subscription_status
     ,null as cancellation_created
     ,rental.rental_id as original_rental_id
     FROM sh_public.rental
     LEFT JOIN sh_public.parking_spot AS parking_spot ON rental.parking_spot_id = parking_spot.parking_spot_id 
     LEFT JOIN sh_public.spothero_city AS spothero_city ON parking_spot.spothero_city_id = spothero_city.spothero_city_id
     LEFT JOIN pipegen.pg_currency_exchange_rate AS currency_exchange_rate ON DATE(rental.created) = currency_exchange_rate.day
     LEFT JOIN pipegen.pg_transaction_fee_summary_gmv AS summary_gmv ON rental.rental_id = summary_gmv.rental_id 
     WHERE subscription_id IS NULL 
     UNION ALL
     (
     SELECT
     CASE WHEN rr.monthly_sequence_number = 1 THEN rr.rental_id ELSE rr.id+1000000000 END as rental_id
     ,r.parking_spot_id as parking_spot_id
     ,rr.starts as starts
     ,rr.ends as ends
     ,r.renter_id as renter_id
     ,CASE WHEN spothero_city.currency_type = 'cad' THEN rr.price/currency_exchange_rate.cad_exchange_rate ELSE rr.price END AS checkout_price
     ,rr.price as local_checkout_price
     ,CASE WHEN blanket_fee_base IS NOT NULL THEN gmv_true ELSE rr.price END as price
     ,rr.created as created
     ,r.rental_source_title as rental_source_title
     ,r.payment_type_title as payment_type_title
     ,r.rule_id as rule_id
     ,'monthly' as rental_rule_type_title
     ,r.qrcode_uuid as qrcode_uuid
     ,null as event_id
     ,r.checked_in as checked_in
     ,rr.last_updated as last_updated
     ,CASE WHEN spothero_city.currency_type = 'cad' THEN rr.remit_amount/currency_exchange_rate.cad_exchange_rate ELSE rr.remit_amount END AS remit_amount
     ,rr.remit_amount as local_remit_amount
     ,r.reconciled as reconciled
     ,r.rpr_id as rpr_id
     ,r.parking_spot_alias_id as parking_spot_alias_id
     ,null as status
     ,r.phone_number as phone_number
     ,r.entry_timestamp as entry_timestamp
     ,r.exit_timestamp as exit_timestamp
     ,r.barcode_content as barcode_content
     ,CASE WHEN spothero_city.currency_type = 'cad' THEN rr.payment_processor_fee/currency_exchange_rate.cad_exchange_rate ELSE rr.payment_processor_fee END AS payment_processor_fee
     ,rr.payment_processor_fee as local_payment_processor_fee
     ,r.dea_migrated as dea_migrated
     ,r.subscription_id as subscription_id
     ,CASE WHEN spothero_city.currency_type = 'cad' THEN r.spothero_credit_used/currency_exchange_rate.cad_exchange_rate ELSE r.spothero_credit_used END AS spothero_credit_used
     ,r.spothero_credit_used as local_spothero_credit_used
     ,CASE WHEN rr.recurrence_status = 'cancelled' THEN r.cancellation_source ELSE NULL END AS cancellation_source
     ,r.discount_promocode_redemption_id as discount_promocode_redemption_id
     ,r.sms_parking_pass as sms_parking_pass
     ,r.short_url as short_url
     ,CASE WHEN rr.recurrence_status = 'cancelled' THEN r.refund_timestamp ELSE NULL END AS refund_timestamp
     ,CASE WHEN rr.recurrence_status = 'cancelled' THEN rr.cancellation_reason_for_operator_real ELSE NULL END AS cancellation_reason_for_operator
     ,r.rental_source_application as rental_source_application
     ,r.rental_source_device_category as rental_source_device_category
     ,r.rental_source_operating_system as rental_source_operating_system
     ,r.display_id as display_id
     ,CASE WHEN rr.monthly_sequence_number = 1 THEN rr.recurrence_status WHEN rr.recurrence_status = 'cancelled' THEN 'cancelled' ELSE 'recurrence' END as reservation_status
     ,rr.payment_status as payment_status
     ,r.license_plate_unknown as license_plate_unknown
     ,r.remit_scheme_id as remit_scheme_id
     ,r.affiliate_id as affiliate_id
     ,r.vendor_rental_id as vendor_rental_id
     ,CASE WHEN spothero_city.currency_type = 'cad' THEN r.discount_amount/currency_exchange_rate.cad_exchange_rate ELSE r.discount_amount END AS discount_amount
     ,r.discount_amount as local_discount_amount
     ,CASE WHEN rr.recurrence_status = 'cancelled' THEN rr.cancellation_reason_real ELSE NULL END AS cancellation_reason
     ,r.rule_trail as rule_trail
     ,r.remit_trail as remit_trail
     ,r.version as version
     ,r.search_id as search_id
     ,r.vehicle_info_id as vehicle_info_id
     ,r.license_plate as license_plate
     ,CASE WHEN rr.recurrence_status = 'cancelled' THEN rr.cancellation_reason_category_real ELSE NULL END AS cancellation_reason_category
     ,r.action_id as action_id
     ,r.test_reservation as test_reservation
     ,r.sent_to_desk as sent_to_desk
     ,r.referral_source as referral_source
     ,CASE WHEN spothero_city.currency_type = 'cad' THEN r.facility_fee_amount/currency_exchange_rate.cad_exchange_rate ELSE r.facility_fee_amount END AS facility_fee_amount
     ,r.facility_fee_amount as local_facility_fee_amount
     ,r.facility_fee_id as facility_fee_id
     ,r.partner_id as partner_id
     ,r.profile_id as profile_id
     ,r.rental_source_referrer as rental_source_referrer
     ,r.memo as memo
     ,rr.taxes_visible_to_operator as taxes_visible_to_operator
     ,r.vehicle_color as vehicle_color
     ,r.license_plate_state as license_plate_state
     ,r.reservation_status as subscription_status
     ,rr.cancellation_created_real as cancellation_created
     ,rr.rental_id as original_rental_id
FROM (
     SELECT 
      rental_recurrence.*
      , rental_recurrence_cancellation.created as cancellation_created_real
      , rental_recurrence_cancellation.reason as cancellation_reason_real
      , rental_recurrence_cancellation.reason_for_operator as cancellation_reason_for_operator_real
      , rental_recurrence_cancellation.reason_category as cancellation_reason_category_real
      , ROW_NUMBER() OVER(PARTITION BY rental_id ORDER BY starts) as monthly_sequence_number 
    FROM {{ source('sh_public', 'rental_recurrence') }} 
    left join {{ source('sh_public', 'rental_recurrence_cancellation') }} 
      on rental_recurrence_cancellation.rental_recurrence_id = rental_recurrence.id ) rr
INNER JOIN
{{ source('sh_public', 'rental') }} r on r.rental_id = rr.rental_id
LEFT JOIN {{ source('sh_public', 'parking_spot') }} AS parking_spot ON r.parking_spot_id = parking_spot.parking_spot_id 
LEFT JOIN {{ source('sh_public', 'spothero_city') }} AS spothero_city ON parking_spot.spothero_city_id = spothero_city.spothero_city_id
LEFT JOIN {{ ref('pg_currency_exchange_rate') }} AS currency_exchange_rate ON DATE(rr.created) = currency_exchange_rate.day
LEFT JOIN {{ ref('pg_transaction_fee_summary_gmv') }} AS summary_gmv ON r.rental_id = summary_gmv.rental_id 
)