with transaction_fee_summary as (SELECT t.rental_id, item_type, 
      sum(case when item_type = 'reservation_fee' then base_amount else null end) over (partition by t.rental_id) reservation_fee_base,
      sum(case when item_type = 'blanket_fee' then base_amount else null end) over (partition by t.rental_id) blanket_fee_base,
      sum(case when item_type = 'airport_tax' then base_amount else null end) over (partition by t.rental_id) airport_tax_base,
      sum(case when item_type = 'airport_fee' then base_amount else null end) over (partition by t.rental_id) airport_fee_base,
      sum(case when item_type = 'rental' then base_amount else null end) over (partition by t.rental_id) rental_item_base,
      sum(case when item_type = 'oversize_fee' then base_amount else null end) over (partition by t.rental_id) oversize_fee_base,
      sum(case when item_type = 'reservation_fee' then tri.remit_amount else null end) over (partition by t.rental_id) reservation_fee_remit,
      sum(case when item_type = 'airport_tax' then tri.remit_amount else null end) over (partition by t.rental_id) airport_tax_remit,
      sum(case when item_type = 'airport_fee' then tri.remit_amount else null end) over (partition by t.rental_id) airport_fee_remit,
      sum(case when item_type = 'rental' then tri.remit_amount else null end) over (partition by t.rental_id) rental_item_remit,
      sum(case when item_type = 'oversize_fee' then tri.remit_amount else null end) over (partition by t.rental_id) oversize_fee_remit
      FROM {{ source('sh_public', 'transaction_remit_item') }} tri 
      LEFT JOIN {{ source('sh_public', 'transaction') }} t on tri.transaction_id = t.transaction_id
      LEFT JOIN {{ source('sh_public', 'rental') }} as rental on t.rental_id = rental.rental_id
      WHERE rental_rule_type_title != 'monthly')

SELECT transaction_fee_summary.*,
COALESCE(reservation_fee_base, 0) + COALESCE(blanket_fee_base,0) + COALESCE(airport_tax_base,0) + COALESCE(airport_fee_base,0) + COALESCE(rental_item_base,0) + COALESCE(oversize_fee_base,0) as checkout_price,
COALESCE(reservation_fee_base, 0) + COALESCE(airport_tax_base,0) + COALESCE(airport_fee_base,0) + COALESCE(rental_item_base,0) + COALESCE(oversize_fee_base,0) as gmv_true,
COALESCE(reservation_fee_remit, 0) + COALESCE(airport_tax_remit,0) + COALESCE(airport_fee_remit,0) + COALESCE(rental_item_remit,0) + COALESCE(oversize_fee_remit,0) as full_remit

FROM transaction_fee_summary
where item_type = 'rental'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16