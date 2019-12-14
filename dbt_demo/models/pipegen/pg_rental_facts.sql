WITH renter_latest_valid_facts AS (
          SELECT
            rental.renter_id,
            rental.star_rating AS latest_star_rating,
            rental.city AS latest_city,
            rental.rental_device_segment AS latest_device_segment,
            rental.rental_segment_rollup AS latest_segment,
            rental.stripe_card_type AS latest_stripe_card_type,
            rental.profile_type AS latest_profile_type,
            rental.neighborhood AS latest_neighborhood,
            rental.rental_created AS latest_rental_created,
            DATEDIFF(day, rental.rental_created, CURRENT_DATE) AS latest_days_since,
            parking_spot.title AS latest_parking_spot
          FROM pipegen.pg_rental_facts AS rental
          JOIN {{ source('sh_public','parking_spot') }} on parking_spot.parking_spot_id = rental.parking_spot_id
          JOIN (
            SELECT rental_id
            FROM (
              SELECT
                rental_id,
                ROW_NUMBER() OVER (
                  PARTITION BY renter_id
                  ORDER BY rental_sequence_number DESC
                ) AS rental_row_number
              FROM pipegen.pg_rental_facts
              WHERE rental_sequence_number IS NOT NULL
            ) AS rentals_sequenced
          WHERE rentals_sequenced.rental_row_number = 1
          ) AS latest ON latest.rental_id = rental.rental_id
        ),

        rentals_ranked_by_start AS (
          SELECT
            rental.rental_id,
            rental.renter_id,
            rental.starts,
            ROW_NUMBER() OVER(
              PARTITION BY renter_id
              ORDER BY rental.starts ASC
            ) AS rank
          FROM {{ ref('pg_rentals') }} AS rental
          WHERE rental.starts > CURRENT_DATE
            AND reservation_status IN ('valid', 'recurrence')
            AND (rental.partner_id != 45 OR rental.partner_id IS NULL)
        ),

        renter_next_facts AS (
          SELECT
            rental.renter_id,
            rental.starts AS next_rental_starts,
            rental.created AS next_rental_created,
            parking_spot.title AS next_parking_spot,
            spothero_city.city AS next_city,
            rental_facts.neighborhood as next_rental_neighborhood,
            rental.ends AS next_rental_ends,
            rental_facts.rental_device_segment AS next_device_segment,
            rental_facts.rental_segment_rollup AS next_segment,
            DATEDIFF(hour, CURRENT_DATE, rental.starts) AS hours_until_next_starts
          FROM {{ ref('pg_rentals') }} AS rental
          LEFT JOIN pipegen.pg_rental_facts rental_facts ON rental_facts.rental_id = rental.rental_id
          LEFT JOIN rentals_ranked_by_start as rank ON rank.rental_id = rental.rental_id
          LEFT JOIN {{ source('sh_public','parking_spot') }} parking_spot ON parking_spot.parking_spot_id = rental.parking_spot_id
          LEFT JOIN {{ source('sh_public','spothero_city') }} spothero_city ON spothero_city.spothero_city_id = parking_spot.spothero_city_id
          WHERE rank.rank = 1
            AND (rental.partner_id != 45 OR rental.partner_id IS NULL)
            AND rental.renter_id IS NOT NULL
        ),

        renter_core_neighborhood AS (
          SELECT
            renter_id,
            core_feature AS core_neighborhood,
            core_count AS core_neighborhood_count
          FROM (
            SELECT
              renter_id,
              core_feature,
              core_count,
              ROW_NUMBER() OVER (
                PARTITION BY renter_id
                ORDER BY core_count DESC, first_core_feature_date ASC
              ) as core_rank
            FROM (
              SELECT
                rental.renter_id,
                rental.neighborhood AS core_feature,
                COUNT(rental.rental_id) AS core_count,
                MIN(rental.rental_created) AS first_core_feature_date
              FROM pipegen.pg_rental_facts AS rental
              WHERE
                rental.renter_id is not null
                AND rental.is_good_rental
                AND rental.rental_sequence_number IS NOT NULL
                AND rental.neighborhood IS NOT NULL
              GROUP BY 1,2
            ) tmp
          ) ranked_core_neighborhood
          WHERE core_rank = 1
        ),

        renter_last_ends_facts AS (
            SELECT DISTINCT
            rental.renter_id,
            LAST_VALUE(parking_spot.title) OVER (
            PARTITION BY rental.renter_id ORDER BY rental.ends
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS latest_ends_title,
            LAST_VALUE(spothero_city.city) OVER (
            PARTITION BY rental.renter_id ORDER BY rental.ends
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS latest_ends_city,
            LAST_VALUE(djstripe_card.stripe_card_type) OVER (
            PARTITION BY rental.renter_id ORDER BY rental.ends
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS latest_ends_stripe_card_type,
            LAST_VALUE(rental.starts) OVER (
            PARTITION BY rental.renter_id ORDER BY rental.ends
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS latest_ends_starts,
            LAST_VALUE(rental_facts.neighborhood) OVER (
            PARTITION BY rental.renter_id ORDER BY rental.ends
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS last_ends_neighborhood,
            LAST_VALUE(rental_facts.rental_segment_rollup) OVER (
            PARTITION BY rental.renter_id ORDER BY rental.ends
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS last_ends_segment,
            LAST_VALUE(rental.rental_source_title) OVER (
            PARTITION BY rental.renter_id ORDER BY rental.ends
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS last_ends_rental_device,
            LAST_VALUE(rental.ends) OVER (
            PARTITION BY rental.renter_id ORDER BY rental.ends
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
          ) AS last_ends_rental_date
          FROM {{ ref('pg_rentals') }} AS rental
          LEFT JOIN pipegen.pg_rental_facts rental_facts ON rental_facts.rental_id = rental.rental_id
          LEFT JOIN {{ source('sh_public','parking_spot') }} ON parking_spot.parking_spot_id = rental.parking_spot_id
          LEFT JOIN {{ source('sh_public','spothero_city') }} ON spothero_city.spothero_city_id = parking_spot.spothero_city_id
          LEFT JOIN (
            SELECT
            rental_id,
            MAX(card_id) AS card_id
            FROM {{ source('sh_public','djstripe_charge') }}
            GROUP BY 1
          ) djstripe_charge ON djstripe_charge.rental_id = rental.rental_id
          LEFT JOIN {{ source('sh_public','djstripe_card') }}
          ON djstripe_card.id = djstripe_charge.card_id
          WHERE
            rental.ends < CURRENT_DATE
            AND reservation_status IN ('valid','recurrence')
        ),

        renter_core_payment_method AS (
          SELECT renter_id, core_payment_method, core_count as core_payment_method_rental_count
          FROM (
            SELECT
            renter_id,
            core_payment_method,
            core_count,
            ROW_NUMBER() OVER (
              PARTITION BY renter_id
              ORDER BY core_count DESC, first_core_feature_date ASC
            ) as core_rank
            FROM (
              SELECT
                rental.renter_id, djstripe_card.stripe_card_type as core_payment_method,
                COUNT(rental.rental_id) as core_count,
                MIN(rental.created) as first_core_feature_date
              FROM {{ ref('pg_rentals') }} as rental
              INNER JOIN pipegen.pg_rental_facts AS rental_facts ON rental_facts.rental_id = rental.rental_id
              INNER JOIN (
                SELECT rental_id, max(card_id) as card_id
                FROM {{ source('sh_public','djstripe_charge') }}
                GROUP BY 1
              ) djstripe_charge on djstripe_charge.rental_id = rental.rental_id
              INNER JOIN {{ source('sh_public','djstripe_card') }} on djstripe_charge.card_id = djstripe_card.id
              WHERE
                rental.renter_id is not null
                AND rental_facts.is_good_rental
                AND (rental.partner_id != 45 OR rental.partner_id IS NULL)
              GROUP BY 1,2
            ) tmp
          ) ranked_core_payment
          WHERE core_rank = 1
        ),

        renter_core_segment as (
          SELECT renter_id, core_feature as core_segment, core_count as core_segment_rental_count
          FROM (
            SELECT
            renter_id, core_feature, core_count,
            ROW_NUMBER() OVER(
              PARTITION BY renter_id
              ORDER BY core_count DESC, first_core_feature_date ASC
            ) as core_rank
            FROM (
              SELECT
                rental.renter_id, rental_facts.rental_segment_rollup as core_feature,
                COUNT(rental.rental_id) as core_count,
                MIN(rental.created) as first_core_feature_date
              FROM {{ ref('pg_rentals') }} as rental
              INNER JOIN pipegen.pg_rental_facts as rental_facts on rental_facts.rental_id = rental.rental_id
              WHERE rental.renter_id is not null and rental_facts.is_good_rental and (rental.partner_id != 45 OR rental.partner_id IS NULL)
              GROUP BY 1,2
            ) tmp
          ) ranked_core_segment
          WHERE core_rank = 1
        ),

        renter_current_credit_balance as (
          SELECT a.user_id as renter_id, COALESCE((COALESCE(total_credit,0) - COALESCE(consumed_credit,0)),0) as current_credit_balance
          FROM (
            SELECT user_id, SUM(amount) AS total_credit
             from {{ source('sh_public','spothero_credithistory') }}
             WHERE history_type in (
               'purchase-as-spothero',
               'purchase-less-than-50c',
               'refund-as-credit',
               'refund',
               'referral-friend-purchased',
               'referral-initial',
               'manual',
               'promocode',
               'auto-employee-benefit',
               'referral-initial-pending'
              )
             AND amount > 0
             GROUP BY 1
          ) as a
          LEFT JOIN (
            SELECT user_id, COALESCE(-SUM(COALESCE(amount,0)),0) AS consumed_credit
            from {{ source('sh_public','spothero_credithistory') }}
            WHERE consumed_credit_history_id IS NOT NULL
            GROUP BY 1
          ) as b on b.user_id = a.user_id
        ),

        renter_pretax_facts as (
          SELECT rental.renter_id,
                 MIN(case when djstripe_commutercardinfo.administrator_id is not null THEN rental.created else null end) as first_pretax_created,
                 COUNT(case when djstripe_commutercardinfo.administrator_id is not null THEN rental.rental_id else null end) as lifetime_pretax_rentals
          from {{ ref('pg_rentals') }} as rental
          LEFT JOIN (SELECT rental_id, max(card_id) as card_id from {{ source('sh_public','djstripe_charge') }} GROUP BY 1) djstripe_charge on djstripe_charge.rental_id = rental.rental_id
          LEFT JOIN {{ source('sh_public','djstripe_card') }} on djstripe_card.id = djstripe_charge.card_id
          LEFT JOIN {{ source('sh_public','djstripe_commutercardinfo') }} on djstripe_commutercardinfo.id = djstripe_card.commuter_info_id
          INNER JOIN pipegen.pg_rental_facts as rental_facts on rental_facts.rental_id = rental.rental_id
          WHERE rental.renter_id is not null and rental_facts.is_good_rental and (rental.partner_id != 45 OR rental.partner_id IS NULL)
          GROUP BY 1
        ),

        renter_referral_count as (
          SELECT
            referred_by_id as renter_id,
            COUNT(distinct referee_id) as renter_customers_referred
          FROM {{ source('sh_public','referrals_referral') }}
          GROUP BY 1
        ),

        renter_powerbookings as (
          SELECT
            rental.renter_id,
            true as powerbooking_used,
            MAX(rental.created) as powerbooking_most_recent
          FROM {{ ref('pg_rentals') }} as rental
          INNER JOIN pipegen.pg_rental_facts as rental_facts
            ON rental_facts.rental_id = rental.rental_id
          INNER JOIN {{ source('sh_public','spothero_rentalbulkpurchased') }} as bulk
            ON bulk.rental_id = rental.rental_id
          WHERE rental_facts.rental_sequence_number IS NOT null
          GROUP BY 1
        ),

        renter_card_count as (
          SELECT
            rental.renter_id,
            count(distinct card_id) as card_count
          FROM {{ ref('pg_rentals') }} as rental
          INNER JOIN pipegen.pg_rental_facts as rental_facts
            ON rental_facts.rental_id = rental.rental_id
          INNER JOIN {{ source('sh_public','djstripe_charge') }} as charge
            ON charge.rental_id = rental.rental_id
          INNER JOIN {{ source('sh_public','djstripe_card') }} as card
            ON charge.card_id = card.id
          WHERE not card.deleted and rental_facts.rental_sequence_number IS NOT null
          GROUP BY 1
        )

        SELECT
          spothero_user.id as renter_id,
          renter_latest_valid_facts.latest_star_rating,
          renter_latest_valid_facts.latest_city,
          renter_latest_valid_facts.latest_device_segment,
          renter_latest_valid_facts.latest_segment,
          renter_latest_valid_facts.latest_stripe_card_type,
          renter_latest_valid_facts.latest_profile_type,
          renter_latest_valid_facts.latest_neighborhood,
          renter_latest_valid_facts.latest_rental_created,
          renter_latest_valid_facts.latest_days_since,
          renter_latest_valid_facts.latest_parking_spot,
          renter_next_facts.next_rental_created,
          renter_next_facts.next_rental_starts,
          renter_next_facts.next_rental_ends,
          renter_next_facts.next_parking_spot,
          renter_next_facts.next_city,
          renter_next_facts.next_rental_neighborhood,
          renter_next_facts.next_device_segment,
          renter_next_facts.next_segment,
          renter_next_facts.hours_until_next_starts,
          renter_last_ends_facts.latest_ends_title,
          renter_last_ends_facts.latest_ends_city,
          renter_last_ends_facts.latest_ends_stripe_card_type,
          renter_last_ends_facts.latest_ends_starts,
          renter_last_ends_facts.last_ends_neighborhood,
          renter_last_ends_facts.last_ends_segment,
          renter_last_ends_facts.last_ends_rental_device,
          renter_last_ends_facts.last_ends_rental_date,
          renter_core_neighborhood.core_neighborhood,
          renter_core_neighborhood.core_neighborhood_count,
          renter_core_payment_method.core_payment_method,
          renter_core_payment_method.core_payment_method_rental_count,
          renter_core_segment.core_segment,
          renter_core_segment.core_segment_rental_count,
          renter_current_credit_balance.current_credit_balance,
          renter_pretax_facts.first_pretax_created,
          renter_pretax_facts.lifetime_pretax_rentals,
          renter_referral_count.renter_customers_referred,
          COALESCE(renter_powerbookings.powerbooking_used, false) as powerbooking_used,
          renter_powerbookings.powerbooking_most_recent as powerbooking_most_recent,
          COALESCE(renter_card_count.card_count, 0) as card_count
        FROM {{ source('sh_public','spothero_user') }}
        LEFT JOIN renter_latest_valid_facts ON renter_latest_valid_facts.renter_id = spothero_user.id
        LEFT JOIN renter_next_facts ON renter_next_facts.renter_id = spothero_user.id
        LEFT JOIN renter_core_neighborhood ON renter_core_neighborhood.renter_id = spothero_user.id
        LEFT JOIN renter_last_ends_facts ON renter_last_ends_facts.renter_id = spothero_user.id
        LEFT JOIN renter_core_payment_method ON renter_core_payment_method.renter_id = spothero_user.id
        LEFT JOIN renter_core_segment ON renter_core_segment.renter_id = spothero_user.id
        LEFT JOIN renter_current_credit_balance ON renter_current_credit_balance.renter_id = spothero_user.id
        LEFT JOIN renter_pretax_facts ON renter_pretax_facts.renter_id = spothero_user.id
        LEFT JOIN renter_referral_count ON renter_referral_count.renter_id = spothero_user.id
        LEFT JOIN renter_powerbookings ON renter_powerbookings.renter_id = spothero_user.id
        LEFT JOIN renter_card_count ON renter_card_count.renter_id = spothero_user.id