### Purpose of Demo

This demo is meant to demonstrate [dbt's](https://docs.getdbt.com/docs) functionality for the following ETL tooling use cases:

- Job dependency managment
- Table-level data lineage
- Column-level documentation
- Column-level testing functionality

Regardless of if we adopt dbt within our stack, this work should clearly demonstrate the the value of this ETL functionality with a subset of existing Pipegen jobs.

To narrow the focus of this work, we are considering the [top 10 most-queried Pipegen tables](https://spothero.looker.com/explore/redshift_model/redshift_table_scans?qid=OJS3iUBsdwogNMaV8ARCkl&toggle=fil,pik) by the shared Looker Redshift user within the past week:

 - pipegen.pg_spot_microclimate
 - pipegen.pg_rentals
 - pipegen.pg_destination_microclimate
 - pipegen.pg_rental_facts
 - pipegen.pg_event_rate_rentals
 - pipegen.pg_currency_exchange_rate
 - pipegen.pg_parent_event
 - pipegen.pg_panda_spothero_event_mapping
 - pipegen.pg_rentals_past_eight_weeks
 - pipegen.pg_proxy_renter_ltv

### Getting Set Up

1. [Install](https://docs.getdbt.com/docs/macos) the dbt CLI using pip or homebrew

```
brew update
brew tap fishtown-analytics/dbt
brew install dbt
```
or 
```
pip install dbt
```

2. Clone this repo

3. Configure your profile

> When you invoke dbt from the CLI, dbt parses your `dbt_project.yml` for the name of the profile to use to connect to your data warehouse.
>
> dbt then checks your `profiles.yml` file for a profile with the same name. A profile contains all the details required to connect to your data warehouse.
>
>By default, dbt expects the `profiles.yml` file to be located in the `~/.dbt/ directory`.

Check out the [connection docs](https://docs.getdbt.com/docs/configure-your-profile#section-what-goes-in-my-profiles-yml-file) for details, but the gist of it is you'll need to create `~/.dbt/profiles.yml`; `~/.dbt/` should be created in your root directory when you install dbt:

```
default:
  outputs:
    dev:
      type: redshift
      threads: 1
      host: fivetran-cluster.c5vrcrz4nn1n.us-west-2.redshift.amazonaws.com 
      port: 5439
      user: <redshift_username>
      pass: <redshift_password>
      dbname: spotheroprod
      schema: <dev_schema_name>
    prod:
      type: redshift
      threads: 1
      host: fivetran-cluster.c5vrcrz4nn1n.us-west-2.redshift.amazonaws.com
      port: 5439
      user: <redshift_username>
      pass: <redshift_password>
      dbname: spotheroprod
      schema: <prod_schema_name>
  target: dev
```

From the dbt repo directory, run `dbt debug` to test your Redshift credentials.

### Notes/Observations

- We can't use `{{ ref() }}` syntax when jobs are self-referential, so this might require some refactoring of existing Pipegen jobs.. For example, in `pg_rental_facts.sql`, the first CTE references itself:

```sql
-- Logic to build `pipegen.pg_rental_facts` 

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
-- self-referential query; cannot use {{ ref('pg_rental_facts') }}
FROM pipegen.pg_rental_facts AS rental 
...
```

This is likely a solid use-case to have a stand-alone `renter_latest_valid_facts.sql` ephemeral materialization; in the meantime, self-referential queries will not use {{ ref() }}
