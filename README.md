# Getting Set Up

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

**Requirements:** Redshift user with write access to target schema(s). For the sake of running this demo, you can specifiy the same schema for dev & prod use cases within your config (below).

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

# Demo

## Purpose

This demo is meant to demonstrate [dbt's](https://docs.getdbt.com/docs) functionality for the following ETL tooling use cases:

- Job dependency management
- Table-level data lineage
- Column-level documentation
- Column-level testing functionality

Regardless of whether or not we adopt dbt within our stack, this work should clearly demonstrate the the value of this ETL functionality with a subset of existing Pipegen jobs.

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
 
## Functionality

### Job dependency management

Leveraging `{{ ref('<model_name>') }}` within interdependent SQL queries in the `/models` directory automatically informs the order & scope of job(s) to be executed and the DAG is dynamically built based on arguments passed in the `dbt run` command. ([See docs](https://docs.getdbt.com/docs/model-selection-syntax#section-model-selection-syntax)) 

#### Demo: Job dependency management

> By default, `dbt run` will execute all of the models in the dependency graph. During development (and deployment), it is useful to specify only a subset of models to run. Use the `--models` flag with dbt run to select a subset of models to run. 

Outcome of `dbt run` - everything runs, regardless of dependencies:

```
19:54:51 | Concurrency: 1 threads (target='dev')
19:54:51 | 
19:54:51 | 1 of 11 START view model maggiehays.pg_currency_exchange_rate........ [RUN]
19:54:59 | 1 of 11 OK created view model maggiehays.pg_currency_exchange_rate... [CREATE VIEW in 8.35s]
19:54:59 | 2 of 11 START view model maggiehays.pg_transaction_fee_summary_gmv... [RUN]
19:55:07 | 2 of 11 OK created view model maggiehays.pg_transaction_fee_summary_gmv [CREATE VIEW in 8.20s]
19:55:07 | 3 of 11 START view model maggiehays.pg_parent_event.................. [RUN]
19:55:16 | 3 of 11 OK created view model maggiehays.pg_parent_event............. [CREATE VIEW in 8.74s]
19:55:16 | 4 of 11 START view model maggiehays.pg_destination_microclimate...... [RUN]
19:55:21 | 4 of 11 OK created view model maggiehays.pg_destination_microclimate. [CREATE VIEW in 4.59s]
19:55:21 | 5 of 11 START view model maggiehays.pg_rentals_past_eight_weeks...... [RUN]
19:55:27 | 5 of 11 OK created view model maggiehays.pg_rentals_past_eight_weeks. [CREATE VIEW in 6.37s]
19:55:27 | 6 of 11 START view model maggiehays.pg_spot_microclimate............. [RUN]
19:55:33 | 6 of 11 OK created view model maggiehays.pg_spot_microclimate........ [CREATE VIEW in 4.80s]
19:55:33 | 7 of 11 START view model maggiehays.pg_rentals....................... [RUN]
19:55:42 | 7 of 11 OK created view model maggiehays.pg_rentals.................. [CREATE VIEW in 8.84s]
19:55:42 | 8 of 11 START view model maggiehays.pg_panda_spothero_event_mapping.. [RUN]
19:55:51 | 8 of 11 OK created view model maggiehays.pg_panda_spothero_event_mapping [CREATE VIEW in 9.42s]
19:55:51 | 9 of 11 START view model maggiehays.pg_event_rate_rentals............ [RUN]
19:56:20 | 9 of 11 OK created view model maggiehays.pg_event_rate_rentals....... [CREATE VIEW in 28.66s]
19:56:20 | 10 of 11 START view model maggiehays.pg_proxy_renter_ltv............. [RUN]
19:56:28 | 10 of 11 OK created view model maggiehays.pg_proxy_renter_ltv........ [CREATE VIEW in 7.47s]
19:56:28 | 11 of 11 START view model maggiehays.pg_rental_facts................. [RUN]
19:56:33 | 11 of 11 OK created view model maggiehays.pg_rental_facts............ [CREATE VIEW in 5.35s]
19:56:33 | 
19:56:33 | Finished running 11 view models in 105.70s.
Completed successfully
```

Outcome of `dbt run --models +pg_rental_facts+` to only build upstream and downstream jobs:

```
19:43:48 | Concurrency: 1 threads (target='dev')
19:43:48 | 
19:43:48 | 1 of 4 START view model maggiehays.pg_currency_exchange_rate......... [RUN]
19:43:56 | 1 of 4 OK created view model maggiehays.pg_currency_exchange_rate.... [CREATE VIEW in 7.81s]
19:43:56 | 2 of 4 START view model maggiehays.pg_transaction_fee_summary_gmv.... [RUN]
19:44:01 | 2 of 4 OK created view model maggiehays.pg_transaction_fee_summary_gmv [CREATE VIEW in 4.88s]
19:44:01 | 3 of 4 START view model maggiehays.pg_rentals........................ [RUN]
19:44:08 | 3 of 4 OK created view model maggiehays.pg_rentals................... [CREATE VIEW in 6.50s]
19:44:08 | 4 of 4 START view model maggiehays.pg_rental_facts................... [RUN]
19:44:13 | 4 of 4 OK created view model maggiehays.pg_rental_facts.............. [CREATE VIEW in 4.95s]
19:44:13 | 
19:44:13 | Finished running 4 view models in 29.22s.
```


### Table-level data lineage

### Column-level documentation

### Column-level testing functionality


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
