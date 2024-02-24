## Module 4 Homework 

SETUP:

We will use the data loaded for:

* Building a source table: `stg_fhv_tripdata`
* Building a fact table: `fact_fhv_trips`
* Create a dashboard 

See folder `taxi_rides_ny` for DBT files

Dashboard URL: `https://lookerstudio.google.com/reporting/44bd8ff2-cbd7-478e-96a2-ce46222a2684`

Dashboard PDF: See file `NY_Taxi_Data_2019_-_2020.pdf` 

-----------------------

### Question 1: 

**What happens when we execute dbt build --vars '{'is_test_run': 'true'}'**

- It's the same as running *dbt build*
- It applies a _limit 100_ to all of our models
- It applies a _limit 100_ only to our staging models
- Nothing

**ANSWER: tbc**

-----------------------

### Question 2: 

**What is the code that our CI job will run? Where is this code coming from?**  

- The code that has been merged into the main branch
- __*The code that is behind the creation object on the `dbt_cloud_pr_` schema*__
- The code from any development branch that has been opened based on main
- The code from the development branch we are requesting to merge to main

**ANSWER: 'The code that is behind the creation object on the `dbt_cloud_pr_` schema'**

*"When you set up CI jobs, dbt Cloud listens for webhooks from your Git provider indicating that a new PR has been opened or updated with new commits. When dbt Cloud receives one of these webhooks, it enqueues a new run of the CI job.*

*dbt Cloud builds and tests the models affected by the code change in a temporary schema, unique to the PR. This process ensures that the code builds without error and that it matches the expectations as defined by the project's dbt tests. The unique schema name follows the naming convention dbt_cloud_pr_<job_id>_<pr_id> (for example, dbt_cloud_pr_1862_1704) and can be found in the run details for the given run.*

*Viewing the temporary schema name for a run triggered by a PR
Viewing the temporary schema name for a run triggered by a PR
When the CI run completes, you can view the run status directly from within the pull request. dbt Cloud updates the pull request in GitHub, GitLab, or Azure DevOps with a status message indicating the results of the run. The status message states whether the models and tests ran successfully or not.*

*dbt Cloud deletes the temporary schema from your data warehouse when you close or merge the pull request."*

-----------------------

### Question 3 (2 points)

**What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)?** 

Create a staging model for the fhv data, similar to the ones made for yellow and green data. Add an additional filter for keeping only records with pickup time in year 2019.
Do not add a deduplication step. Run this models without limits (is_test_run: false).

```
file: taxi_rides_ny/models/staging/stg_fhv_tripdata.sql

Run Command:

dbt build --select stg_fhv_tripdata --vars '{'is_test_run': 'false'}'
```

Create a core model similar to fact trips, but selecting from stg_fhv_tripdata and joining with dim_zones.
Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries for pickup and dropoff locations. 
Run the dbt model without limits (is_test_run: false).

```
file: taxi_rides_ny/models/core/fact_fhv_trips.sql

Run Command:

dbt build --select fact_fhv_trips --vars '{'is_test_run': 'false'}'
```

```bash
OUTPUT: 

15:21:44 BigQuery adapter: https://console.cloud.google.com/bigquery?project=dbt-module-4-28646&j=bq:US:7b05cdee-baa3-4229-ba9f-fb5b2ada1973&page=queryresults
15:21:50 Timing info for model.taxi_rides_ny.fact_fhv_trips (execute): 15:21:43.339722 => 15:21:50.111130
15:21:50 1 of 1 OK created sql table model dbt_lpollard.fact_fhv_trips .................. [CREATE TABLE (23.0m rows, 1.9 GiB processed) in 6.80s]
15:21:50 Finished running node model.taxi_rides_ny.fact_fhv_trips
```

- 12998722
- __*22998722*__
- 32998722
- 42998722

**ANSWER: 22,998,722**

-----------------------

### Question 4 (2 points)

**What is the service that had the most rides during the month of July 2019 month with the biggest amount of rides after building a tile for the fact_fhv_trips table?**

Create a dashboard with some tiles that you find interesting to explore the data. One tile should show the amount of trips per month, as done in the videos for fact_trips, including the fact_fhv_trips data.

- FHV
- Green
- __*Yellow*__
- FHV and Green

**ANSWER: Yellow**

---------------------
