# RisingWave Workshop - Homework

## Setting up

In order to get a static set of results, we will use historical data from the dataset.

Run the following commands:
```bash
# Load the cluster op commands.
source commands.sh
# First, reset the cluster:
clean-cluster
# Start a new cluster
start-cluster
# wait for cluster to start
sleep 5
# Seed historical data instead of real-time data
seed-kafka
# Recreate trip data table
psql -f risingwave-sql/table/trip_data.sql
# Wait for a while for the trip_data table to be populated.
sleep 5
# Check that you have 100K records in the trip_data table
# You may rerun it if the count is not 100K
psql -c "SELECT COUNT(*) FROM trip_data"
```

--------------------------------------------------------

Set Up Output: 

```bash
(base) lottie@de-zoomcamp:~$ cd risingwave-data-talks-workshop-2024-03-04/
(base) lottie@de-zoomcamp:~/risingwave-data-talks-workshop-2024-03-04$ source commands.sh
(base) lottie@de-zoomcamp:~/risingwave-data-talks-workshop-2024-03-04$ source .venv/bin/activate
(.venv) (base) lottie@de-zoomcamp:~/risingwave-data-talks-workshop-2024-03-04$ clean-cluster
[+] Running 13/13
 ✔ Container risingwave-standalone  Removed                                                                                                        10.5s
 ✔ Container prometheus-0           Removed                                                                                                         0.4s
 ✔ Container grafana-0              Removed                                                                                                         0.3s
 ✔ Container message_queue          Removed                                                                                                         1.0s
 ✔ Container clickhouse             Removed                                                                                                         1.7s
 ✔ Container etcd-0                 Removed                                                                                                         0.4s
 ✔ Container minio-0                Removed                                                                                                         0.5s
 ✔ Volume docker_message_queue      Removed                                                                                                         0.0s
 ✔ Volume docker_grafana-0          Removed                                                                                                         0.0s
 ✔ Volume docker_prometheus-0       Removed                                                                                                         0.1s
 ✔ Volume docker_minio-0            Removed                                                                                                         0.0s
 ✔ Volume docker_etcd-0             Removed                                                                                                         0.0s
 ✔ Network docker_default           Removed                                                                                                         0.3s
(.venv) (base) lottie@de-zoomcamp:~/risingwave-data-talks-workshop-2024-03-04$ start-cluster
[+] Running 13/13
 ✔ Network docker_default           Created                                                                                                         0.1s
 ✔ Volume "docker_etcd-0"           Created                                                                                                         0.0s
 ✔ Volume "docker_grafana-0"        Created                                                                                                         0.0s
 ✔ Volume "docker_minio-0"          Created                                                                                                         0.0s
 ✔ Volume "docker_prometheus-0"     Created                                                                                                         0.0s
 ✔ Volume "docker_message_queue"    Created                                                                                                         0.0s
 ✔ Container message_queue          Started                                                                                                         0.1s
 ✔ Container clickhouse             Started                                                                                                         0.1s
 ✔ Container etcd-0                 Started                                                                                                         0.1s
 ✔ Container minio-0                Started                                                                                                         0.1s
 ✔ Container grafana-0              Started                                                                                                         0.1s
 ✔ Container prometheus-0           Started                                                                                                         0.1s
 ✔ Container risingwave-standalone  Started                                                                                                         0.0s
(.venv) (base) lottie@de-zoomcamp:~/risingwave-data-talks-workshop-2024-03-04$ sleep 5
(.venv) (base) lottie@de-zoomcamp:~/risingwave-data-talks-workshop-2024-03-04$ seed-kafka
INFO:root:Loading taxi zone data
INFO:root:Checking connection to the RisingWave
INFO:root:RisingWave started with version: PostgreSQL 9.5-RisingWave-1.6.1 (02ee186211e44001c645027bf5aca3db5f076d29)
INFO:root:Created taxi_zone table
INFO:root:Created 265 records in taxi_zone table
INFO:root:Loading trip data
INFO:root:Topic trip_data created
INFO:root:Sending historical data
INFO:root:Sending 100,000 records to Kafka
INFO:root:Sent 0 records
INFO:root:Sent 100,000 records to Kafka
(.venv) (base) lottie@de-zoomcamp:~/risingwave-data-talks-workshop-2024-03-04$ psql -f risingwave-sql/table/trip_data.sql
CREATE_TABLE
(.venv) (base) lottie@de-zoomcamp:~/risingwave-data-talks-workshop-2024-03-04$ sleep 5
(.venv) (base) lottie@de-zoomcamp:~/risingwave-data-talks-workshop-2024-03-04$ psql -c "SELECT COUNT(*) FROM trip_data"
 count
--------
 100000
(1 row)

(.venv) (base) lottie@de-zoomcamp:~/risingwave-data-talks-workshop-2024-03-04$ psql
psql (14.11 (Ubuntu 14.11-0ubuntu0.22.04.1), server 9.5.0)
Type "help" for help.

dev=> SHOW TABLES;
   Name
-----------
 taxi_zone
 trip_data
(2 rows)
```


## Question 1

Create a materialized view to compute the average, min and max trip time **between each taxi zone**.

Note that we consider the do not consider `a->b` and `b->a` as the same trip pair.
So as an example, you would consider the following trip pairs as different pairs:

```plaintext
Yorkville East -> Steinway
Steinway -> Yorkville East
```
--------------------------

```sql
-- CREATE MATERIALISED VIEW 

CREATE MATERIALIZED VIEW trip_time_taxi_zone AS
WITH t AS (
    SELECT 
        pulocationid,
        dolocationid,
        AVG(tpep_dropoff_datetime - tpep_pickup_datetime) AS avg_trip_time,
        MIN(tpep_dropoff_datetime - tpep_pickup_datetime) AS min_trip_time,
        MAX(tpep_dropoff_datetime - tpep_pickup_datetime) AS max_trip_time
    FROM 
        trip_data
    GROUP BY pulocationid, dolocationid
)
SELECT  
    taxi_zone_pu.Zone as pickup_taxi_zone, 
    taxi_zone_do.Zone as dropoff_taxi_zone,
    t.avg_trip_time,
    t.min_trip_time,
    t.max_trip_time
FROM t
JOIN taxi_zone as taxi_zone_pu
    ON t.pulocationid = taxi_zone_pu.location_id
JOIN taxi_zone as taxi_zone_do
    ON t.dolocationid = taxi_zone_do.location_id;
```

```bash
          pickup_taxi_zone           |          dropoff_taxi_zone          |  avg_trip_time  | min_trip_time | max_trip_time
-------------------------------------+-------------------------------------+-----------------+---------------+----------------
 Astoria                             | Forest Hills                        | 00:16:39        | 00:16:39      | 00:16:39
 Alphabet City                       | Midtown North                       | 00:19:23        | 00:19:23      | 00:19:23
 Alphabet City                       | Stuy Town/Peter Cooper Village      | 00:06:31        | 00:06:05      | 00:06:57
 Astoria                             | Coney Island                        | 00:41:35        | 00:41:35      | 00:41:35
 Alphabet City                       | TriBeCa/Civic Center                | 00:14:37        | 00:12:16      | 00:16:58
 Alphabet City                       | Clinton East                        | 00:24:55.333333 | 00:19:23      | 00:33:08

```


From this MV, find the pair of taxi zones with the highest average trip time.
You may need to use the [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/) for this.

Options:

1. _**Yorkville East, Steinway**_
2. Murray Hill, Midwood
3. East Flatbush/Farragut, East Harlem North
4. Midtown Center, University Heights/Morris Heights


**ANSWER: Yorkville East, Steinway**

```sql
-- FIND PAIR OF TAXI ZONES WITH THE HIGHEST AVERAGE TRIP TIME 

SELECT
    pickup_taxi_zone,
    dropoff_taxi_zone,
    avg_trip_time
FROM
    trip_time_taxi_zone
WHERE
    avg_trip_time = (SELECT MAX(avg_trip_time) FROM trip_time_taxi_zone);
```

```bash
 pickup_taxi_zone | dropoff_taxi_zone | avg_trip_time
------------------+-------------------+---------------
 Yorkville East   | Steinway          | 23:59:33
(1 row)

```

## Question 1 - Bonus

Bonus (no marks): Create an MV which can identify anomalies in the data. For example, if the average trip time between two zones is 1 minute,
but the max trip time is 10 minutes and 20 minutes respectively.

**ANSWER: Bonus Anomaly Detection SQL Query**

```sql
CREATE MATERIALIZED VIEW trip_time_taxi_zone_anomalies AS
WITH t AS (
    SELECT 
        pulocationid,
        dolocationid,
        AVG(tpep_dropoff_datetime - tpep_pickup_datetime) AS avg_trip_time,
        MIN(tpep_dropoff_datetime - tpep_pickup_datetime) AS min_trip_time,
        MAX(tpep_dropoff_datetime - tpep_pickup_datetime) AS max_trip_time
    FROM 
        trip_data
    GROUP BY pulocationid, dolocationid
)
SELECT  
    taxi_zone_pu.Zone as pickup_taxi_zone, 
    taxi_zone_do.Zone as dropoff_taxi_zone,
    avg_trip_time,
    tpep_dropoff_datetime - tpep_pickup_datetime AS actual_trip_time,
    'Anomaly' AS anomaly_type,
    (tpep_dropoff_datetime - tpep_pickup_datetime) - t.avg_trip_time AS variance_from_average
FROM t
JOIN trip_data ON t.pulocationid = trip_data.pulocationid AND t.dolocationid = trip_data.dolocationid
JOIN taxi_zone AS taxi_zone_pu
    ON t.pulocationid = taxi_zone_pu.location_id
JOIN taxi_zone AS taxi_zone_do
    ON t.dolocationid = taxi_zone_do.location_id
WHERE
    (tpep_dropoff_datetime - tpep_pickup_datetime) > t.avg_trip_time * 1.5 OR (tpep_dropoff_datetime - tpep_pickup_datetime) < t.avg_trip_time * 0.5;
```


```bash
         pickup_taxi_zone         |        dropoff_taxi_zone         |  avg_trip_time  | actual_trip_time | anomaly_type | variance_from_average
----------------------------------+----------------------------------+-----------------+------------------+--------------+-----------------------
 LaGuardia Airport                | Windsor Terrace                  | 00:25:20.142857 | 00:38:14         | Anomaly      | 00:12:53.857143
 Lincoln Square West              | Upper East Side North            | 00:09:52.487179 | 00:16:10         | Anomaly      | 00:06:17.512821
 East Village                     | JFK Airport                      | 00:30:57.6      | 01:20:16         | Anomaly      | 00:49:18.4
 Elmhurst/Maspeth                 | Elmhurst/Maspeth                 | 00:01:46        | 00:04:54         | Anomaly      | 00:03:08
 Yorkville West                   | Mott Haven/Port Morris           | 00:11:49        | 00:19:08         | Anomaly      | 00:07:19
 Inwood                           | Inwood                           | 00:01:31        | 00:02:58         | Anomaly      | 00:01:27
 Central Park                     | Yorkville East                   | 00:09:45.076923 | 00:17:03         | Anomaly      | 00:07:17.923077
 Gramercy                         | Bushwick South                   | 00:40:01.2      | 00:17:15         | Anomaly      | -00:22:46.2
 Lincoln Square West              | Lenox Hill West                  | 00:11:50.548387 | 00:18:17         | Anomaly      | 00:06:26.451613
 Lincoln Square West              | Flatiron                         | 00:13:00.75     | 00:06:06         | Anomaly      | -00:06:54.75
 Lincoln Square West              | NV                               | 00:02:37        | 00:00:00         | Anomaly      | -00:02:37
 Lincoln Square West              | Central Park                     | 00:08:44.888889 | 00:13:15         | Anomaly      | 00:04:30.111111
 Lincoln Square West              | Sutton Place/Turtle Bay North    | 00:14:12.176471 | 00:24:59         | Anomaly      | 00:10:46.823529
 Astoria                          | Astoria                          | 00:04:08.944444 | 00:07:36         | Anomaly      | 00:03:27.055556
 Astoria                          | Sunnyside                        | 00:06:35        | 00:01:03         | Anomaly      | -00:05:32
 Lincoln Square West              | Midtown Center                   | 00:12:47.642857 | 00:06:21         | Anomaly      | -00:06:26.642857
 Lincoln Square West              | East Chelsea                     | 00:09:05.896552 | 00:14:28         | Anomaly      | 00:05:22.103448
 Central Park                     | Upper East Side North            | 00:06:48.027027 | 00:03:06         | Anomaly      | -00:03:42.027027
 SoHo                             | JFK Airport                      | 00:34:36.866667 | 01:04:02         | Anomaly      | 00:29:25.133333
 Elmhurst/Maspeth                 | Elmhurst/Maspeth                 | 00:01:46        | 00:00:12         | Anomaly      | -00:01:34
 East Harlem North                | Mott Haven/Port Morris           | 00:06:34.666667 | 00:10:05         | Anomaly      | 00:03:30.333333
 Inwood                           | Inwood                           | 00:01:31        | 00:00:06         | Anomaly      | -00:01:25
 Central Park                     | Yorkville East                   | 00:09:45.076923 | 00:15:59         | Anomaly      | 00:06:13.923077
 Gramercy                         | Bushwick South                   | 00:40:01.2      | 00:16:24         | Anomaly      | -00:23:37.2
 Lincoln Square West              | Morningside Heights              | 00:08:28.4375   | 00:14:40         | Anomaly      | 00:06:11.5625
 Central Park                     | Flatiron                         | 00:19:22.625    | 00:07:21         | Anomaly      | -00:12:01.625
 Lincoln Square West              | NV                               | 00:02:37        | 00:00:21         | Anomaly      | -00:02:16
 Lincoln Square West              | Central Park                     | 00:08:44.888889 | 00:03:35         | Anomaly      | -00:05:09.888889
 Lincoln Square West              | Sutton Place/Turtle Bay North    | 00:14:12.176471 | 00:24:59         | Anomaly      | 00:10:46.823529
 Astoria                          | Astoria                          | 00:04:08.944444 | 00:00:15         | Anomaly      | -00:03:53.944444
 Midtown North                    | Sunnyside                        | 00:13:03.428571 | 00:19:47         | Anomaly      | 00:06:43.571429
 Lincoln Square West              | Midtown Center                   | 00:12:47.642857 | 00:19:34         | Anomaly      | 00:06:46.357143
 Lincoln Square West              | East Chelsea                     | 00:09:05.896552 | 00:14:03         | Anomaly      | 00:04:57.103448
 Central Park                     | Upper East Side North            | 00:06:48.027027 | 00:02:42         | Anomaly      | -00:04:06.027027
 World Trade Center               | JFK Airport                      | 00:35:32.289474 | 01:15:29         | Anomaly      | 00:39:56.710526
```

------------------------------------------------------------------------------------------------


## Question 2

Recreate the MV(s) in question 1, to also find the **number of trips** for the pair of taxi zones with the highest average trip time.

Options:
1. 5
2. 3
3. 10
4. _**1**_

**ANSWER: Yorkville East, Steinway - trip_count = 1**

```sql
-- DROP MATERIALISED VIEW TO RECREATE IT 

DROP MATERIALIZED VIEW trip_time_taxi_zone;

-- CREATE MATERIALISED VIEW 

CREATE MATERIALIZED VIEW trip_time_taxi_zone AS
WITH t AS (
    SELECT 
        pulocationid,
        dolocationid,
        COUNT(*) AS trip_count,
        AVG(tpep_dropoff_datetime - tpep_pickup_datetime) AS avg_trip_time,
        MIN(tpep_dropoff_datetime - tpep_pickup_datetime) AS min_trip_time,
        MAX(tpep_dropoff_datetime - tpep_pickup_datetime) AS max_trip_time
    FROM 
        trip_data
    GROUP BY pulocationid, dolocationid
)
SELECT  
    taxi_zone_pu.Zone as pickup_taxi_zone, 
    taxi_zone_do.Zone as dropoff_taxi_zone,
    t.trip_count,
    t.avg_trip_time,
    t.min_trip_time,
    t.max_trip_time
FROM t
JOIN taxi_zone AS taxi_zone_pu
    ON t.pulocationid = taxi_zone_pu.location_id
JOIN taxi_zone AS taxi_zone_do
    ON t.dolocationid = taxi_zone_do.location_id;
```

```bash
          pickup_taxi_zone           |          dropoff_taxi_zone          | trip_count |  avg_trip_time  | min_trip_time | max_trip_time
-------------------------------------+-------------------------------------+------------+-----------------+---------------+----------------
 Alphabet City                       | Prospect-Lefferts Gardens           |          2 | 00:23:16        | 00:20:26      | 00:26:06
 Alphabet City                       | Flatbush/Ditmas Park                |          1 | 00:26:07        | 00:26:07      | 00:26:07
 Allerton/Pelham Gardens             | Allerton/Pelham Gardens             |          1 | 00:00:07        | 00:00:07      | 00:00:07
 Alphabet City                       | Times Sq/Theatre District           |          1 | 00:23:50        | 00:23:50      | 00:23:50
 Allerton/Pelham Gardens             | Alphabet City                       |          1 | 00:31:10        | 00:31:10      | 00:31:10
 Alphabet City                       | SoHo                                |          2 | 00:08:52        | 00:08:32      | 00:09:12
 Alphabet City                       | Kips Bay                            |          5 | 00:07:03.8      | 00:06:08      | 00:08:13

```

```sql
-- NO. OF TRIPS FOR THE PAIR OF TAXI ZONES WITH THE HIGHEST AVERAGE TRIP TIME (Yorkville East, Steinway)

SELECT 
    pickup_taxi_zone,
    dropoff_taxi_zone,
    trip_count
FROM 
    trip_time_taxi_zone
WHERE 
    avg_trip_time = (SELECT MAX(avg_trip_time) FROM trip_time_taxi_zone);
```

```bash
 pickup_taxi_zone | dropoff_taxi_zone | trip_count
------------------+-------------------+------------
 Yorkville East   | Steinway          |          1
(1 row)
```

-------------------------------------------------------------------

## Question 3

From the latest pickup time to 17 hours before, what are the top 3 busiest zones in terms of number of pickups?
For example if the latest pickup time is 2020-01-01 17:00:00,
then the query should return the top 3 busiest zones from 2020-01-01 00:00:00 to 2020-01-01 17:00:00.

HINT: You can use [dynamic filter pattern](https://docs.risingwave.com/docs/current/sql-pattern-dynamic-filters/)
to create a filter condition based on the latest pickup time.

NOTE: For this question `17 hours` was picked to ensure we have enough data to work with.

Options:
1. Clinton East, Upper East Side North, Penn Station
2. _**LaGuardia Airport, Lincoln Square East, JFK Airport**_
3. Midtown Center, Upper East Side South, Upper East Side North
4. LaGuardia Airport, Midtown Center, Upper East Side North

**ANSWER: LaGuardia Airport, Lincoln Square East, JFK Airport**


```sql
-- CREATE MATERIALISED VIEW FOR LATEST PICKUP TIME

CREATE MATERIALIZED VIEW latest_pickup AS
    SELECT MAX(tpep_pickup_datetime) AS pickup_datetime
    FROM trip_data;
```

```bash
   pickup_datetime
---------------------
 2022-01-03 10:53:33
(1 row)
```

```sql
WITH t AS (
    SELECT
        COUNT(*) AS num_trips,
        trip_data.pulocationid
    FROM latest_pickup, trip_data
    WHERE trip_data.tpep_pickup_datetime > (SELECT pickup_datetime FROM latest_pickup) - interval '17 hours'
    GROUP BY trip_data.pulocationid
    ORDER BY num_trips DESC
    LIMIT 3
)
SELECT taxi_zone.zone
FROM t
JOIN taxi_zone AS taxi_zone
    ON t.pulocationid = taxi_zone.location_id;
```

```bash
        zone
---------------------
 LaGuardia Airport
 Lincoln Square East
 JFK Airport
(3 rows)
```
-------------------------------------------------------------------
