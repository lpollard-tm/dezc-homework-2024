## Module 6 Homework

## Question 1: Redpanda version

``` bash
$ git clone https://github.com/redpanda-data-blog/2023-python-gsg.git

$ cd 2023-python-gsg/

$ docker-compose up -d

$ docker exec -it redpanda-1 redpanda --version
```

``` bash
# Output - v22.3.5 - 28b2443c33b0c2f8807bc5c460cc2478cfee1044-dirty
```

## Question 2. Creating a topic

```bash
$ docker exec -it redpanda-1 rpk topic create test-topic
```

``` bash
# output

TOPIC       STATUS
test-topic  OK
```

``` bash
$ docker exec -it redpanda-1 rpk topic list
```

```bash
# output 

NAME        PARTITIONS  REPLICAS
test-topic  1           1
```

## Question 3. Connecting to the Kafka server

```python
import json
import time 

from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

producer.bootstrap_connected()
```

```bash
# output

True
```

## Question 4. Sending data to the stream

```python
import time

t0 = time.time()

topic_name = 'test-topic'

for i in range(10):
    message = {'number': i}
    producer.send(topic_name, value=message)
    print(f"Sent: {message}")
    time.sleep(0.05)

t_sent = time.time()
print(f'Sending messages took {(t_sent - t0):.2f} seconds')

producer.flush()

t_flush = time.time()
print(f'Flushing took {(t_flush - t_sent):.2f} seconds')

t1 = time.time()
print(f'Total time took {(t1 - t0):.2f} seconds')
```

```bash
# output

Sent: {'number': 0}
Sent: {'number': 1}
Sent: {'number': 2}
Sent: {'number': 3}
Sent: {'number': 4}
Sent: {'number': 5}
Sent: {'number': 6}
Sent: {'number': 7}
Sent: {'number': 8}
Sent: {'number': 9}
Sending messages took 0.53 seconds
Flushing took 0.00 seconds
Total time took 0.53 seconds
```

**ANSWER: Sending the messages - 0.52 seconds**

## Question 5: Sending the Trip Data

How much time in seconds did it take? (You can round it to a whole number)

```bash
$ docker exec -it redpanda-1 rpk topic create green-trips    
```

```bash
#output 

TOPIC       STATUS
green-trips OK
```

```python
import pandas as pd
import time
import json
from kafka import KafkaProducer

file_path = 'green_tripdata_2019-10.csv.gz'

df = pd.read_csv(file_path)

df_green = df[['lpep_pickup_datetime',
               'lpep_dropoff_datetime',
               'PULocationID',
               'DOLocationID',
               'passenger_count',
               'trip_distance',
               'tip_amount']]

producer = KafkaProducer(bootstrap_servers='localhost:9092')

t0 = time.time()
topic_name = 'green-trips'
for row in df_green.itertuples(index=False):
    row_dict = {col: getattr(row, col) for col in row._fields}
    value_bytes = json.dumps(row_dict).encode('utf-8')
    producer.send(topic_name, value=value_bytes)
t1 = time.time()

time_taken = round(t1 - t0)

print(f'It took {time_taken} seconds to send the data')
```

```bash
#output 

It took 129 seconds to send the data
```

**ANSWER: 129 seconds to send the data**

## Question 6. Parsing the data

```python
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

pyspark_version = pyspark.__version__
kafka_jar_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark_version}"
spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("GreenTripsConsumer") \
    .config("spark.jars.packages", kafka_jar_package) \
    .getOrCreate()

green_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "green-trips") \
    .option("startingOffsets", "earliest") \
    .load()

schema = StructType() \
    .add("lpep_pickup_datetime", StringType()) \
    .add("lpep_dropoff_datetime", StringType()) \
    .add("PULocationID", IntegerType()) \
    .add("DOLocationID", IntegerType()) \
    .add("passenger_count", DoubleType()) \
    .add("trip_distance", DoubleType()) \
    .add("tip_amount", DoubleType())

parsed_stream = green_stream \
    .select(F.from_json(F.col("value").cast('STRING'), schema).alias("data")) \
    .select("data.*")

query = parsed_stream \
    .writeStream \
    .format("console") \
    .start()

query.awaitTermination()
```

How does the record look after parsing?

*Answer:*

```bash
# output

+--------------------+---------------------+------------+------------+---------------+-------------+----------+
|lpep_pickup_datetime|lpep_dropoff_datetime|PULocationID|DOLocationID|passenger_count|trip_distance|tip_amount|
+--------------------+---------------------+------------+------------+---------------+-------------+----------+
| 2019-10-01 00:26:02|  2019-10-01 00:39:58|         112|         196|              1|         5.88|       0.0|
| 2019-10-01 00:18:11|  2019-10-01 00:22:38|          43|         263|              1|          0.8|       0.0|
| 2019-10-01 00:09:31|  2019-10-01 00:24:47|         255|         228|              2|          7.5|       0.0|
| 2019-10-01 00:37:40|  2019-10-01 00:41:49|         181|         181|              1|          0.9|       0.0|
| 2019-10-01 00:08:13|  2019-10-01 00:17:56|          97|         188|              1|         2.52|      2.26|
| 2019-10-01 00:35:01|  2019-10-01 00:43:40|          65|          49|              1|         1.47|      1.86|
| 2019-10-01 00:28:09|  2019-10-01 00:30:49|           7|         179|              1|          0.6|       1.0|
| 2019-10-01 00:28:26|  2019-10-01 00:32:01|          41|          74|              1|         0.56|       0.0|
| 2019-10-01 00:14:01|  2019-10-01 00:26:16|         255|          49|              1|         2.42|       0.0|
| 2019-10-01 00:03:03|  2019-10-01 00:17:13|         130|         131|              1|          3.4|      2.85|
| 2019-10-01 00:07:10|  2019-10-01 00:23:38|          24|          74|              3|         3.18|       0.0|
| 2019-10-01 00:25:48|  2019-10-01 00:49:52|         255|         188|              1|          4.7|       1.0|
| 2019-10-01 00:03:12|  2019-10-01 00:14:43|         129|         160|              1|          3.1|       0.0|
| 2019-10-01 00:44:56|  2019-10-01 00:51:06|          18|         169|              1|         1.19|      0.25|
| 2019-10-01 00:55:14|  2019-10-01 01:00:49|         223|           7|              1|         1.09|      1.46|
| 2019-10-01 00:06:06|  2019-10-01 00:11:05|          75|         262|              1|         1.24|      2.01|
| 2019-10-01 00:00:19|  2019-10-01 00:14:32|          97|         228|              1|         3.03|      3.58|
| 2019-10-01 00:09:31|  2019-10-01 00:20:41|          41|          74|              1|         2.03|      2.16|
| 2019-10-01 00:30:36|  2019-10-01 00:34:30|          41|          42|              1|         0.73|      1.26|
| 2019-10-01 00:58:32|  2019-10-01 01:05:08|          41|         116|              1|         1.48|       0.0|
+--------------------+---------------------+------------+------------+---------------+-------------+----------+
```

### Question 7: Most popular destination

```python
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType

pyspark_version = pyspark.__version__
kafka_jar_package = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark_version}"
spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("GreenTripsConsumer") \
    .config("spark.jars.packages", kafka_jar_package) \
    .getOrCreate()

green_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "green-trips") \
    .option("startingOffsets", "earliest") \
    .load()

schema = StructType() \
    .add("lpep_pickup_datetime", StringType()) \
    .add("lpep_dropoff_datetime", StringType()) \
    .add("PULocationID", IntegerType()) \
    .add("DOLocationID", IntegerType()) \
    .add("passenger_count", DoubleType()) \
    .add("trip_distance", DoubleType()) \
    .add("tip_amount", DoubleType())

parsed_stream = green_stream \
    .select(F.from_json(F.col("value").cast('STRING'), schema).alias("data")) \
    .select("data.*") \

parsed_stream_with_timestamp = parsed_stream.withColumn("timestamp", F.current_timestamp())

popular_destinations = parsed_stream_with_timestamp \
    .groupBy(
        F.window(F.col("timestamp"), "5 minutes"),
        F.col("DOLocationID")
    ) \
    .count() \
    .orderBy("count", ascending=False)

query = popular_destinations \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()
```

**ANSWER: The most popular DOLocationId is 74**

```bash
# output

-------------------------------------------
Batch: 0
-------------------------------------------
+------------------------------------------+------------+-----+
|window                                    |DOLocationID|count|
+------------------------------------------+------------+-----+
|{2024-04-04 14:27:00, 2024-04-04 14:32:00}|74          |17741|
|{2024-04-04 14:27:00, 2024-04-04 14:32:00}|42          |15942|
|{2024-04-04 14:27:00, 2024-04-04 14:32:00}|41          |14061|
|{2024-04-04 14:27:00, 2024-04-04 14:32:00}|75          |12840|
|{2024-04-04 14:27:00, 2024-04-04 14:32:00}|129         |11930|
|{2024-04-04 14:27:00, 2024-04-04 14:32:00}|7           |11533|
|{2024-04-04 14:27:00, 2024-04-04 14:32:00}|166         |10845|
|{2024-04-04 14:27:00, 2024-04-04 14:32:00}|236         |7913 |
|{2024-04-04 14:27:00, 2024-04-04 14:32:00}|223         |7542 |
|{2024-04-04 14:27:00, 2024-04-04 14:32:00}|238         |7318 |
|{2024-04-04 14:27:00, 2024-04-04 14:32:00}|82          |7292 |
|{2024-04-04 14:27:00, 2024-04-04 14:32:00}|181         |7282 |
|{2024-04-04 14:27:00, 2024-04-04 14:32:00}|95          |7244 |
|{2024-04-04 14:27:00, 2024-04-04 14:32:00}|244         |6733 |
|{2024-04-04 14:27:00, 2024-04-04 14:32:00}|61          |6606 |
|{2024-04-04 14:27:00, 2024-04-04 14:32:00}|116         |6339 |
|{2024-04-04 14:27:00, 2024-04-04 14:32:00}|138         |6144 |
|{2024-04-04 14:27:00, 2024-04-04 14:32:00}|97          |6050 |
|{2024-04-04 14:27:00, 2024-04-04 14:32:00}|49          |5221 |
|{2024-04-04 14:27:00, 2024-04-04 14:32:00}|151         |5153 |
+------------------------------------------+------------+-----+
```
