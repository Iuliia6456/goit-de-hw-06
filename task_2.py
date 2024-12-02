from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import os
os.environ['HADOOP_HOME'] = 'C:\\hadoop'
os.environ['PATH'] += ';C:\\hadoop\\bin'
from pyspark.sql import SparkSession
from configs import kafka_config

# Configure Spark to read from Kafka
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

# Create Spark session
spark = SparkSession.builder \
    .appName("KafkaSensorDataProcessingWithAlerts") \
    .master("local[*]") \
    .getOrCreate()

# Define the schema for sensor data
sensor_schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True)
])

# Read the data stream from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("subscribe", "iuliia_building_sensors") \
    .option("startingOffsets", "latest") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
    .load()

# Parse Kafka value as JSON
parsed_df = df.selectExpr("CAST(value AS STRING) as json_value") \
    .withColumn("data", from_json(col("json_value"), sensor_schema)) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

print("parsed_df: ", parsed_df)
parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Define the watermark and sliding window
aggregated_df = parsed_df \
    .withWatermark("event_time", "10 seconds") \
    .groupBy(
        window(col("event_time"), "1 minute", "30 seconds")
    ) \
    .agg(
        round(avg("temperature"), 2).alias("avg_temperature"),
        round(avg("humidity"), 2).alias("avg_humidity")
    )

windowed_df = aggregated_df \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .start()

alert_rules_path = r"C:\GoIT\Data_Engineering\goit-de-hw-06\alerts_conditions.csv"
alert_rules_df = spark.read.csv(
    alert_rules_path,
    header=True,
    schema=StructType([
        StructField("id", IntegerType(), True),
        StructField("humidity_min", DoubleType(), True),
        StructField("humidity_max", DoubleType(), True),
        StructField("temperature_min", DoubleType(), True),
        StructField("temperature_max", DoubleType(), True),
        StructField("code", IntegerType(), True),
        StructField("message", StringType(), True)
    ])
)

cross_join_df = aggregated_df.crossJoin(alert_rules_df)

alerts_filtered_df = cross_join_df.filter(
    (
        (col("humidity_min") != -999) &
        (col("humidity_max") != -999) &
        (col("avg_humidity") >= col("humidity_min")) &
        (col("avg_humidity") <= col("humidity_max"))
    ) |
    (
        (col("temperature_min") != -999) &
        (col("temperature_max") != -999) &
        (col("avg_temperature") >= col("temperature_min")) &
        (col("avg_temperature") <= col("temperature_max"))
    )
)

# alert DataFrame
alerts_prepared_df = alerts_filtered_df.select(
    struct(
        date_format(col("window.start"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("start"),
        date_format(col("window.end"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("end")
    ).alias("window"),
    col("avg_temperature"),
    col("avg_humidity"),
    col("code"),
    col("message"),
    date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("timestamp")
)

# Convert the alert struct to JSON
alerts_json_df = alerts_prepared_df.select(
    to_json(struct(
        "window",
        "avg_temperature",
        "avg_humidity",
        "code",
        "message",
        "timestamp"
    )).alias("value")
)

alerts_console_query = (
    alerts_json_df.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .option("checkpointLocation", "C:/GoIT/Data_Engineering/goit-de-hw-06/checkpoints/alerts_console")
    .start()
)


# Write alerts to Kafka
alerts_checkpoint_path = r"C:\GoIT\Data_Engineering\goit-de-hw-06\checkpoints\alerts"
alerts_query = (
    alerts_json_df.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0])
    .option("kafka.security.protocol", kafka_config['security_protocol'])
    .option("kafka.sasl.mechanism", kafka_config['sasl_mechanism'])
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";')
    .option("topic", "iuliia_building_sensors_alerts")
    .option("checkpointLocation", alerts_checkpoint_path)
    .start()
)

spark.streams.awaitAnyTermination()

