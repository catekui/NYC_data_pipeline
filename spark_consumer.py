from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkTaxiProcessor:
    def __init__(self):
        """Initialize Spark - JARs already in classpath from Docker image"""
        self.spark = SparkSession.builder \
            .appName("TaxiDataProcessor") \
            .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.postgresql:postgresql:42.7.1") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        
        # Kafka configuration from environment
        self.kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self.topic = os.getenv("KAFKA_TOPIC", "nyc_taxi_data")
        
        # PostgreSQL configuration from environment
        db_host = os.getenv("DB_HOST", "postgres")
        db_user = os.getenv("DB_USER", "postgres")
        db_pass = os.getenv("DB_PASSWORD", "Access")
        
        self.pg_url = f"jdbc:postgresql://{db_host}:5432/nyc_taxi_analytics"
        self.pg_props = {
            "user": db_user,
            "password": db_pass,
            "driver": "org.postgresql.Driver"
        }
        
        logger.info("Spark initialized")
    
    def create_stream(self):
        """Read and process Kafka stream"""
        schema = StructType([
            StructField("VendorID", IntegerType()),
            StructField("tpep_pickup_datetime", StringType()),
            StructField("tpep_dropoff_datetime", StringType()),
            StructField("passenger_count", IntegerType()),
            StructField("trip_distance", DoubleType()),
            StructField("fare_amount", DoubleType()),
            StructField("tip_amount", DoubleType()),
            StructField("total_amount", DoubleType())
        ])
        
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_servers) \
            .option("subscribe", self.topic) \
            .option("startingOffsets", "earliest") \
            .load()
        
        # Parse and enrich
        df = df.select(from_json(col("value").cast("string"), schema).alias("data")) \
               .select("data.*") \
               .withColumn("pickup_dt", to_timestamp("tpep_pickup_datetime")) \
               .withColumn("dropoff_dt", to_timestamp("tpep_dropoff_datetime")) \
               .withColumn("duration_min", 
                          (unix_timestamp("dropoff_dt") - unix_timestamp("pickup_dt")) / 60) \
               .withColumn("speed_mph", 
                          when((col("duration_min") > 0) & (col("trip_distance") > 0),
                               col("trip_distance") / (col("duration_min") / 60)).otherwise(0)) \
               .withColumn("pickup_hour", hour("pickup_dt")) \
               .withColumn("trip_category",
                          when(col("trip_distance") <= 2, "short")
                          .when(col("trip_distance") <= 10, "medium").otherwise("long")) \
               .withColumn("tip_pct",
                          when(col("fare_amount") > 0, 
                               (col("tip_amount") / col("fare_amount") * 100)).otherwise(0)) \
               .filter((col("trip_distance") >= 0) & (col("fare_amount") >= 0) &
                       (col("duration_min") > 0) & (col("duration_min") < 300))
        
        return df
    
    def process_batch(self, batch_df, batch_id):
        """Process each micro-batch"""
        try:
            # Use a different variable name to avoid shadowing the count() function
            record_count = batch_df.count()
            if record_count == 0:
                return
            
            logger.info(f"Batch {batch_id}: {record_count} records")
            
            # Save to PostgreSQL
            batch_df.select(
                col("VendorID").alias("vendor_id"),
                col("pickup_dt").alias("pickup_datetime"),
                col("dropoff_dt").alias("dropoff_datetime"),
                col("passenger_count"),
                col("trip_distance"),
                col("fare_amount"),
                col("tip_amount"),
                col("total_amount"),
                col("duration_min").alias("trip_duration_minutes"),
                col("pickup_hour"),
                col("trip_category"),
                col("tip_pct").alias("tip_percentage")
            ).write.jdbc(self.pg_url, "taxi_trips", mode="append", properties=self.pg_props)
            
            # Analytics - now count() function works properly
            # batch_df.agg(
            #     count("*").alias("total_trips"),
            #     avg("fare_amount").alias("avg_fare"),
            #     avg("trip_distance").alias("avg_distance"),
            #     sum("total_amount").alias("total_revenue")
            # ).withColumn("analysis_date", current_date()).write.jdbc(
            #     self.pg_url, "analytics_summary", mode="append", properties=self.pg_props)
            
            # logger.info(f"✅ Batch {batch_id} saved")
            
        except Exception as e:
            logger.error(f"Error batch {batch_id}: {e}")
    
    def start(self):
        """Start streaming"""
        logger.info("Starting streaming...")
        
        stream = self.create_stream()
        
        query = stream.writeStream \
            .foreachBatch(self.process_batch) \
            .option("checkpointLocation", "/tmp/checkpoints") \
            .trigger(processingTime='10 seconds') \
            .start()
        
        logger.info("Processing... Ctrl+C to stop")
        
        try:
            query.awaitTermination()
        except KeyboardInterrupt:
            query.stop()
            self.spark.stop()

if __name__ == "__main__":
    SparkTaxiProcessor().start()