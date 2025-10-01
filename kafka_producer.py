import json
import time
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NYCTaxiProducer:
    def __init__(self, bootstrap_servers=None, topic='nyc_taxi_data'):
        # Use environment variable or default
        if bootstrap_servers is None:
            bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092').split(',')
        
        self.topic = topic
        
        # Initialize Kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: str(k).encode('utf-8') if k else None,
            # Add some reliability configs
            acks='all',  # Wait for all replicas to acknowledge
            retries=3,   # Retry failed sends
            retry_backoff_ms=1000
        )
        
        logger.info(f"Kafka producer initialized for topic: {topic}")
    
    def load_taxi_data(self, file_path):
        """Load NYC taxi data from CSV or Parquet file"""
        try:
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_path.endswith('.parquet'):
                df = pd.read_parquet(file_path)
            else:
                raise ValueError("File must be CSV or Parquet format")
            
            logger.info(f"Loaded {len(df)} records from {file_path}")
            return df
        
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            return None
    
    def prepare_message(self, row):
        """Convert pandas row to Kafka message format"""
        # Convert row to dictionary and handle NaN values
        message = row.to_dict()
        
        # Replace NaN with None (which becomes null in JSON)
        for key, value in message.items():
            if pd.isna(value):
                message[key] = None
        
        # Add metadata
        message['timestamp'] = datetime.now().isoformat()
        message['source'] = 'nyc_taxi_dataset'
        
        return message
    
    def send_batch(self, df, batch_size=100, delay_seconds=1):
        """Send taxi data in batches to Kafka"""
        total_records = len(df)
        sent_count = 0
        failed_count = 0
        
        logger.info(f"Starting to send {total_records} records in batches of {batch_size}")
        
        for i in range(0, total_records, batch_size):
            batch = df.iloc[i:i + batch_size]
            batch_failed = 0
            
            for _, row in batch.iterrows():
                try:
                    # Prepare message
                    message = self.prepare_message(row)
                    
                    # Create partition key based on available location data
                    if 'PULocationID' in message and message['PULocationID'] is not None:
                        # Newer datasets with location IDs
                        key = str(message.get('PULocationID', 'unknown'))
                    elif 'pickup_longitude' in message and 'pickup_latitude' in message:
                        # Older datasets with coordinates
                        pickup_lon = message.get('pickup_longitude', 0)
                        pickup_lat = message.get('pickup_latitude', 0)
                        key = str(hash(f"{pickup_lon}_{pickup_lat}") % 1000)
                    else:
                        # Fallback key
                        key = str(hash(str(message.get('VendorID', 1))) % 100)
                    
                    # Send to Kafka
                    future = self.producer.send(self.topic, key=key, value=message)
                    sent_count += 1
                    
                except Exception as e:
                    logger.error(f"Error sending message: {e}")
                    failed_count += 1
                    batch_failed += 1
            
            # Log progress
            batch_sent = len(batch) - batch_failed
            logger.info(f"Sent batch {i//batch_size + 1}: {batch_sent} sent, {batch_failed} failed | Total: {sent_count}/{total_records}")
            
            # Wait between batches to simulate real-time streaming
            time.sleep(delay_seconds)
        
        # Ensure all messages are sent
        self.producer.flush()
        logger.info(f"Batch processing complete: {sent_count} sent, {failed_count} failed out of {total_records} total records")
    
    def send_single_record(self, record):
        """Send a single record to Kafka"""
        try:
            message = self.prepare_message(record)
            
            # Create partition key
            if 'PULocationID' in message and message['PULocationID'] is not None:
                key = str(message.get('PULocationID', 'unknown'))
            elif 'pickup_longitude' in message and 'pickup_latitude' in message:
                pickup_lon = message.get('pickup_longitude', 0)
                pickup_lat = message.get('pickup_latitude', 0)
                key = str(hash(f"{pickup_lon}_{pickup_lat}") % 1000)
            else:
                key = str(hash(str(message.get('VendorID', 1))) % 100)
            
            future = self.producer.send(self.topic, key=key, value=message)
            self.producer.flush()
            
            logger.info("Single record sent successfully")
            return True
        
        except Exception as e:
            logger.error(f"Error sending single record: {e}")
            return False
    
    def send_continuous_stream(self, df, records_per_second=10):
        """Send data in continuous stream mode"""
        total_records = len(df)
        delay_between_records = 1.0 / records_per_second
        sent_count = 0
        
        logger.info(f"Starting continuous stream: {records_per_second} records/second")
        
        try:
            for _, row in df.iterrows():
                try:
                    message = self.prepare_message(row)
                    
                    # Create partition key
                    if 'PULocationID' in message and message['PULocationID'] is not None:
                        key = str(message.get('PULocationID', 'unknown'))
                    elif 'pickup_longitude' in message and 'pickup_latitude' in message:
                        pickup_lon = message.get('pickup_longitude', 0)
                        pickup_lat = message.get('pickup_latitude', 0)
                        key = str(hash(f"{pickup_lon}_{pickup_lat}") % 1000)
                    else:
                        key = str(hash(str(message.get('VendorID', 1))) % 100)
                    
                    # Send to Kafka
                    self.producer.send(self.topic, key=key, value=message)
                    sent_count += 1
                    
                    # Log progress every 100 records
                    if sent_count % 100 == 0:
                        logger.info(f"Streamed {sent_count}/{total_records} records")
                    
                    # Wait to maintain desired rate
                    time.sleep(delay_between_records)
                    
                except KeyboardInterrupt:
                    logger.info("Stream interrupted by user")
                    break
                except Exception as e:
                    logger.error(f"Error in continuous stream: {e}")
            
            self.producer.flush()
            logger.info(f"Continuous stream complete: {sent_count} records sent")
            
        except KeyboardInterrupt:
            logger.info("Continuous stream stopped by user")
            self.producer.flush()
    
    def get_data_info(self, df):
        """Display information about the dataset"""
        logger.info("=== Dataset Information ===")
        logger.info(f"Total records: {len(df):,}")
        logger.info(f"Columns: {list(df.columns)}")
        logger.info(f"Date range: {df.iloc[0].get('tpep_pickup_datetime', 'N/A')} to {df.iloc[-1].get('tpep_pickup_datetime', 'N/A')}")
        logger.info(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MB")
        
        # Show sample of data
        logger.info("=== Sample Records ===")
        for i in range(min(3, len(df))):
            sample_msg = self.prepare_message(df.iloc[i])
            logger.info(f"Record {i+1}: {dict(list(sample_msg.items())[:5])}...")  # First 5 fields
    
    def close(self):
        """Close the Kafka producer"""
        self.producer.close()
        logger.info("Kafka producer closed")

# Main execution function
def main():
    # Configuration - read from environment
    KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'nyc_taxi_data')
    DATA_FILE_PATH = os.getenv('DATA_FILE_PATH', 'yellow_tripdata_2015-01.csv')
    
    # Initialize producer (will use environment variable)
    producer = NYCTaxiProducer(topic=KAFKA_TOPIC)
    
    try:
        # Load data
        logger.info("Loading taxi data...")
        df = producer.load_taxi_data(DATA_FILE_PATH)
        
        if df is not None:
            # Show dataset information
            producer.get_data_info(df)
            
            # Send test batch
            logger.info("Starting test batch (first 100 records)...")
            test_df = df.head(100)
            producer.send_batch(test_df, batch_size=10, delay_seconds=1)

           # Send first 10,000 records
            # logger.info("Starting batch (first 10,000 records)...")
            # test_df = df.head(10000)
            # producer.send_batch(test_df, batch_size=100, delay_seconds=0.1)
            
        else:
            logger.error("Failed to load data")
    
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Error in main execution: {e}")
    
    finally:
        # Clean up
        producer.close()

if __name__ == "__main__":
    main()