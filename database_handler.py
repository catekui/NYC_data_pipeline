import psycopg2
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool
import pandas as pd
import logging
from datetime import datetime
import json
from typing import List, Dict, Any, Optional
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NYCTaxiDatabaseHandler:
    def __init__(self, 
                 host='postgres', 
                 database='nyc_taxi_analytics',
                 user='postgres', 
                 password='Access',
                 port=5432,
                 min_connections=1,
                 max_connections=20):
        """
        Initialize database connection pool
        """
        self.connection_params = {
            'host': host,
            'database': database,
            'user': user,
            'password': password,
            'port': port
        }
        
        try:
            # Create connection pool
            self.connection_pool = SimpleConnectionPool(
                min_connections,
                max_connections,
                **self.connection_params
            )
            
            logger.info(f"Database connection pool created successfully")
            logger.info(f"Connected to database: {database} at {host}:{port}")
            
            # Initialize database schema
            self.init_database_schema()
            
        except Exception as e:
            logger.error(f"Failed to initialize database: {e}")
            raise
    
    def get_connection(self):
        """Get connection from pool"""
        return self.connection_pool.getconn()
    
    def return_connection(self, connection):
        """Return connection to pool"""
        self.connection_pool.putconn(connection)
    
    def init_database_schema(self):
        """Create database tables if they don't exist"""
        connection = None
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            
            # Drop the old table if it exists (to match the new schema)
            logger.info("Checking for existing tables...")
            
            # Create raw taxi trips table with updated schema
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS taxi_trips (
                    id SERIAL PRIMARY KEY,
                    vendor_id INTEGER,
                    pickup_datetime TIMESTAMP,
                    dropoff_datetime TIMESTAMP,
                    passenger_count INTEGER,
                    trip_distance DECIMAL(10,2),
                    pickup_longitude DECIMAL(10,6),
                    pickup_latitude DECIMAL(10,6),
                    dropoff_longitude DECIMAL(10,6),
                    dropoff_latitude DECIMAL(10,6),
                    rate_code_id INTEGER,
                    store_and_fwd_flag CHAR(1),
                    payment_type INTEGER,
                    fare_amount DECIMAL(10,2),
                    extra DECIMAL(10,2),
                    mta_tax DECIMAL(10,2),
                    tip_amount DECIMAL(10,2),
                    tolls_amount DECIMAL(10,2),
                    improvement_surcharge DECIMAL(10,2),
                    total_amount DECIMAL(10,2),
                    trip_duration_minutes DECIMAL(10,2),
                    average_speed DECIMAL(10,2),
                    pickup_hour INTEGER,
                    pickup_day_of_week INTEGER,
                    trip_category VARCHAR(20),
                    tip_percentage DECIMAL(5,2),
                    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    kafka_offset BIGINT,
                    kafka_partition INTEGER
                )
            """)
            logger.info("✅ Table 'taxi_trips' created or already exists")
            
            # Create aggregated analytics table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS taxi_analytics_summary (
                    id SERIAL PRIMARY KEY,
                    analysis_date DATE,
                    analysis_hour INTEGER,
                    total_trips INTEGER,
                    avg_fare_amount DECIMAL(10,2),
                    avg_trip_distance DECIMAL(10,2),
                    avg_trip_duration DECIMAL(10,2),
                    avg_speed DECIMAL(10,2),
                    avg_tip_percentage DECIMAL(5,2),
                    total_revenue DECIMAL(12,2),
                    vendor_1_trips INTEGER,
                    vendor_2_trips INTEGER,
                    short_trips INTEGER,
                    medium_trips INTEGER,
                    long_trips INTEGER,
                    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            logger.info("✅ Table 'taxi_analytics_summary' created or already exists")
            
            # Create vendor performance table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS vendor_performance (
                    id SERIAL PRIMARY KEY,
                    vendor_id INTEGER,
                    analysis_date DATE,
                    total_trips INTEGER,
                    avg_fare DECIMAL(10,2),
                    avg_distance DECIMAL(10,2),
                    avg_duration DECIMAL(10,2),
                    total_revenue DECIMAL(12,2),
                    avg_tip_percentage DECIMAL(5,2),
                    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            logger.info("✅ Table 'vendor_performance' created or already exists")
            
            # Create hourly statistics table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS hourly_statistics (
                    id SERIAL PRIMARY KEY,
                    hour_of_day INTEGER,
                    analysis_date DATE,
                    trip_count INTEGER,
                    avg_fare DECIMAL(10,2),
                    avg_distance DECIMAL(10,2),
                    total_revenue DECIMAL(12,2),
                    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            logger.info("✅ Table 'hourly_statistics' created or already exists")
            
            # Create indexes for better performance
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_taxi_trips_pickup_datetime 
                ON taxi_trips(pickup_datetime)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_taxi_trips_vendor_id 
                ON taxi_trips(vendor_id)
            """)
            
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_taxi_trips_pickup_hour 
                ON taxi_trips(pickup_hour)
            """)
            
            logger.info("✅ Indexes created or already exist")
            
            connection.commit()
            logger.info("🎉 Database schema initialized successfully")
            
        except Exception as e:
            logger.error(f"❌ Error initializing database schema: {e}")
            if connection:
                connection.rollback()
            raise
        finally:
            if connection:
                cursor.close()
                self.return_connection(connection)
    
    def insert_taxi_trips(self, trips_data: List[Dict[str, Any]]) -> bool:
        """Insert taxi trip records into database"""
        connection = None
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            
            insert_query = """
                INSERT INTO taxi_trips (
                    vendor_id, pickup_datetime, dropoff_datetime, passenger_count,
                    trip_distance, pickup_longitude, pickup_latitude, 
                    dropoff_longitude, dropoff_latitude, rate_code_id,
                    store_and_fwd_flag, payment_type, fare_amount, extra,
                    mta_tax, tip_amount, tolls_amount, improvement_surcharge,
                    total_amount, trip_duration_minutes, average_speed,
                    pickup_hour, pickup_day_of_week, trip_category,
                    tip_percentage, kafka_offset, kafka_partition
                ) VALUES %s
            """
            
            # Prepare data for insertion
            values_list = []
            for trip in trips_data:
                values = (
                    trip.get('VendorID'),
                    trip.get('tpep_pickup_datetime'),
                    trip.get('tpep_dropoff_datetime'),
                    trip.get('passenger_count'),
                    trip.get('trip_distance'),
                    trip.get('pickup_longitude'),
                    trip.get('pickup_latitude'),
                    trip.get('dropoff_longitude'),
                    trip.get('dropoff_latitude'),
                    trip.get('RateCodeID'),
                    trip.get('store_and_fwd_flag'),
                    trip.get('payment_type'),
                    trip.get('fare_amount'),
                    trip.get('extra'),
                    trip.get('mta_tax'),
                    trip.get('tip_amount'),
                    trip.get('tolls_amount'),
                    trip.get('improvement_surcharge'),
                    trip.get('total_amount'),
                    trip.get('trip_duration_minutes'),
                    trip.get('average_speed'),
                    trip.get('pickup_hour'),
                    trip.get('pickup_day_of_week'),
                    trip.get('trip_category'),
                    trip.get('tip_percentage'),
                    trip.get('kafka_offset'),
                    trip.get('kafka_partition')
                )
                values_list.append(values)
            
            # Execute batch insert
            psycopg2.extras.execute_values(
                cursor, insert_query, values_list, template=None, page_size=100
            )
            
            connection.commit()
            logger.info(f"✅ Successfully inserted {len(trips_data)} taxi trip records")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error inserting taxi trips: {e}")
            if connection:
                connection.rollback()
            return False
        finally:
            if connection:
                cursor.close()
                self.return_connection(connection)
    
    def insert_analytics_summary(self, analytics_data: Dict[str, Any]) -> bool:
        """Insert aggregated analytics summary"""
        connection = None
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            
            insert_query = """
                INSERT INTO taxi_analytics_summary (
                    analysis_date, analysis_hour, total_trips, avg_fare_amount,
                    avg_trip_distance, avg_trip_duration, avg_speed,
                    avg_tip_percentage, total_revenue, vendor_1_trips,
                    vendor_2_trips, short_trips, medium_trips, long_trips
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            values = (
                analytics_data.get('analysis_date'),
                analytics_data.get('analysis_hour'),
                analytics_data.get('total_trips'),
                analytics_data.get('avg_fare_amount'),
                analytics_data.get('avg_trip_distance'),
                analytics_data.get('avg_trip_duration'),
                analytics_data.get('avg_speed'),
                analytics_data.get('avg_tip_percentage'),
                analytics_data.get('total_revenue'),
                analytics_data.get('vendor_1_trips'),
                analytics_data.get('vendor_2_trips'),
                analytics_data.get('short_trips'),
                analytics_data.get('medium_trips'),
                analytics_data.get('long_trips')
            )
            
            cursor.execute(insert_query, values)
            connection.commit()
            
            logger.info("✅ Analytics summary inserted successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error inserting analytics summary: {e}")
            if connection:
                connection.rollback()
            return False
        finally:
            if connection:
                cursor.close()
                self.return_connection(connection)
    
    def insert_vendor_performance(self, vendor_data: List[Dict[str, Any]]) -> bool:
        """Insert vendor performance data"""
        connection = None
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            
            insert_query = """
                INSERT INTO vendor_performance (
                    vendor_id, analysis_date, total_trips, avg_fare,
                    avg_distance, avg_duration, total_revenue, avg_tip_percentage
                ) VALUES %s
            """
            
            values_list = []
            for vendor in vendor_data:
                values = (
                    vendor.get('vendor_id'),
                    vendor.get('analysis_date'),
                    vendor.get('total_trips'),
                    vendor.get('avg_fare'),
                    vendor.get('avg_distance'),
                    vendor.get('avg_duration'),
                    vendor.get('total_revenue'),
                    vendor.get('avg_tip_percentage')
                )
                values_list.append(values)
            
            psycopg2.extras.execute_values(
                cursor, insert_query, values_list, template=None, page_size=100
            )
            
            connection.commit()
            logger.info(f"✅ Vendor performance data inserted for {len(vendor_data)} vendors")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error inserting vendor performance: {e}")
            if connection:
                connection.rollback()
            return False
        finally:
            if connection:
                cursor.close()
                self.return_connection(connection)
    
    def insert_hourly_statistics(self, hourly_data: List[Dict[str, Any]]) -> bool:
        """Insert hourly statistics"""
        connection = None
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            
            insert_query = """
                INSERT INTO hourly_statistics (
                    hour_of_day, analysis_date, trip_count, avg_fare,
                    avg_distance, total_revenue
                ) VALUES %s
            """
            
            values_list = []
            for hour_stat in hourly_data:
                values = (
                    hour_stat.get('hour_of_day'),
                    hour_stat.get('analysis_date'),
                    hour_stat.get('trip_count'),
                    hour_stat.get('avg_fare'),
                    hour_stat.get('avg_distance'),
                    hour_stat.get('total_revenue')
                )
                values_list.append(values)
            
            psycopg2.extras.execute_values(
                cursor, insert_query, values_list, template=None, page_size=100
            )
            
            connection.commit()
            logger.info(f"✅ Hourly statistics inserted for {len(hourly_data)} hours")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error inserting hourly statistics: {e}")
            if connection:
                connection.rollback()
            return False
        finally:
            if connection:
                cursor.close()
                self.return_connection(connection)
    
    def get_trip_statistics(self, start_date: str = None, end_date: str = None) -> Optional[Dict]:
        """Get trip statistics from database"""
        connection = None
        try:
            connection = self.get_connection()
            cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            query = """
                SELECT 
                    COUNT(*) as total_trips,
                    AVG(fare_amount) as avg_fare,
                    AVG(trip_distance) as avg_distance,
                    AVG(trip_duration_minutes) as avg_duration,
                    AVG(tip_percentage) as avg_tip_percentage,
                    SUM(total_amount) as total_revenue
                FROM taxi_trips
                WHERE 1=1
            """
            
            params = []
            if start_date:
                query += " AND pickup_datetime >= %s"
                params.append(start_date)
            if end_date:
                query += " AND pickup_datetime <= %s"
                params.append(end_date)
            
            cursor.execute(query, params)
            result = cursor.fetchone()
            
            return dict(result) if result else None
            
        except Exception as e:
            logger.error(f"❌ Error getting trip statistics: {e}")
            return None
        finally:
            if connection:
                cursor.close()
                self.return_connection(connection)
    
    def get_vendor_comparison(self) -> Optional[List[Dict]]:
        """Get vendor comparison data"""
        connection = None
        try:
            connection = self.get_connection()
            cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            query = """
                SELECT 
                    vendor_id,
                    COUNT(*) as total_trips,
                    AVG(fare_amount) as avg_fare,
                    AVG(trip_distance) as avg_distance,
                    SUM(total_amount) as total_revenue,
                    AVG(tip_percentage) as avg_tip_percentage
                FROM taxi_trips
                GROUP BY vendor_id
                ORDER BY vendor_id
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            
            return [dict(row) for row in results] if results else []
            
        except Exception as e:
            logger.error(f"❌ Error getting vendor comparison: {e}")
            return []
        finally:
            if connection:
                cursor.close()
                self.return_connection(connection)
    
    def close_all_connections(self):
        """Close all database connections"""
        try:
            self.connection_pool.closeall()
            logger.info("All database connections closed")
        except Exception as e:
            logger.error(f"Error closing connections: {e}")

# Test function
def test_database_connection():
    """Test database connection and basic operations"""
    try:
        # Initialize database handler with correct credentials
        db_handler = NYCTaxiDatabaseHandler(
            host='postgres',
            database='nyc_taxi_analytics',
            user='postgres',
            password='Access',
            port=5432
        )
        
        # Test connection
        logger.info("✅ Database connection successful!")
        
        # Test getting statistics (should be empty initially)
        stats = db_handler.get_trip_statistics()
        logger.info(f"Current trip statistics: {stats}")
        
        # Close connections
        db_handler.close_all_connections()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        return False

if __name__ == "__main__":
    # Run connection test
    test_database_connection()