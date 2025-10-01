import streamlit as st
import psycopg2
import pandas as pd
from psycopg2.extras import RealDictCursor
import os

# Database connection
def get_connection():
    """Create database connection with environment variable support"""
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        database=os.getenv("DB_NAME", "nyc_taxi_analytics"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "Access"),
        port=int(os.getenv("DB_PORT", "5432"))
    )

# Load available date range from DB
def get_date_range():
    """Get min and max dates from the database"""
    query = "SELECT MIN(pickup_datetime) as min_date, MAX(pickup_datetime) as max_date FROM taxi_trips;"
    try:
        with get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query)
                return cur.fetchone()
    except psycopg2.OperationalError as e:
        st.error(f"Database connection failed: {e}")
        st.info("Please check that PostgreSQL is running and accessible.")
        return None
    except Exception as e:
        st.error(f"Error fetching date range: {e}")
        return None

# Load trip stats
def load_trip_stats(start_date, end_date):
    """Load aggregated trip statistics for date range"""
    query = """
        SELECT 
            COUNT(*) as total_trips,
            AVG(fare_amount) as avg_fare,
            AVG(trip_distance) as avg_distance,
            AVG(trip_duration_minutes) as avg_duration,
            AVG(tip_percentage) as avg_tip,
            SUM(total_amount) as total_revenue,
            AVG(passenger_count) as avg_passengers
        FROM taxi_trips
        WHERE pickup_datetime BETWEEN %s AND %s;
    """
    try:
        with get_connection() as conn:
            return pd.read_sql(query, conn, params=(start_date, end_date))
    except Exception as e:
        st.error(f"Error loading trip stats: {e}")
        return pd.DataFrame()

# Streamlit UI
st.set_page_config(page_title="NYC Taxi Analytics Dashboard", layout="wide")
st.title("🚖 NYC Taxi Analytics Dashboard")

# Get min/max available dates
date_range = get_date_range()

if not date_range or not date_range["min_date"]:
    st.error("No data available in taxi_trips table.")
    st.info("""
    **Troubleshooting:**
    - Ensure PostgreSQL container is running: `docker-compose ps`
    - Check if data has been loaded into the database
    - View logs: `docker-compose logs streamlit`
    """)
else:
    min_date = date_range["min_date"].date() if hasattr(date_range["min_date"], 'date') else date_range["min_date"]
    max_date = date_range["max_date"].date() if hasattr(date_range["max_date"], 'date') else date_range["max_date"]

    # Sidebar date picker
    st.sidebar.header("Filters")
    start_date = st.sidebar.date_input("Start Date", min_date, min_value=min_date, max_value=max_date)
    end_date = st.sidebar.date_input("End Date", max_date, min_value=min_date, max_value=max_date)

    # Ensure valid range
    if start_date > end_date:
        st.error("Start date must be before end date.")
    else:
        # Load stats
        with st.spinner("Loading data..."):
            stats = load_trip_stats(start_date, end_date)

        if stats.empty or stats.iloc[0]["total_trips"] == 0:
            st.warning("No data available for the selected period.")
        else:
            st.subheader("📈 Key Performance Indicators")
            
            # First row of metrics
            col1, col2, col3 = st.columns(3)
            col1.metric("🚖 Total Trips", f"{int(stats['total_trips'][0]):,}")
            col2.metric("💰 Average Fare", f"${stats['avg_fare'][0]:.2f}")
            col3.metric("📏 Avg Distance", f"{stats['avg_distance'][0]:.2f} mi")

            # Second row of metrics
            col4, col5, col6 = st.columns(3)
            col4.metric("💵 Total Revenue", f"${stats['total_revenue'][0]:,.2f}")
            col5.metric("⏱️ Avg Duration", f"{stats['avg_duration'][0]:.2f} min")
            col6.metric("💸 Avg Tip %", f"{stats['avg_tip'][0]:.2f}%")
            
            # Optional: Show raw data
            with st.expander("📊 View Raw Statistics"):
                st.dataframe(stats)