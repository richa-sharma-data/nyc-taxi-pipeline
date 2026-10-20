import streamlit as st
import redshift_connector
import pandas as pd
import plotly.express as px
import os
from dotenv import load_dotenv

load_dotenv()

# ── Page config ──────────────────────────────────────────
st.set_page_config(
    page_title="NYC Taxi Analytics",
    page_icon="🚕",
    layout="wide"
)

# ── Redshift connection ───────────────────────────────────
@st.cache_resource(ttl=600) # Fresh connection at least every 10 mins, avoids stale paused-warehouse connections
def get_connection():
    return redshift_connector.connect(
        host=os.environ['REDSHIFT_HOST'],
        database=os.environ['REDSHIFT_DB'],
        user=os.environ['REDSHIFT_USER'],
        password=os.environ['REDSHIFT_PASSWORD'],
        port=int(os.environ['REDSHIFT_PORT']),
	timeout=30 #fail fast instead of hanging forever on a paused warehouse 
    )

def _run_query(query: str) -> pd.DataFrame:
    conn   = get_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    rows    = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    return pd.DataFrame(rows, columns=columns)

@st.cache_data(ttl=3600)   # cache query results for 1 hour
def load_data(query: str) -> pd.DataFrame:
    try:
        return _run_query(query)
    except Exception:
        get_connection.clear()  # drop the possibly stale connection
        return _run_query(query)  # retry once on a fresh connection


# ── Load data ─────────────────────────────────────────────

try:
    df = load_data("""
        SELECT
            trip_date,
            total_trips,
            total_revenue,
            avg_fare,
            avg_distance_miles,
            avg_duration_mins,
            avg_tip
        FROM taxi_marts.mart_daily_trips
        ORDER BY trip_date
    """)
except Exception:
    st.title("🚕 NYC Taxi Analytics Dashboard")
    st.warning(
        "Data is temporarily unavailable while the warehouse is paused "
        "for cost control. Please check back in a little while."
    )
    st.stop()

df['trip_date'] = pd.to_datetime(df['trip_date'])

# ── Header ────────────────────────────────────────────────
st.title("🚕 NYC Taxi Analytics Dashboard")
st.caption("Data sourced from NYC TLC · Transformed with dbt · Hosted on Streamlit Cloud")

st.divider()

# ── Date filter ───────────────────────────────────────────
col_l, col_r = st.columns(2)
with col_l:
    start_date = st.date_input("From", df['trip_date'].min())
with col_r:
    end_date   = st.date_input("To",   df['trip_date'].max())

filtered = df[
    (df['trip_date'] >= pd.to_datetime(start_date)) &
    (df['trip_date'] <= pd.to_datetime(end_date))
]

st.divider()

# ── KPI tiles ─────────────────────────────────────────────
k1, k2, k3, k4 = st.columns(4)

k1.metric("Total Trips",       f"{filtered['total_trips'].sum():,.0f}")
k2.metric("Total Revenue",     f"${filtered['total_revenue'].sum():,.0f}")
k3.metric("Avg Fare",          f"${filtered['avg_fare'].mean():.2f}")
k4.metric("Avg Distance (mi)", f"{filtered['avg_distance_miles'].mean():.2f}")

st.divider()

# ── Charts row 1 ──────────────────────────────────────────
c1, c2 = st.columns(2)

with c1:
    st.subheader("Daily Trip Volume")
    fig = px.line(
        filtered, x='trip_date', y='total_trips',
        labels={'trip_date': 'Date', 'total_trips': 'Trips'},
        color_discrete_sequence=['#f5c542']
    )
    st.plotly_chart(fig, use_container_width=True)

with c2:
    st.subheader("Daily Revenue")
    fig = px.bar(
        filtered, x='trip_date', y='total_revenue',
        labels={'trip_date': 'Date', 'total_revenue': 'Revenue (USD)'},
        color_discrete_sequence=['#1f77b4']
    )
    st.plotly_chart(fig, use_container_width=True)

# ── Charts row 2 ──────────────────────────────────────────
c3, c4 = st.columns(2)

with c3:
    st.subheader("Avg Fare vs Avg Tip")
    fig = px.line(
        filtered, x='trip_date', y=['avg_fare', 'avg_tip'],
        labels={'trip_date': 'Date', 'value': 'USD'},
        color_discrete_sequence=['#2ca02c', '#ff7f0e']
    )
    st.plotly_chart(fig, use_container_width=True)

with c4:
    st.subheader("Avg Trip Duration (mins)")
    fig = px.area(
        filtered, x='trip_date', y='avg_duration_mins',
        labels={'trip_date': 'Date', 'avg_duration_mins': 'Minutes'},
        color_discrete_sequence=['#9467bd']
    )
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Raw data table ────────────────────────────────────────
with st.expander("View Raw Data"):
    st.dataframe(filtered, use_container_width=True)
