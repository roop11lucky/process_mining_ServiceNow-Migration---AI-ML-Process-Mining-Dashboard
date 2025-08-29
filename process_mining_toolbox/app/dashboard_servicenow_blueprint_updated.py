import sys, os
from pathlib import Path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
from dotenv import load_dotenv

from src.dfg_graphviz_visualizer import generate_perf_dfg_graphviz
from src.heuristics_miner import generate_heuristics_miner_graph
from src.variant_summary import compute_variant_summary
from src.network_process_map_v2 import generate_network_process_map_v2
from src.heuristics_miner import generate_heuristics_miner_graph  # ✅ NEW IMPORT

# ✅ Load DB credentials from .env
load_dotenv()
PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5433")
PG_DB   = os.getenv("PG_DB")

DB_URI = f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
engine = create_engine(DB_URI)

@st.cache_data
def load_data():
    event_df = pd.read_sql("SELECT * FROM event_log", engine)
    queue_df = pd.read_sql("SELECT * FROM queue_summary", engine)
    return event_df, queue_df

event_df, queue_df = load_data()

# === Streamlit Config ===
st.set_page_config(page_title="ServiceNow Migration Dashboard", layout="wide")
st.title("🟢 ServiceNow Migration - AI/ML Process Mining Dashboard")

# === KPIs ===
st.subheader("📌 Key Metrics")
total_tickets = event_df['ticket_id'].nunique()
sla_met = event_df['sla_met'].sum()
sla_rate = round((sla_met / total_tickets) * 100, 2) if total_tickets > 0 else 0

# ✅ MTTR: avg time from Open to Closed
df_resolved = event_df[event_df['activity'].isin(['Resolved', 'Closed'])]
ticket_start = event_df[event_df['activity'] == 'Open'][['ticket_id', 'timestamp']]
ticket_end = df_resolved.groupby('ticket_id')['timestamp'].max().reset_index()
merged = ticket_end.merge(ticket_start, on='ticket_id', suffixes=('_end', '_start'))
merged['duration'] = (merged['timestamp_end'] - merged['timestamp_start']).dt.total_seconds() / 3600
mttr = round(merged['duration'].mean(), 2)

# ✅ MTTA: avg time from Open to Assigned
assigned = event_df[event_df['activity'] == 'Assigned'][['ticket_id', 'timestamp']]
merged_tta = assigned.merge(ticket_start, on='ticket_id', suffixes=('_assigned', '_open'))
merged_tta['ack_time'] = (merged_tta['timestamp_assigned'] - merged_tta['timestamp_open']).dt.total_seconds() / 60
mtta = round(merged_tta['ack_time'].mean(), 2)

# ✅ Four KPI columns in one row
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Tickets", total_tickets)
col2.metric("SLA Compliance %", f"{sla_rate}%")
col3.metric("MTTR (Hours)", mttr)
col4.metric("MTTA (Minutes)", mtta)

# === Queue Selector for Other Charts (Like Variant Summary) ===
st.subheader("🎯 Queue Selector")
queues = sorted(event_df['queue_id'].unique().tolist())
selected_queues = st.multiselect("Select Queues", queues, default=queues[:3], key="queue_selector_unique")

# === SLA Breach & Category Charts Side-by-Side ===
col1, col2 = st.columns(2)

with col1:
    st.subheader("🔥 SLA Breach Trends by Priority")
    sla_priority = event_df.groupby('priority')['sla_met'].mean().reset_index()
    sla_priority['SLA Compliance %'] = (sla_priority['sla_met'] * 100).round(2)
    fig_sla = px.bar(sla_priority, x='priority', y='SLA Compliance %', color='priority',
                     color_discrete_map={"P1": "red", "P2": "orange", "P3": "green", "P4": "blue"})
    st.plotly_chart(fig_sla, use_container_width=True, key="sla_priority_chart")

with col2:
    st.subheader("📂 Category Distribution")
    category_count = event_df['category'].value_counts().reset_index()
    category_count.columns = ['Category', 'Tickets']
    fig_category = px.pie(category_count, values='Tickets', names='Category', hole=0.4, title="Tickets by Category")
    st.plotly_chart(fig_category, use_container_width=True, key="category_chart")

# === Process Mining DFG (Graphviz Style) ===
st.subheader("🔄 Process Flow - Graphviz")
dfg_path = "app/static/dfg_perf_graphviz.png"
generate_perf_dfg_graphviz(event_df, output_path=dfg_path, mode="avg")
if os.path.exists(dfg_path):
    st.image(dfg_path, caption="ServiceNow-style Performance DFG", use_container_width=True)

# === Heuristics Miner Graph (PNG Output) ===
st.subheader("🧠 Heuristics Miner Process Map")
hm_path = Path("app/static/heuristics_graph.png").resolve()
generate_heuristics_miner_graph(event_df, output_path=str(hm_path))
if hm_path.exists():
    st.image(str(hm_path), caption="Heuristics Miner Graph", use_container_width=True)
else:
    st.error("Unable to generate Heuristics Miner graph.")


# === Variant Summary Table ===
st.subheader("📌 Top Workflow Variants")
variant_df = compute_variant_summary(event_df, selected_queues)
if not variant_df.empty:
    st.dataframe(variant_df.head(10))
else:
    st.info("No variants found for the selected queues.")
