import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
from datetime import timedelta
from dotenv import load_dotenv

from src.dfg_perf_visualizer import generate_perf_dfg
from src.dfg_graphviz_visualizer import generate_perf_dfg_graphviz
from src.variant_analysis import discover_variants
from src.sequence_flow import generate_sequence_flow   # ✅ enhanced version

# === Load DB Connection from .env ===
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

col1, col2, col3 = st.columns(3)
col1.metric("Total Tickets", total_tickets)
col2.metric("SLA Compliance %", f"{sla_rate}%")
col3.metric("MTTR (Hours)", mttr)

col4, _ = st.columns([1, 3])
col4.metric("MTTA (Minutes)", mtta)

# === SLA Breach by Priority ===
st.subheader("🔥 SLA Breach Trends by Priority")
sla_priority = event_df.groupby('priority')['sla_met'].mean().reset_index()
sla_priority['SLA Compliance %'] = (sla_priority['sla_met'] * 100).round(2)
fig_sla = px.bar(sla_priority, x='priority', y='SLA Compliance %', color='priority',
                 color_discrete_map={"P1": "red", "P2": "orange", "P3": "green", "P4": "blue"})
st.plotly_chart(fig_sla, use_container_width=True, key="sla_priority_chart")

# === Assignment Group Performance ===
st.subheader("👥 Assignment Group Performance")
group_perf = event_df.groupby('assignment_group').agg(
    total_tickets=('ticket_id', 'nunique'),
    sla_compliance=('sla_met', 'mean')
).reset_index()
group_perf['SLA Compliance %'] = (group_perf['sla_compliance'] * 100).round(2)
st.dataframe(group_perf)

# === Category Distribution ===
st.subheader("📂 Category Distribution")
category_count = event_df['category'].value_counts().reset_index()
category_count.columns = ['Category', 'Tickets']
fig_category = px.pie(category_count, values='Tickets', names='Category', title="Tickets by Category")
st.plotly_chart(fig_category, use_container_width=True, key="category_chart")

# === Pending States Breakdown ===
st.subheader("⏳ Pending State Breakdown")
pending_states = event_df[event_df['activity'].str.startswith("Pending")]['activity'].value_counts().reset_index()
pending_states.columns = ['Pending State', 'Count']
fig_pending = px.bar(pending_states, x='Pending State', y='Count', color='Pending State')
st.plotly_chart(fig_pending, use_container_width=True, key="pending_states_chart")

# === Process Mining DFG (Graphviz Style) ===
st.subheader("🔄 Process Flow - Graphviz")
dfg_path = "app/static/dfg_perf_graphviz.png"
generate_perf_dfg_graphviz(event_df, output_path=dfg_path, mode="avg")
if os.path.exists(dfg_path):
    st.image(dfg_path, caption="ServiceNow-style Performance DFG", use_container_width=True)

# === Enhanced ServiceNow Sequence Flow ===
st.subheader("🔗 ServiceNow-Style Sequence Flow")
queues = sorted(event_df['queue_id'].unique().tolist())
selected_queues = st.multiselect("Select Queues for Flow Map", queues, default=queues[:3])

seq_fig = generate_sequence_flow(event_df, selected_queues)
st.plotly_chart(seq_fig, use_container_width=True, key="sequence_flow_chart")
