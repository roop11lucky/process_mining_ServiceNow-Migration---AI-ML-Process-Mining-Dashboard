import sys
import os
import pandas as pd

# ✅ Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# ✅ Imports
from src.log_parser import load_event_log
from src.sla_analysis import analyze_sla
from src.queue_analysis import analyze_queues
from src.dfg_generator import generate_dfg
from src.db_writer import write_to_postgres

from src.dfg_perf_visualizer import generate_perf_dfg         # NetworkX version
from src.dfg_graphviz_visualizer import generate_perf_dfg_graphviz  # Graphviz version
from src.variant_analysis import discover_variants

# ✅ Load synthetic event log
df = load_event_log("data/raw_event_log.csv")

# ✅ SLA Analysis
df = analyze_sla(df)

# ✅ Queue summary per queue_id
queue_summary = analyze_queues(df)

# ✅ Generate DFG edges per queue_id
dfg_edges = generate_dfg(df)

# ✅ Generate Performance DFG (NetworkX)
generate_perf_dfg(df, mode="avg")

# ✅ Generate Performance DFG (Graphviz)
generate_perf_dfg_graphviz(df, mode="avg")

# ✅ Write data to PostgreSQL
db_uri = "postgresql://postgres:Password@localhost:5433/eventlog_db"
write_to_postgres(df, db_uri, table_name="event_log")
write_to_postgres(queue_summary, db_uri, table_name="queue_summary")
write_to_postgres(dfg_edges, db_uri, table_name="dfg_edges")

print("✅ Pipeline complete. Multi-queue data written to DB.")

# ✅ Variant Analysis
variant_summary = discover_variants(df)
os.makedirs("app/output", exist_ok=True)
variant_summary.to_csv("app/output/variant_summary.csv", index=False)
print("✅ Variant analysis complete. Results saved to app/output/variant_summary.csv")
