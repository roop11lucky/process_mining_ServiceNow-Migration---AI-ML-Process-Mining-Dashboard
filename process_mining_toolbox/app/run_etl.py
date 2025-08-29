import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

from log_parser import load_event_log
from sla_analysis import analyze_sla
from queue_analysis import analyze_queues
from dfg_generator import generate_dfg
from db_writer import write_to_postgres

from dotenv import load_dotenv
load_dotenv()

db_uri = os.getenv("PG_URI", "postgresql://postgres:Password@localhost:5433/eventlog_db")

def main():
    print("🔁 Running pipeline...")
    df = load_event_log("data/raw_event_log.csv")
    df_sla = analyze_sla(df)
    queue_summary = analyze_queues(df_sla)

    # ✅ Generate DFG edges
    edge_df = generate_dfg(df_sla)

    write_to_postgres(df_sla, db_uri, table_name="event_log")
    write_to_postgres(queue_summary, db_uri, table_name="queue_summary")
    write_to_postgres(edge_df, db_uri, table_name="dfg_edges")

    print("✅ Pipeline complete. DFG edges generated.")

if __name__ == "__main__":
    main()
