import os
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
from datetime import timedelta

def format_duration(seconds):
    """Convert seconds into human-readable format."""
    total_sec = int(seconds)
    hours = total_sec // 3600
    minutes = (total_sec % 3600) // 60
    secs = total_sec % 60

    if hours > 0:
        return f"{hours} hr {minutes} min {secs} sec"
    elif minutes > 0:
        return f"{minutes} min {secs} sec"
    else:
        return f"{secs} sec"

def generate_perf_dfg(df, output_path="app/static/dfg_perf_human_readable.png"):
    """
    Generate a performance-based DFG with human-readable durations.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df = df.sort_values(by=['ticket_id', 'timestamp'])

    edges = []
    for ticket_id, group in df.groupby('ticket_id'):
        activities = group['activity'].tolist()
        timestamps = group['timestamp'].tolist()
        for i in range(len(activities)-1):
            source = activities[i]
            target = activities[i+1]
            duration_sec = (timestamps[i+1] - timestamps[i]).total_seconds()
            edges.append((source, target, duration_sec))

    edge_df = pd.DataFrame(edges, columns=['source', 'target', 'duration_sec'])

    dfg_summary = edge_df.groupby(['source','target']).agg(
        avg_duration_sec=('duration_sec','mean'),
        count=('duration_sec','count')
    ).reset_index()

    # Build graph
    G = nx.DiGraph()

    for _, row in dfg_summary.iterrows():
        source = row['source']
        target = row['target']
        human_dur = format_duration(row['avg_duration_sec'])
        label = f"{human_dur} ({row['count']} tickets)"
        G.add_edge(source, target, label=label)

    pos = nx.spring_layout(G, k=1, iterations=50)
    plt.figure(figsize=(12,8))
    nx.draw(G, pos, with_labels=True, node_color="lightblue", node_size=3000, arrows=True)
    edge_labels = nx.get_edge_attributes(G, 'label')
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_size=8)

    plt.title("Directly-Follows Graph (Performance View)")
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()

    return output_path
