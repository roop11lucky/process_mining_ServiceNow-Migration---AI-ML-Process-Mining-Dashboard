import os
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

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

def generate_perf_dfg(df, output_path="app/static/dfg_perf_human_readable.png", mode="avg"):
    """
    Generate a performance-based DFG with human-readable durations.
    mode = 'avg' | 'median' | 'total'
    """
    # Ensure output folder exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Sort events by ticket and timestamp
    df = df.sort_values(by=['ticket_id', 'timestamp'])

    # Build edge list with durations
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

    # Aggregate based on mode
    if mode == "median":
        dfg_summary = edge_df.groupby(['source','target']).agg(
            duration=('duration_sec','median'),
            count=('duration_sec','count')
        ).reset_index()
    elif mode == "total":
        dfg_summary = edge_df.groupby(['source','target']).agg(
            duration=('duration_sec','sum'),
            count=('duration_sec','count')
        ).reset_index()
    else:  # default avg
        dfg_summary = edge_df.groupby(['source','target']).agg(
            duration=('duration_sec','mean'),
            count=('duration_sec','count')
        ).reset_index()

    # Build directed graph
    G = nx.DiGraph()

    for _, row in dfg_summary.iterrows():
        source = row['source']
        target = row['target']
        human_dur = format_duration(row['duration'])
        label = f"{human_dur} ({row['count']} tickets)"
        G.add_edge(source, target, label=label)

    # Draw graph
    pos = nx.spring_layout(G, k=1, iterations=50)
    plt.figure(figsize=(12,8))
    nx.draw(G, pos, with_labels=True, node_color="lightblue", node_size=3000, arrows=True)
    edge_labels = nx.get_edge_attributes(G, 'label')
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_size=8)

    plt.title(f"Directly-Follows Graph (Performance View - {mode.capitalize()})")
    
    # ✅ Remove tight_layout to avoid warning, add padding manually
    plt.subplots_adjust(left=0.05, right=0.95, top=0.90, bottom=0.05)

    plt.savefig(output_path, bbox_inches='tight')
    plt.close()


    return output_path
