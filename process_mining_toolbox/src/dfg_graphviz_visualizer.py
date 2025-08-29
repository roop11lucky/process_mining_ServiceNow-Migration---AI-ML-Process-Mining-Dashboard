import os
import pandas as pd
from datetime import timedelta
from graphviz import Digraph

def format_duration(seconds):
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

def generate_perf_dfg_graphviz(df, output_path="app/static/dfg_perf_graphviz.png", mode="avg"):
    """
    Generate a Celonis-style DFG using Graphviz with human-readable durations.
    mode = 'avg' | 'median' | 'total'
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
    else:
        dfg_summary = edge_df.groupby(['source','target']).agg(
            duration=('duration_sec','mean'),
            count=('duration_sec','count')
        ).reset_index()

    # ✅ Build Graphviz Digraph
    dot = Digraph(comment="Performance DFG", format='png')
    dot.attr(rankdir='LR', splines='true', overlap='false', fontsize='10')

    # Add nodes
    activities = set(dfg_summary['source']).union(set(dfg_summary['target']))
    for act in activities:
        dot.node(act, shape='box', style='filled', color='lightblue')

    # Add edges with labels
    for _, row in dfg_summary.iterrows():
        human_dur = format_duration(row['duration'])
        label = f"{human_dur}\n({row['count']} tickets)"
        dot.edge(row['source'], row['target'], label=label)

    # Render PNG
    tmp_path = output_path.replace(".png", "")
    dot.render(tmp_path, cleanup=True)
    return output_path
