import os
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
from datetime import timedelta

def generate_network_process_map(df, selected_queues, output_path="app/static/network_process_map.png"):
    """
    Generates a static BPMN-style network process map for selected queues.
    - Node size = ticket volume
    - Edge label = Avg Duration + Ticket Count
    - SLA breaches = Red edges, Compliant = Green edges
    """

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Filter for selected queues
    df = df[df['queue_id'].isin(selected_queues)].sort_values(by=['ticket_id', 'timestamp'])

    edges = []
    for ticket_id, group in df.groupby('ticket_id'):
        activities = group['activity'].tolist()
        timestamps = group['timestamp'].tolist()
        sla_values = group['sla_met'].tolist()
        for i in range(len(activities)-1):
            source = activities[i]
            target = activities[i+1]
            duration = (timestamps[i+1] - timestamps[i]).total_seconds()
            sla_flag = all(sla_values)  # True if all SLA met
            edges.append((source, target, duration, sla_flag))

    if not edges:
        return None

    edge_df = pd.DataFrame(edges, columns=['source', 'target', 'duration', 'sla_met'])

    # Aggregate edges
    summary = edge_df.groupby(['source','target']).agg(
        avg_duration=('duration','mean'),
        count=('duration','count'),
        sla_rate=('sla_met','mean')
    ).reset_index()

    # Build graph
    G = nx.DiGraph()

    for _, row in summary.iterrows():
        G.add_edge(
            row['source'],
            row['target'],
            avg_duration=row['avg_duration'],
            count=row['count'],
            sla_rate=row['sla_rate']
        )

    # Node size based on ticket count
    node_counts = df['activity'].value_counts().to_dict()
    node_sizes = [node_counts.get(node, 1)*300 for node in G.nodes()]

    # Edge colors based on SLA
    edge_colors = []
    for u, v, data in G.edges(data=True):
        if data['sla_rate'] < 0.5:
            edge_colors.append("red")
        elif data['sla_rate'] < 0.8:
            edge_colors.append("orange")
        else:
            edge_colors.append("green")

    pos = nx.spring_layout(G, k=1, seed=42)  # Stable layout
    plt.figure(figsize=(12,8))
    nx.draw_networkx_nodes(G, pos, node_size=node_sizes, node_color="lightblue")
    nx.draw_networkx_labels(G, pos, font_size=10, font_weight="bold")
    nx.draw_networkx_edges(G, pos, edge_color=edge_colors, arrows=True, width=2)

    # Add edge labels with human-readable durations
    edge_labels = {}
    for u, v, data in G.edges(data=True):
        avg_dur = str(timedelta(seconds=data['avg_duration'])).split('.')[0]
        edge_labels[(u,v)] = f"{avg_dur}\n({data['count']} tickets)"

    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_size=8)

    plt.title("Network Process Map (ServiceNow Style)")
    plt.axis("off")
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()

    return output_path
