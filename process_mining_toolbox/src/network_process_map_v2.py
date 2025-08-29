"""
src/network_process_map_v2.py

Improved Network Process Map:
  - Computes global multi-queue transitions, then filters to the selected subset.
  - Adds self-loops for tickets that remained in a single selected queue (stuck).
  - Distinguishes stuck vs flowing edges visually with colors/styles.
  - Grid layout fallback when only self-loops exist to reduce overlap.
  - Supports diagnostics via debug flag.
"""

import os
from collections import Counter

import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt


def grid_layout(nodes, max_cols=8, x_gap=2.0, y_gap=1.5):
    """Simple grid layout for a list of nodes to reduce overlap (used when only self-loops exist)."""
    pos = {}
    cols = min(max_cols, len(nodes)) if nodes else 1
    for idx, node in enumerate(sorted(nodes)):
        row = idx // cols
        col = idx % cols
        x = col * x_gap - (cols - 1) * x_gap / 2
        y = -row * y_gap
        pos[node] = (x, y)
    return pos


def _compute_queue_transitions(event_df: pd.DataFrame, selected_queues=None):
    """
    Compute:
      - global transitions (deduplicated consecutive queue changes)
      - per-ticket queue sequence (deduped)
    If selected_queues is provided, it's used only later for filtering; internal here computes full.
    Returns (global_transitions: Counter, per_ticket_sequences: dict)
    """
    df = event_df.sort_values(['ticket_id', 'timestamp'])
    transitions = Counter()
    per_ticket_sequences = {}

    for ticket_id, group in df.groupby('ticket_id'):
        queues = group['queue_id'].tolist()
        filtered = []
        for q in queues:
            if not q:
                continue
            if not filtered or filtered[-1] != q:
                filtered.append(q)
        per_ticket_sequences[ticket_id] = filtered
        for a, b in zip(filtered, filtered[1:]):
            transitions[(a, b)] += 1

    return transitions, per_ticket_sequences


def generate_network_process_map_v2(event_df: pd.DataFrame, selected_queues, output_path: str,
                                    debug: bool = False, min_edge_threshold: int = 1):
    """
    Generates a network process map combining:
      * Multi-queue transitions (filtered to selected_queues)
      * Self-loops for tickets that only visited one selected queue (stuck)
    Parameters:
      - event_df: DataFrame with columns ['ticket_id', 'timestamp', 'queue_id']
      - selected_queues: list of queue_ids to include in the filtered view
      - output_path: where to save PNG
      - debug: if True returns diagnostics dict
      - min_edge_threshold: minimum count to show an edge (both transitions and self-loops)
    """
    required = {'ticket_id', 'timestamp', 'queue_id'}
    if not required.issubset(event_df.columns):
        raise ValueError(f"event_df must contain columns {required}, got {list(event_df.columns)}")

    # Normalize data
    df = event_df.copy()
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df['queue_id'] = df['queue_id'].astype(str).str.strip()
    selected_queues = [str(q).strip() for q in selected_queues]

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Compute global transitions and per-ticket sequences
    global_transitions, per_ticket_sequences = _compute_queue_transitions(df)

    # Filter transitions to selected subset (multi-queue)
    transitions = Counter({
        (src, dst): cnt
        for (src, dst), cnt in global_transitions.items()
        if src in selected_queues and dst in selected_queues and src != dst and cnt >= min_edge_threshold
    })

    # Count tickets stuck in a single selected queue
    single_queue_counts = Counter()
    for ticket_id, seq in per_ticket_sequences.items():
        if len(seq) == 1 and seq[0] in selected_queues:
            single_queue_counts[seq[0]] += 1
    # apply threshold
    single_queue_counts = Counter({q: c for q, c in single_queue_counts.items() if c >= min_edge_threshold})

    # Diagnostics snapshot
    diagnostics = {
        "distinct_queues_in_data": sorted(df['queue_id'].dropna().unique().tolist()),
        "selected_queues": selected_queues,
        "total_tickets": df['ticket_id'].nunique(),
        "per_ticket_filtered_sequences_sample": dict(list(per_ticket_sequences.items())[:10]),
        "filtered_transition_counts": dict(transitions),
        "single_queue_ticket_counts": dict(single_queue_counts),
    }

    # Build graph
    G = nx.DiGraph()

    # Add transitions (flowing edges)
    for (src, dst), count in transitions.items():
        G.add_edge(src, dst, weight=count, kind='transition')

    # Add self-loops (stuck)
    for queue, count in single_queue_counts.items():
        G.add_edge(queue, queue, weight=count, kind='self_loop')

    # If nothing to show, present placeholder
    if G.number_of_edges() == 0:
        nodes = sorted(set(selected_queues))
        pos = grid_layout(nodes, max_cols=10)
        fig, ax = plt.subplots(figsize=(max(8, len(nodes) * 0.3), 4))
        nx.draw_networkx_nodes(G if G.number_of_nodes() else nx.DiGraph(nodes), pos,
                               node_size=1000, node_color='lightgray', edgecolors='black', ax=ax)
        nx.draw_networkx_labels(G if G.number_of_nodes() else nx.DiGraph(nodes), pos,
                                font_size=9, font_weight='bold', ax=ax)
        ax.set_title("Queues selected (no transitions or stuck tickets above threshold)", fontsize=14, weight='bold')
        ax.axis('off')
        fig.savefig(output_path, bbox_inches='tight', dpi=150)
        plt.close(fig)
        if debug:
            return diagnostics
        return

    # Edge width scaling across all edges
    weights = [d['weight'] for (_, _, d) in G.edges(data=True)]
    max_w = max(weights)
    min_w = min(weights)

    def scale_width(w):
        if max_w == min_w:
            return 4
        return 1 + 7 * ((w - min_w) / (max_w - min_w))  # scale into [1,8]

    edge_widths = [scale_width(d['weight']) for (_, _, d) in G.edges(data=True)]

    # Determine layout: if only self-loops exist, use grid to spread; else apply layering heuristic
    only_self_loops = all(data.get("kind") == "self_loop" for _, _, data in G.edges(data=True))
    if only_self_loops:
        pos = grid_layout(sorted(set(selected_queues)), max_cols=10)
    else:
        try:
            node_order = sorted(
                G.nodes(),
                key=lambda n: (G.in_degree(n), -G.out_degree(n))
            )
            layer = {}
            for n in G.nodes():
                if G.in_degree(n) == 0:
                    layer[n] = 0
                else:
                    layer[n] = max((layer.get(p, 0) for p in G.predecessors(n))) + 1
            layers = {}
            for n, l in layer.items():
                layers.setdefault(l, []).append(n)
            y_gap = 1.5
            pos = {}
            for l, nodes in layers.items():
                x_spacing = 1.6
                total = len(nodes)
                for idx, n in enumerate(sorted(nodes)):
                    x = idx * x_spacing - (total - 1) * x_spacing / 2
                    y = -l * y_gap
                    pos[n] = (x, y)
        except Exception:
            pos = nx.spring_layout(G, seed=42)

    # Ensure every node has a position
    for n in G.nodes():
        if n not in pos:
            pos[n] = (0, 0)

    # Begin plotting
    fig, ax = plt.subplots(figsize=(12, 7))
    nx.draw_networkx_nodes(G, pos, node_size=1200, node_color='white',
                           edgecolors='black', linewidths=1.5, ax=ax)
    nx.draw_networkx_labels(G, pos, font_size=10, font_weight='bold', ax=ax)

    # Colors/styles
    transition_color = "#1f77b4"  # blue
    self_loop_color = "#ff7f0e"    # orange

    for (u, v, data), width in zip(G.edges(data=True), edge_widths):
        kind = data.get("kind", "transition")
        if kind == "self_loop":
            # dashed, lighter self-loop
            nx.draw_networkx_edges(
                G,
                pos,
                edgelist=[(u, v)],
                width=width,
                connectionstyle="arc3,rad=0.5",
                arrowstyle='-|>',
                arrowsize=10,
                ax=ax,
                alpha=0.7,
                edge_color=self_loop_color,
                style='dashed'
            )
            label_x = pos[u][0] + 0.25
            label_y = pos[u][1] + 0.25
        else:
            nx.draw_networkx_edges(
                G,
                pos,
                edgelist=[(u, v)],
                width=width,
                arrowstyle='-|>',
                arrowsize=12,
                connectionstyle='arc3,rad=0.1',
                ax=ax,
                alpha=0.85,
                edge_color=transition_color,
            )
            label_x = (pos[u][0] + pos[v][0]) / 2
            label_y = (pos[u][1] + pos[v][1]) / 2

        ax.text(
            label_x,
            label_y,
            str(data['weight']),
            fontsize=8,
            ha='center',
            va='center',
            backgroundcolor='white',
            bbox=dict(boxstyle="round,pad=0.1", fc="white", ec="none", alpha=0.7)
        )

    ax.set_title("ServiceNow-Style Network Process Map (Transitions + Stuck Self-Loops)", fontsize=16, weight='bold')
    ax.axis('off')

    # Legend construction
    import matplotlib.lines as mlines
    legend_handles = [
        mlines.Line2D([], [], color=transition_color, linewidth=3, label="Multi-queue transition"),
        mlines.Line2D([], [], color=self_loop_color, linewidth=3, linestyle='--', label="Stuck ticket (self-loop)"),
    ]
    ax.legend(handles=legend_handles, title="Legend", loc='lower left', frameon=True)

    # Save and close
    fig.savefig(output_path, bbox_inches='tight', dpi=150)
    plt.close(fig)

    if debug:
        return diagnostics
