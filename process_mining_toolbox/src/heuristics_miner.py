import os
import pandas as pd
from pm4py.objects.log.util import dataframe_utils
from pm4py.objects.conversion.log import converter as log_converter
from pm4py.algo.discovery.heuristics import algorithm as heuristics_miner
from pm4py.visualization.heuristics_net import visualizer as hn_visualizer

def generate_heuristics_miner_graph(df, output_path="heuristics_miner_graph.png"):
    """
    Generate a Heuristics Miner graph from the event log dataframe and save it to a PNG file.

    Parameters:
        df (pd.DataFrame): The event log dataframe containing at least the columns 'ticket_id', 'activity', and 'timestamp'.
        output_path (str): File path to save the generated PNG image.
    """
    if df.empty:
        print("Event log DataFrame is empty. Cannot generate heuristics miner graph.")
        return

    # Rename columns to match PM4Py expected schema
    df = df.rename(columns={
        'ticket_id': 'case:concept:name',
        'activity': 'concept:name',
        'timestamp': 'time:timestamp'
    })

    # Format timestamps and convert to proper event log format
    df['time:timestamp'] = pd.to_datetime(df['time:timestamp'])
    df = dataframe_utils.convert_timestamp_columns_in_df(df)

    event_log = log_converter.apply(df)

    # Discover Heuristics Net model
    heu_net = heuristics_miner.apply_heu(event_log)

    # Visualize
    gviz = hn_visualizer.apply(heu_net)

    # Save image
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    hn_visualizer.save(gviz, output_path)

    print(f"Heuristics Miner graph saved to: {output_path}")