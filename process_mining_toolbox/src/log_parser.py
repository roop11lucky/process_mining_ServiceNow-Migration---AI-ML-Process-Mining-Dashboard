import pandas as pd

def load_event_log(filepath):
    """
    Load and normalize raw event log from multi-queue GUTS data
    """
    df = pd.read_csv(filepath, parse_dates=['timestamp'])

    # Sort and assign order
    df = df.sort_values(by=['ticket_id', 'timestamp'])
    df['event_order'] = df.groupby('ticket_id').cumcount() + 1

    # Calculate step durations
    df['next_timestamp'] = df.groupby('ticket_id')['timestamp'].shift(-1)
    df['duration_sec'] = (df['next_timestamp'] - df['timestamp']).dt.total_seconds()
    df['duration_sec'] = df['duration_sec'].fillna(0)

    return df
