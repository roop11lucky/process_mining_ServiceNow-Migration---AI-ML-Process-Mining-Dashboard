import pandas as pd
from datetime import timedelta

def analyze_sla(df, sla_threshold_minutes=60):
    """
    Adds SLA calculations and preserves existing sla_met if present.
    - If sla_met exists (from generator), keeps it.
    - If missing, calculates based on Created -> Resolved/Closed timestamps.
    """

    # Ensure timestamp column is datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Calculate step durations
    df['duration_sec'] = df.groupby('ticket_id')['timestamp'].diff().dt.total_seconds().fillna(0)

    # ✅ If sla_met already exists, skip recalculation
    if 'sla_met' in df.columns:
        return df

    # --- SLA Calculation for real logs ---
    # Get ticket creation times
    created_rows = df[df['activity'] == 'Created']
    if created_rows.empty:
        # Use first timestamp per ticket as created time if 'Created' missing
        created_rows = df.groupby('ticket_id').first().reset_index()[['ticket_id', 'timestamp']]
        created_rows = created_rows.rename(columns={'timestamp': 'created_time'})
    else:
        created_rows = created_rows[['ticket_id', 'timestamp']].rename(columns={'timestamp': 'created_time'})

    # Get resolution/close times
    sla_df = df[df['activity'].isin(['Resolved', 'Closed'])].copy()
    if sla_df.empty:
        # Use last timestamp per ticket if no Resolved/Closed
        sla_df = df.groupby('ticket_id').last().reset_index()

    # Merge to compute resolution time
    sla_df = sla_df.merge(created_rows, on='ticket_id', how='left')
    sla_df['resolution_time_min'] = (sla_df['timestamp'] - sla_df['created_time']).dt.total_seconds() / 60
    sla_df['sla_met'] = sla_df['resolution_time_min'] <= sla_threshold_minutes

    # Merge back to main DataFrame
    final_df = df.merge(
        sla_df[['ticket_id', 'resolution_time_min', 'sla_met']],
        on='ticket_id',
        how='left'
    )

    return final_df
