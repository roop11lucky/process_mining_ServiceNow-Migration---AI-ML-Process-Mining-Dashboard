import pandas as pd

def generate_dfg(df):
    """
    Generate Directly-Follows Graph (DFG) with:
    - Edge frequency (count of transitions)
    - Average duration between activities
    - SLA breach percentage per edge
    """

    # Ensure timestamp sorted per ticket
    df = df.sort_values(by=['ticket_id', 'timestamp'])

    edges = []

    for ticket_id, group in df.groupby('ticket_id'):
        activities = group['activity'].tolist()
        timestamps = group['timestamp'].tolist()
        sla_flags = group['sla_met'].tolist() if 'sla_met' in group.columns else [1] * len(group)

        for i in range(len(activities)-1):
            source = activities[i]
            target = activities[i+1]
            duration_sec = (timestamps[i+1] - timestamps[i]).total_seconds()
            sla_value = sla_flags[i+1]

            edges.append({
                'queue_id': group['queue_id'].iloc[0],
                'source': source,
                'target': target,
                'duration_sec': duration_sec,
                'sla_met': sla_value
            })

    dfg_df = pd.DataFrame(edges)

    # Aggregate: count, avg_duration, SLA%
    dfg_summary = dfg_df.groupby(['queue_id','source','target']).agg(
        count=('source','count'),
        avg_duration_sec=('duration_sec','mean'),
        sla_compliance_pct=('sla_met','mean')
    ).reset_index()

    return dfg_summary
