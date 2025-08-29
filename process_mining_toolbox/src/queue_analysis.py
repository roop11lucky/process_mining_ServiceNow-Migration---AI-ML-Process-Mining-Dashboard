import pandas as pd

def analyze_queues(df):
    """
    Generate queue-level summary:
    - Total Tickets
    - SLA Compliance %
    - Avg Resolution Time
    """
    summary = df.groupby('queue_id').agg(
        total_tickets=('ticket_id', 'nunique'),
        sla_met=('sla_met', 'sum'),
        avg_duration=('duration_sec', 'mean')
    ).reset_index()

    # SLA Compliance %
    summary['sla_compliance'] = (summary['sla_met'] / summary['total_tickets']) * 100
    return summary
