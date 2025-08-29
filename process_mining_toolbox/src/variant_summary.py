import pandas as pd

def compute_variant_summary(df, selected_queues=None):
    """
    Generate a variant summary table for the given queues.
    """
    if selected_queues:
        df = df[df['queue_id'].isin(selected_queues)]

    # Ensure chronological order per ticket
    df = df.sort_values(by=['ticket_id', 'timestamp'])

    # Build variants
    variants = df.groupby('ticket_id')['activity'].apply(lambda x: " → ".join(x)).reset_index()
    merged = variants.merge(df[['ticket_id','queue_id']], on='ticket_id')
    
    summary = merged.groupby(['queue_id','activity']).size().reset_index(name='count')
    summary = summary.rename(columns={'activity':'variant'})
    summary = summary.sort_values(by='count', ascending=False)

    return summary
