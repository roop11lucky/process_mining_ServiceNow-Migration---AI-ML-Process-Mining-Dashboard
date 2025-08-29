import pandas as pd

def discover_variants(df):
    """
    Identify unique workflow variants per queue.
    Returns: DataFrame with queue_id, variant_id, activities_sequence, ticket_count
    """

    # Ensure correct order of activities per ticket
    df = df.sort_values(by=['ticket_id', 'timestamp'])

    variants = []

    for (queue_id, ticket_id), group in df.groupby(['queue_id', 'ticket_id']):
        activities = " → ".join(group['activity'].tolist())
        variants.append({'queue_id': queue_id, 'ticket_id': ticket_id, 'sequence': activities})

    variant_df = pd.DataFrame(variants)

    # Count unique variants per queue
    variant_summary = variant_df.groupby(['queue_id', 'sequence']).agg(
        ticket_count=('ticket_id', 'count')
    ).reset_index()

    # Assign variant IDs per queue
    variant_summary['variant_id'] = variant_summary.groupby('queue_id').cumcount() + 1

    return variant_summary
