import pandas as pd
from sqlalchemy import create_engine

def write_to_postgres(df, db_uri, table_name="event_log"):
    """
    Write the event log DataFrame to PostgreSQL
    """
    try:
        engine = create_engine(db_uri)
        with engine.begin() as conn:
            df.to_sql(table_name, con=conn, if_exists='replace', index=False)
        print(f"✅ Data successfully written to {table_name}")
    except Exception as e:
        print(f"❌ Failed to write to PostgreSQL: {e}")
