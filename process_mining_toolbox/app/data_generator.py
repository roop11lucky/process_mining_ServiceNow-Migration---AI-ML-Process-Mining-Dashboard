import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

OUTPUT_FILE = "data/raw_event_log.csv"
NUM_QUEUES = 83
TICKETS_PER_QUEUE = 120
USERS = ["agent_A", "agent_B", "agent_C", "agent_D"]

# ✅ ServiceNow-like attributes
PRIORITIES = ["P1", "P2", "P3", "P4"]
ASSIGNMENT_GROUPS = ["Network Ops", "App Support", "Security", "Database Team"]
CATEGORIES = ["Hardware", "Software", "Access", "Other"]

# ✅ Extended Workflow Patterns with Pending States
WORKFLOW_PATTERNS = [
    ["Open", "Assigned", "In Progress", "Resolved", "Closed"],
    ["Open", "Assigned", "In Progress", "Escalated", "Resolved", "Closed"],
    ["Open", "Assigned", "In Progress", "Blocked", "In Progress", "Resolved", "Closed"],
    ["Open", "Assigned", "In Progress", "Pending Customer Action", "In Progress", "Resolved", "Closed"],
    ["Open", "Assigned", "In Progress", "Pending Monitoring", "Resolved", "Closed"],
    ["Open", "Assigned", "In Progress", "Pending Follow Up", "Resolved", "Closed"],
    ["Open", "Assigned", "QA", "Resolved", "Closed"],
    ["Open", "Assigned", "In Progress", "Reopened", "In Progress", "Resolved", "Closed"]
]

def generate_ticket(ticket_id, queue_id, start_time, allowed_patterns):
    workflow = random.choice(allowed_patterns)
    rows = []
    timestamp = start_time
    sla_threshold = random.choice([30, 60, 120, 240])

    # ✅ Random ServiceNow-like fields
    priority = random.choice(PRIORITIES)
    assignment_group = random.choice(ASSIGNMENT_GROUPS)
    category = random.choice(CATEGORIES)

    for activity in workflow:
        rows.append({
            "ticket_id": ticket_id,
            "activity": activity,
            "timestamp": timestamp,
            "user": random.choice(USERS),
            "queue_id": queue_id,
            "priority": priority,
            "assignment_group": assignment_group,
            "category": category,
            "sla_met": None
        })
        timestamp += timedelta(minutes=random.randint(5, 120))

    total_time = (rows[-1]["timestamp"] - rows[0]["timestamp"]).total_seconds() / 60
    sla_breached = total_time > sla_threshold
    for r in rows:
        r["sla_met"] = not sla_breached
    return rows

def generate_data():
    all_rows = []
    ticket_counter = 1
    base_time = datetime.now() - timedelta(days=30)

    for q in range(1, NUM_QUEUES + 1):
        queue_id = f"Q{q:02d}"

        # 🔥 Assign complexity per queue
        complexity = random.choice(["low", "medium", "high"])
        if complexity == "low":
            allowed_patterns = WORKFLOW_PATTERNS[:2]
        elif complexity == "medium":
            allowed_patterns = WORKFLOW_PATTERNS[:5]
        else:
            allowed_patterns = WORKFLOW_PATTERNS

        for _ in range(TICKETS_PER_QUEUE):
            start_time = base_time + timedelta(minutes=random.randint(0, 100000))
            all_rows.extend(generate_ticket(ticket_counter, queue_id, start_time, allowed_patterns))
            ticket_counter += 1

    df = pd.DataFrame(all_rows)
    os.makedirs("data", exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Synthetic ServiceNow-style data generated: {OUTPUT_FILE} ({len(df)} rows)")

if __name__ == "__main__":
    generate_data()
