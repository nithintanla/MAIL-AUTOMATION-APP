# Clickhouse Query Runner

This script runs a specific query against a Clickhouse database and displays the results.

## Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Running the Script

Simply run:
```bash
python query_runner.py
```

By default, the script will query data for the last 7 days. The results will show:
- Date
- Aggregator
- Brand
- AgentID
- Agent
- TrafficType
- ContentType
- TemplateType
- Parts
- Received
- Sent
- Delivered

The first 5 rows of results will be displayed in the console.
