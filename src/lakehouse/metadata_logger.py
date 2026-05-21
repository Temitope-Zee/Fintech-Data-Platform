import json
import os
from datetime import datetime

METADATA_PATH = os.path.expanduser('~/fintech-data-platform/metadata')
os.makedirs(METADATA_PATH, exist_ok=True)

LINEAGE_FILE = f'{METADATA_PATH}/pipeline_lineage.json'

def load_lineage():
    """Load existing lineage records or start fresh."""
    if os.path.exists(LINEAGE_FILE):
        with open(LINEAGE_FILE) as f:
            return json.load(f)
    return {"pipeline": "Fintech Data Platform", "runs": []}

def save_lineage(data):
    """Save lineage records to file."""
    with open(LINEAGE_FILE, 'w') as f:
        json.dump(data, f, indent=2)

def log_pipeline_run(
    layer,
    source,
    destination,
    records_in,
    records_out,
    transformation
):
    """
    Log a single pipeline stage run.

    Args:
        layer: Bronze, Silver, or Gold
        source: Where data came from
        destination: Where data is going
        records_in: How many records entered this stage
        records_out: How many records left this stage
        transformation: What happened to the data
    """
    lineage = load_lineage()

    run_entry = {
        "run_id": f"run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
        "timestamp": datetime.utcnow().isoformat(),
        "layer": layer,
        "lineage": {
            "source": source,
            "destination": destination,
        },
        "transformation": transformation,
        "metrics": {
            "records_in": records_in,
            "records_out": records_out,
            "records_dropped": records_in - records_out,
            "pass_rate": f"{(records_out/records_in*100):.1f}%" if records_in > 0 else "N/A"
        },
        "status": "SUCCESS"
    }

    lineage["runs"].append(run_entry)
    save_lineage(lineage)

    # Print a nice summary
    print(f"\n{'='*50}")
    print(f"📊 METADATA LOGGED — {layer} Layer")
    print(f"{'='*50}")
    print(f"  Source      : {source}")
    print(f"  Destination : {destination}")
    print(f"  Records In  : {records_in}")
    print(f"  Records Out : {records_out}")
    print(f"  Dropped     : {records_in - records_out}")
    print(f"  Pass Rate   : {run_entry['metrics']['pass_rate']}")
    print(f"  Transform   : {transformation}")
    print(f"  Logged at   : {run_entry['timestamp']}")
    print(f"{'='*50}\n")

    return run_entry

def print_full_lineage():
    """Print the complete pipeline lineage history."""
    lineage = load_lineage()

    print(f"\n{'='*60}")
    print(f"  FINTECH DATA PLATFORM — FULL PIPELINE LINEAGE")
    print(f"{'='*60}")
    print(f"  Total runs logged: {len(lineage['runs'])}")
    print(f"{'='*60}")

    for run in lineage["runs"]:
        print(f"\n  Run ID    : {run['run_id']}")
        print(f"  Layer     : {run['layer']}")
        print(f"  Timestamp : {run['timestamp']}")
        print(f"  Source    : {run['lineage']['source']}")
        print(f"  Dest      : {run['lineage']['destination']}")
        print(f"  Records   : {run['metrics']['records_in']} in → "
              f"{run['metrics']['records_out']} out "
              f"({run['metrics']['pass_rate']} pass rate)")
        print(f"  Transform : {run['transformation']}")
        print(f"  Status    : ✅ {run['status']}")

    print(f"\n{'='*60}\n")

if __name__ == "__main__":
    print_full_lineage()
