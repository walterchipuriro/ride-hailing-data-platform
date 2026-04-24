#!/usr/bin/env python3

import argparse
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_OUTPUT_DIR = PROJECT_ROOT / "data" / "local" / "raw" / "ride_events"


def run_command(command: list[str], description: str) -> None:
    print(f"\n=== {description} ===")
    print("Running:", " ".join(command))
    result = subprocess.run(command, cwd=PROJECT_ROOT)
    if result.returncode != 0:
        print(f"\nERROR: Step failed -> {description}")
        sys.exit(result.returncode)


def get_latest_generated_file() -> Path:
    jsonl_files = sorted(RAW_OUTPUT_DIR.glob("*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not jsonl_files:
        print("ERROR: No generated JSONL file found in data/local/raw/ride_events/")
        sys.exit(1)
    return jsonl_files[0]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the full recovery pipeline.")
    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help="Number of ride events to generate. Default is 100.",
    )
    args = parser.parse_args()

    # Step 1: Generate data
    run_command(
        ["python", "data_generator/generate_ride_events.py", "--count", str(args.count)],
        "Generate ride events",
    )

    latest_file = get_latest_generated_file()
    print(f"\nLatest generated file: {latest_file}")

    # Step 2: Upload to MinIO
    run_command(
        ["python", "ingestion/upload_to_minio.py", "--file", str(latest_file)],
        "Upload raw file to MinIO",
    )

    # Step 3: Load to staging
    run_command(
        ["python", "ingestion/load_to_postgres.py", "--file", str(latest_file)],
        "Load raw file into staging.ride_events",
    )

    # Step 4: Load staging to warehouse facts
    run_command(
        ["python", "ingestion/load_staging_to_warehouse.py"],
        "Load staging data into warehouse fact tables",
    )

    # Step 5: Populate dimensions
    run_command(
        ["python", "ingestion/load_dim_payment_method.py"],
        "Populate dim_payment_method",
    )
    run_command(
        ["python", "ingestion/load_dim_location.py"],
        "Populate dim_location",
    )
    run_command(
        ["python", "ingestion/load_dim_date.py"],
        "Populate dim_date",
    )
    run_command(
        ["python", "ingestion/load_dim_driver.py"],
        "Populate dim_driver",
    )
    run_command(
        ["python", "ingestion/load_dim_rider.py"],
        "Populate dim_rider",
    )

    # Step 6: Update fact table dimension keys
    run_command(
        ["python", "ingestion/update_fct_ride_events_dimension_keys.py"],
        "Update dimension keys in fct_ride_events",
    )
    run_command(
        ["python", "ingestion/update_fct_trips_dimension_keys.py"],
        "Update dimension keys in fct_trips",
    )
    run_command(
        ["python", "ingestion/update_fct_payments_dimension_keys.py"],
        "Update dimension keys in fct_payments",
    )

    print("\n=== Recovery pipeline completed successfully ===")


if __name__ == "__main__":
    main()
