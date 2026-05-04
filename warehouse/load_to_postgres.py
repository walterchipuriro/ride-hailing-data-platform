#!/usr/bin/env python3

import argparse
import json
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load ride event JSONL data into PostgreSQL staging.ride_events."
    )
    parser.add_argument(
        "--file",
        required=True,
        help="Path to the local JSONL file to load.",
    )
    return parser.parse_args()


def load_config() -> dict:
    load_dotenv()

    return {
        "host": os.getenv("POSTGRES_HOST"),
        "port": os.getenv("POSTGRES_PORT"),
        "database": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }


def connect_to_postgres(config: dict):
    return psycopg2.connect(
        host=config["host"],
        port=config["port"],
        dbname=config["database"],
        user=config["user"],
        password=config["password"],
    )


def read_jsonl(file_path: Path) -> list[dict]:
    events = []
    with file_path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Invalid JSON on line {line_number} in file {file_path}"
                ) from exc
    return events


def insert_events(connection, events: list[dict]) -> int:
    insert_sql = """
        INSERT INTO staging.ride_events (
            event_id,
            ride_id,
            driver_id,
            rider_id,
            event_type,
            event_timestamp,
            pickup_location,
            dropoff_location,
            fare_usd,
            payment_method,
            payment_status,
            event_version
        )
        VALUES (
            %(event_id)s,
            %(ride_id)s,
            %(driver_id)s,
            %(rider_id)s,
            %(event_type)s,
            %(event_timestamp)s,
            %(pickup_location)s,
            %(dropoff_location)s,
            %(fare_usd)s,
            %(payment_method)s,
            %(payment_status)s,
            %(event_version)s
        )
        ON CONFLICT (event_id) DO NOTHING;
    """

    with connection.cursor() as cursor:
        cursor.executemany(insert_sql, events)

    connection.commit()
    return len(events)


def main() -> None:
    args = parse_args()
    file_path = Path(args.file)

    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    config = load_config()
    events = read_jsonl(file_path)

    if not events:
        print("No events found in file.")
        return

    connection = connect_to_postgres(config)
    try:
        inserted_count = insert_events(connection, events)
    finally:
        connection.close()

    print(f"Loaded {inserted_count} events into staging.ride_events")
    print(f"Source file: {file_path}")


if __name__ == "__main__":
    main()
