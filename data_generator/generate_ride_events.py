#!/usr/bin/env python3

import argparse
import json
import random
from datetime import datetime, timedelta
from pathlib import Path


EVENT_TYPES = [
    "ride_requested",
    "driver_assigned",
    "trip_started",
    "trip_completed",
    "payment_completed",
]

LOCATIONS = [
    "Harare CBD",
    "Avondale",
    "Borrowdale",
    "Mbare",
    "Highfield",
    "Westgate",
    "Belvedere",
    "Waterfalls",
    "Marlborough",
    "Greendale",
]

PAYMENT_METHODS = [
    "cash",
    "EcoCash",
    "card",
]


def generate_event(index: int) -> dict:
    """
    Generate one synthetic ride-hailing event.
    """
    event_type = random.choice(EVENT_TYPES)
    pickup_location = random.choice(LOCATIONS)
    dropoff_location = random.choice([loc for loc in LOCATIONS if loc != pickup_location])

    now = datetime.now()
    random_minutes_ago = random.randint(1, 1440)
    event_timestamp = now - timedelta(minutes=random_minutes_ago)

    fare_usd = None
    payment_status = "pending"

    if event_type in ["trip_completed", "payment_completed"]:
        fare_usd = round(random.uniform(3.0, 25.0), 2)

    if event_type == "payment_completed":
        payment_status = "paid"

    event = {
        "event_id": f"evt_{index:06d}",
        "ride_id": f"ride_{random.randint(1, 999999):06d}",
        "driver_id": f"drv_{random.randint(1, 9999):04d}",
        "rider_id": f"rdr_{random.randint(1, 9999):04d}",
        "event_type": event_type,
        "event_version": "1.0",
        "event_timestamp": event_timestamp.isoformat(timespec="seconds"),
        "pickup_location": pickup_location,
        "dropoff_location": dropoff_location,
        "fare_usd": fare_usd,
        "payment_method": random.choice(PAYMENT_METHODS),
        "payment_status": payment_status,
    }

    return event


def build_output_file_path(output_dir: Path) -> Path:
    """
    Build a timestamped JSONL output filename.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return output_dir / f"ride_events_{timestamp}.jsonl"


def write_events_to_jsonl(events: list[dict], output_file: Path) -> None:
    """
    Write events to a JSON Lines file.
    """
    with output_file.open("w", encoding="utf-8") as file:
        for event in events:
            file.write(json.dumps(event) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic ride-hailing events as JSON Lines.")
    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help="Number of events to generate. Default is 100.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.count <= 0:
        raise ValueError("Event count must be greater than 0.")

    output_dir = Path("data/local/raw/ride_events")
    output_file = build_output_file_path(output_dir)

    events = [generate_event(index=i + 1) for i in range(args.count)]
    write_events_to_jsonl(events, output_file)

    print(f"Generated {len(events)} ride events")
    print(f"Saved file: {output_file}")


if __name__ == "__main__":
    main()
