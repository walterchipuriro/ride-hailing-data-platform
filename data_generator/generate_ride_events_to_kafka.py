#!/usr/bin/env python3

import argparse
import json
import random
import time
from datetime import datetime, timedelta

from kafka import KafkaProducer


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

KAFKA_TOPIC = "ride_events"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"


def random_event_timestamp() -> str:
    now = datetime.now()
    random_minutes_ago = random.randint(1, 1440)
    event_timestamp = now - timedelta(minutes=random_minutes_ago)
    return event_timestamp.isoformat(timespec="seconds")


def generate_good_event(index: int) -> dict:
    """
    Generate one valid synthetic ride-hailing event.
    """
    event_type = random.choice(EVENT_TYPES)

    pickup_location = random.choice(LOCATIONS)
    dropoff_location = random.choice([loc for loc in LOCATIONS if loc != pickup_location])

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
        "event_timestamp": random_event_timestamp(),
        "pickup_location": pickup_location,
        "dropoff_location": dropoff_location,
        "fare_usd": fare_usd,
        "payment_method": random.choice(PAYMENT_METHODS),
        "payment_status": payment_status,
    }

    return event


def make_bad_event(event: dict) -> dict:
    """
    Take a good event and intentionally damage it.
    This helps test the rejected data lake path.
    """
    bad_event = event.copy()

    bad_case = random.choice([
        "missing_event_id",
        "missing_ride_id",
        "missing_driver_id",
        "missing_rider_id",
        "invalid_event_type",
        "invalid_timestamp",
        "missing_pickup_location",
        "missing_dropoff_location",
        "invalid_payment_method",
        "invalid_payment_status",
        "completed_event_missing_fare",
        "completed_event_negative_fare",
        "payment_completed_not_paid",
    ])

    if bad_case == "missing_event_id":
        bad_event.pop("event_id", None)

    elif bad_case == "missing_ride_id":
        bad_event.pop("ride_id", None)

    elif bad_case == "missing_driver_id":
        bad_event.pop("driver_id", None)

    elif bad_case == "missing_rider_id":
        bad_event.pop("rider_id", None)

    elif bad_case == "invalid_event_type":
        bad_event["event_type"] = "unknown_event"

    elif bad_case == "invalid_timestamp":
        bad_event["event_timestamp"] = "not-a-valid-date"

    elif bad_case == "missing_pickup_location":
        bad_event["pickup_location"] = None

    elif bad_case == "missing_dropoff_location":
        bad_event["dropoff_location"] = None

    elif bad_case == "invalid_payment_method":
        bad_event["payment_method"] = "gold_coins"

    elif bad_case == "invalid_payment_status":
        bad_event["payment_status"] = "unknown_status"

    elif bad_case == "completed_event_missing_fare":
        bad_event["event_type"] = random.choice(["trip_completed", "payment_completed"])
        bad_event["fare_usd"] = None
        if bad_event["event_type"] == "payment_completed":
            bad_event["payment_status"] = "paid"

    elif bad_case == "completed_event_negative_fare":
        bad_event["event_type"] = random.choice(["trip_completed", "payment_completed"])
        bad_event["fare_usd"] = -5.00
        if bad_event["event_type"] == "payment_completed":
            bad_event["payment_status"] = "paid"

    elif bad_case == "payment_completed_not_paid":
        bad_event["event_type"] = "payment_completed"
        bad_event["fare_usd"] = round(random.uniform(3.0, 25.0), 2)
        bad_event["payment_status"] = "pending"

    bad_event["bad_record_type"] = bad_case

    return bad_event


def create_kafka_producer(bootstrap_servers: str) -> KafkaProducer:
    """
    Create a Kafka producer.

    This script runs from your laptop, so localhost:9092 is correct.
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def send_event_to_kafka(producer: KafkaProducer, topic: str, event: dict) -> None:
    """
    Send one ride event to Kafka.
    """
    kafka_key = (
        event.get("ride_id")
        or event.get("event_id")
        or f"bad_record_{random.randint(1, 999999)}"
    )

    producer.send(
        topic,
        key=kafka_key,
        value=event,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic ride-hailing events and send them to Kafka."
    )

    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help="Number of events to generate. Default is 100.",
    )

    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Delay in seconds between events. Default is 1 second.",
    )

    parser.add_argument(
        "--bad-rate",
        type=float,
        default=0.15,
        help="Bad record rate between 0 and 1. Example: 0.25 means 25%% bad records.",
    )

    parser.add_argument(
        "--bootstrap-server",
        default=KAFKA_BOOTSTRAP_SERVERS,
        help="Kafka bootstrap server. Default is localhost:9092.",
    )

    parser.add_argument(
        "--topic",
        default=KAFKA_TOPIC,
        help="Kafka topic. Default is ride_events.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.count <= 0:
        raise ValueError("Event count must be greater than 0.")

    if args.delay < 0:
        raise ValueError("Delay must not be negative.")

    if args.bad_rate < 0 or args.bad_rate > 1:
        raise ValueError("Bad record rate must be between 0 and 1.")

    producer = create_kafka_producer(args.bootstrap_server)

    print(f"Sending {args.count} ride events to Kafka topic: {args.topic}")
    print(f"Kafka bootstrap server: {args.bootstrap_server}")
    print(f"Bad record rate: {args.bad_rate}")

    good_count = 0
    bad_count = 0

    for index in range(1, args.count + 1):
        event = generate_good_event(index)

        if random.random() < args.bad_rate:
            event = make_bad_event(event)
            record_status = "BAD"
            bad_count += 1
        else:
            record_status = "GOOD"
            good_count += 1

        send_event_to_kafka(producer, args.topic, event)

        print(f"Sent {record_status} event {index}: {event}")

        time.sleep(args.delay)

    producer.flush()
    producer.close()

    print("Finished sending ride events to Kafka.")
    print(f"Good events sent: {good_count}")
    print(f"Bad events sent: {bad_count}")


if __name__ == "__main__":
    main()