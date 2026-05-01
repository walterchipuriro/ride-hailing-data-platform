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


def create_kafka_producer() -> KafkaProducer:
    """
    Create a Kafka producer.

    This is the connection between Python and Kafka.
    Since this script runs on your laptop, we use localhost:9092.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def send_event_to_kafka(producer: KafkaProducer, event: dict) -> None:
    """
    Send one ride event to Kafka.
    """
    producer.send(
        KAFKA_TOPIC,
        key=event["ride_id"],
        value=event,
    )

    producer.flush()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate synthetic ride-hailing events and send them to Kafka.")
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if args.count <= 0:
        raise ValueError("Event count must be greater than 0.")

    producer = create_kafka_producer()

    print(f"Sending {args.count} ride events to Kafka topic: {KAFKA_TOPIC}")
    print(f"Kafka bootstrap server: {KAFKA_BOOTSTRAP_SERVERS}")

    for index in range(1, args.count + 1):
        event = generate_event(index)
        send_event_to_kafka(producer, event)

        print(f"Sent event {index}: {event}")

        time.sleep(args.delay)

    producer.close()

    print("Finished sending ride events to Kafka.")


if __name__ == "__main__":
    main()
