#!/usr/bin/env python3

import os

import psycopg2
from dotenv import load_dotenv


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


def move_data_to_warehouse(connection) -> int:
    insert_sql = """
        INSERT INTO warehouse.fct_ride_events (
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
        SELECT
            s.event_id,
            s.ride_id,
            s.driver_id,
            s.rider_id,
            s.event_type,
            s.event_timestamp,
            s.pickup_location,
            s.dropoff_location,
            s.fare_usd,
            s.payment_method,
            s.payment_status,
            s.event_version
        FROM staging.ride_events s
        ON CONFLICT (event_id) DO NOTHING;
    """

    count_sql = "SELECT COUNT(*) FROM warehouse.fct_ride_events;"

    with connection.cursor() as cursor:
        cursor.execute(insert_sql)
        cursor.execute(count_sql)
        total_count = cursor.fetchone()[0]

    connection.commit()
    return total_count


def main() -> None:
    config = load_config()
    connection = connect_to_postgres(config)

    try:
        total_count = move_data_to_warehouse(connection)
    finally:
        connection.close()

    print("Data moved from staging.ride_events to warehouse.fct_ride_events")
    print(f"Current total rows in warehouse.fct_ride_events: {total_count}")


if __name__ == "__main__":
    main()
