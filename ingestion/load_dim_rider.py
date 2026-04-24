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


def load_dim_rider(connection) -> int:
    insert_sql = """
        INSERT INTO warehouse.dim_rider (rider_id)
        SELECT DISTINCT rider_id
        FROM staging.ride_events
        WHERE rider_id IS NOT NULL
        ON CONFLICT (rider_id) DO NOTHING;
    """

    count_sql = "SELECT COUNT(*) FROM warehouse.dim_rider;"

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
        total_count = load_dim_rider(connection)
    finally:
        connection.close()

    print("Loaded riders into warehouse.dim_rider")
    print(f"Current total rows in warehouse.dim_rider: {total_count}")


if __name__ == "__main__":
    main()
