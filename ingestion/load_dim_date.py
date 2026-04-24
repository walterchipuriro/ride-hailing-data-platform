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


def load_dim_date(connection) -> int:
    insert_sql = """
        INSERT INTO warehouse.dim_date (
            date_key,
            full_date,
            day_of_month,
            month_number,
            month_name,
            quarter_number,
            year_number,
            day_name,
            is_weekend
        )
        SELECT DISTINCT
            TO_CHAR(event_timestamp::date, 'YYYYMMDD')::int AS date_key,
            event_timestamp::date AS full_date,
            EXTRACT(DAY FROM event_timestamp)::int AS day_of_month,
            EXTRACT(MONTH FROM event_timestamp)::int AS month_number,
            TO_CHAR(event_timestamp, 'Month')::varchar(20) AS month_name,
            EXTRACT(QUARTER FROM event_timestamp)::int AS quarter_number,
            EXTRACT(YEAR FROM event_timestamp)::int AS year_number,
            TO_CHAR(event_timestamp, 'Day')::varchar(20) AS day_name,
            CASE
                WHEN EXTRACT(ISODOW FROM event_timestamp) IN (6, 7) THEN true
                ELSE false
            END AS is_weekend
        FROM staging.ride_events
        WHERE event_timestamp IS NOT NULL
        ON CONFLICT (date_key) DO NOTHING;
    """

    count_sql = "SELECT COUNT(*) FROM warehouse.dim_date;"

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
        total_count = load_dim_date(connection)
    finally:
        connection.close()

    print("Loaded dates into warehouse.dim_date")
    print(f"Current total rows in warehouse.dim_date: {total_count}")


if __name__ == "__main__":
    main()
