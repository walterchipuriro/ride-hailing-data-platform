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


def update_dimension_keys(connection) -> None:
    payment_method_sql = """
        UPDATE warehouse.fct_payments f
        SET payment_method_key = d.payment_method_key
        FROM warehouse.dim_payment_method d
        WHERE f.payment_method = d.payment_method_name
          AND f.payment_method_key IS NULL;
    """

    payment_date_sql = """
        UPDATE warehouse.fct_payments f
        SET payment_date_key = d.date_key
        FROM warehouse.dim_date d
        WHERE f.payment_timestamp::date = d.full_date
          AND f.payment_date_key IS NULL;
    """

    with connection.cursor() as cursor:
        cursor.execute(payment_method_sql)
        cursor.execute(payment_date_sql)

    connection.commit()


def main() -> None:
    config = load_config()
    connection = connect_to_postgres(config)

    try:
        update_dimension_keys(connection)
    finally:
        connection.close()

    print("Updated dimension keys in warehouse.fct_payments")


if __name__ == "__main__":
    main()
