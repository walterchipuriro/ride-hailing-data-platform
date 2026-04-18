CREATE TABLE IF NOT EXISTS staging.ride_events (
    event_id VARCHAR(100) PRIMARY KEY,
    ride_id VARCHAR(100) NOT NULL,
    driver_id VARCHAR(100),
    rider_id VARCHAR(100),
    event_type VARCHAR(50) NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    pickup_location VARCHAR(100),
    dropoff_location VARCHAR(100),
    fare_usd NUMERIC(10,2),
    payment_method VARCHAR(50),
    payment_status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS warehouse.fct_ride_events (
    event_id VARCHAR(100) PRIMARY KEY,
    ride_id VARCHAR(100) NOT NULL,
    driver_id VARCHAR(100),
    rider_id VARCHAR(100),
    event_type VARCHAR(50) NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    pickup_location VARCHAR(100),
    dropoff_location VARCHAR(100),
    fare_usd NUMERIC(10,2),
    payment_method VARCHAR(50),
    payment_status VARCHAR(50),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
