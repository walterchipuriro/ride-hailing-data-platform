#!/usr/bin/env bash
docker exec -it \
  -e PGUSER=postgres \
  -e PGDATABASE=ride_hailing_dw \
  ride_hailing_postgres psql
