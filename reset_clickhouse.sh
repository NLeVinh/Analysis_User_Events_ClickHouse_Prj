#!/bin/bash

echo "Stopping ClickHouse container..."
docker compose down

echo "Removing data and logs..."
rm -rf clickhouse_data/ch1/*
rm -rf clickhouse_data/ch2/*
rm -rf clickhouse_data/ch3/*
rm -rf clickhouse_data/ch4/*
rm -rf keeper_data/*
rm -rf grafana_data/*
rm -rf clickhouse_logs/ch1/*
rm -rf clickhouse_logs/ch2/*
rm -rf clickhouse_logs/ch3/*
rm -rf clickhouse_logs/ch4/*

echo "Starting ClickHouse..."
docker compose up -d

echo "ClickHouse reset completed."