#!/bin/bash
# Quick start script for the feed-handler aggregator

set -e

echo "Building aggregator..."
go build -o aggregator ./cmd/aggregator

echo "Starting aggregator..."
echo "Press Ctrl+C to stop"
echo ""

./aggregator "$@"
