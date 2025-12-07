#!/bin/bash
# save as analyze.sh

echo "=== Speedtest Analysis ==="
echo "Current directory: $PWD"
cd ..
echo "Changed to directory: $PWD"

sudo docker-compose logs speedtest > analysis_speedtest.log