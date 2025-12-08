# Install jq if needed
sudo apt-get install jq

# Analyze current logs
sudo docker-compose logs speedtest | python3 parse_speedtest.py

# Save analysis from log file
sudo docker-compose logs speedtest > speedtest.log
python3 parse_speedtest.py speedtest.log

# Export to CSV
python3 parse_speedtest.py speedtest.log --csv

# Real-time monitoring with jq
sudo docker-compose logs -f speedtest | grep --line-buffered "Result:" | while read line; do
    echo "$line" | sed 's/.*Result: //' | jq -r '"Download: \(.result.download/1000000 | floor)Mbps, Upload: \(.result.upload/1000000 | floor)Mbps"'
done
