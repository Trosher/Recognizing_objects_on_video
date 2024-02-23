#!/bin/bash

set -e

exec python3 /app/ubuntu/app/src/server_yolo_analys.py &
exec python3 /app/ubuntu/app/src/server_get_video.py &
exec python3 /app/ubuntu/app/src/server_fast_api.py