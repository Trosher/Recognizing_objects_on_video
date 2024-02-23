#!/bin/bash

set -e

exec python3 /app/src/server_fast_api.py & \
exec python3 /app/src/server_get_video.py & \
exec python3 /app/src/server_yolo_analys.py