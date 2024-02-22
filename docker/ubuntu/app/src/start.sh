#!/bin/bash

set -e

exec python3 server_fast_api.py &
exec python3 server_get_video.py &
exec python3 server_fast_api.py &