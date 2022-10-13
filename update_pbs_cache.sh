#!/bin/bash

if [[ -f /opt/rh/rh-python38/enable ]]; then
  source /opt/rh/rh-python38/enable
fi

python3.8 cache_pbs_data.py