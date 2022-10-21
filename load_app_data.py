#!/usr/bin/env python
# author: thuhak.zhou@nio.com
"""
load app config to redis
"""
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import List, Optional
from argparse import ArgumentParser
import json
import logging

import toml
import redis

parser = ArgumentParser(description=__doc__)
parser.add_argument('-c', '--config', default='/etc/pbs_cache.toml', help='config file')
parser.add_argument('-a', '--app_path', default='/etc/app.d/', help='app config')
parser.add_argument('-l', '--log_level', choices=['debug', 'info', 'error'], default='info', help='debug level')
parser.add_argument('-t', '--test', action='store_true', help='test mode')


@dataclass
class App:
    Name: str
    DefaultMinCores: int = 0
    MaxCores: int = 0
    DefaultVersion: Optional[str] = None
    Versions: List[str] = field(default_factory=list)
    MPI: Optional[bool] = None
    OpenMP: int = 0
    MaxGPU: int = 0
    DefaultGPU: int = 0
    DefaultCoreWithGPU: int = -1


def load_app_data(config_path) -> dict:
    data = {}
    configs = Path(config_path)
    for conf in configs.glob('*.toml'):
        k = conf.name.split('.')[0]
        with open(conf) as f:
            v = toml.load(f)
        data[k] = asdict(App(**v))
    return data


if __name__ == '__main__':
    args = parser.parse_args()
    log_level = getattr(logging, args.log_level.upper())
    logging.basicConfig(level=log_level)
    logging.info('start syncing pbs data')
    try:
        with open(args.config) as f:
            config = toml.load(f)
    except Exception as e:
        logging.error(f"can not load configuration: {str(e)}")
        exit(-1)
    data = load_app_data(args.app_path)
    if args.test:
        print(json.dumps(data, indent=True))
        exit(0)
    location = config['location']
    conn = redis.ConnectionPool(**config['redis'][location])
    logging.info('save app config data')
    r = redis.Redis(connection_pool=conn)
    j = r.json()
    j.set('app', '$', data)

