#!/usr/bin/env python
from argparse import ArgumentParser
import subprocess
import json
import logging
import time

import redis
import toml


parser = ArgumentParser(description="save pbs data into redis cache")
parser.add_argument('-c', '--config', default='/etc/pbs_cache.toml', help='config file')


def safety_loads(data: str) -> dict:
    """
    skip error line
    """
    while True:
        try:
            d = json.loads(data)
            return d
        except json.decoder.JSONDecodeError as e:
            l = data.split('\n')
            l.pop(e.lineno - 1)
            data = '\n'.join(l)


if __name__ == '__main__':
    logging.info('start syncing pbs data')
    args = parser.parse_args()
    try:
        with open(args.config) as f:
            config = toml.load(f)
    except Exception as e:
        logging.error(f"can not load configuration: {str(e)}")
        exit(-1)
    redis_conf = config['redis']
    interval = config['interval']
    r = redis.Redis(**redis_conf)
    j = r.json()
    while True:
        logging.info('getting data from pbs server')
        pbs_server_data = safety_loads(subprocess.getoutput(f'/opt/pbs/bin/qstat -Bf -F json'))
        pbs_node_data = safety_loads(subprocess.getoutput(f'/opt/pbs/bin/pbsnodes -avj -F json'))
        pbs_job_data = safety_loads(subprocess.getoutput(f'/opt/pbs/bin/qstat -f -F json'))
        pbs_queue_data = safety_loads(subprocess.getoutput(f'/opt/pbs/bin/qstat -Qf -F json'))
        server = pbs_server_data['pbs_server']
        pbs_data = json.dumps({**pbs_server_data, **pbs_node_data, **pbs_queue_data, **pbs_job_data})
        logging.info('saving data to redis')
        j.set(server, '$', pbs_data)
        time.sleep(interval)