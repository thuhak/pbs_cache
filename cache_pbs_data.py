#!/usr/bin/env python3.8
# author: thuhak.zhou@nio.com
"""
save pbs data into redis cache
"""
from argparse import ArgumentParser
from collections import Counter
import subprocess
import json
import logging

import redis
import toml
import jmespath

parser = ArgumentParser(description=__doc__)
parser.add_argument('-c', '--config', default='/etc/pbs_cache.toml', help='config file')
parser.add_argument('-l', '--log_level', choices=['debug', 'info', 'error'], default='info', help='debug level')
parser.add_argument('-t', '--test', action='store_true', help='test mode')
replacement = [('.', '_'), ('[', '_'), (']', '')]


def safety_loads(data: str) -> dict:
    """
    fix pbs error
    """
    while True:
        try:
            d = json.loads(data)
            return d
        except json.decoder.JSONDecodeError as e:
            l = data.split('\n')
            l.pop(e.lineno - 1)
            data = '\n'.join(l)


def trans_key(key: str) -> str:
    for a, b in replacement:
        key = key.replace(a, b)
    return key


def get_resource_list(res: str, data: dict) -> list:
    raw = jmespath.search(f'resources_available."{res}"', data)
    return raw.split(',') if raw else []


class DevNode:
    """
    ibswitch, host, socket, vnode
    """

    def __init__(self, dev_type, name, full_cores=None):
        self.type = dev_type
        self.name = name
        self.full_cores = full_cores
        self.unused_cores = 0
        self.children = {}

    def add_child(self, node):
        self.children[node.name] = node

    def has_child(self, name) -> bool:
        return name in self.children

    def free_cores_group(self) -> [int]:
        if self.type == 'vnode':
            return [self.unused_cores]
        else:
            cores = [0]
            for c in self.children.values():
                c_free = c.free_cores_group()
                if sum(c_free) == c.full_cores:
                    cores[0] += c.full_cores
                else:
                    cores.extend(c_free)
            return sorted([c for c in cores if c != 0], reverse=True)


class QueueInfo:
    """
    pbs queue info
    """

    def __init__(self, queue: str, host: int, socket: int, vnode: int):
        self.root = DevNode('cluster', 'root')
        self.name = queue
        self.counter = Counter(min_cores=0,
                               max_cores=0,
                               waiting_cores=0,
                               offline_cores=0,
                               free_cores=0,
                               using_cores=0,
                               )
        self.full_cores_map = {
            "host": host,
            "socket": socket,
            "vnode": vnode
        }

    def add_vnode(self, all_cores: int, assigned_cores: int, is_offline: bool, is_private: bool, **devices):
        if is_offline:
            self.counter.update(offline_cores=all_cores)
            self.counter.update(max_cores=assigned_cores)
            if is_private:
                self.counter.update(min_cores=assigned_cores)
            return
        self.counter.update(max_cores=all_cores)
        if is_private:
            self.counter.update(min_cores=all_cores)
        free_cores = all_cores - assigned_cores
        self.counter.update(free_cores=free_cores)
        if free_cores == 0:
            return
        cur_node = self.root
        for dev_type in ("ibswitch", "host", "socket", "vnode"):
            dev_name = devices.get(dev_type)
            if not dev_name:
                continue
            if cur_node.has_child(dev_name):
                cur_node = cur_node.children[dev_name]
            else:
                node = DevNode(dev_type, dev_name, full_cores=self.full_cores_map.get(dev_type, 0))
                if dev_type == 'vnode':
                    node.unused_cores = free_cores
                cur_node.add_child(node)
                cur_node = node

    @property
    def load(self) -> float:
        """
        queue load = (using_cores + waiting_cores)/(using_cores + free_cores)
        """
        using_cores = self.counter['using_cores']
        waiting_cores = self.counter['waiting_cores']
        free_cores = self.counter['free_cores']
        try:
            l = round((using_cores + waiting_cores) / (using_cores + free_cores), 2)
        except ZeroDivisionError:
            l = 0.0
        return l

    def export(self) -> dict:
        """
        dump queue info to dict
        """
        result = {'queue': self.name}
        result.update(self.counter)
        result['free_cores_group'] = self.root.free_cores_group()
        result['load'] = self.load
        return result


def pbs_data_ex() -> dict:
    """
    parse and update pbs data
    """
    logging.debug('getting data from pbs')
    pbs_server = safety_loads(subprocess.getoutput(f'/opt/pbs/bin/qstat -Bf -F json'))
    pbs_queues = safety_loads(subprocess.getoutput(f'/opt/pbs/bin/qstat -Qf -F json'))
    pbs_nodes = safety_loads(subprocess.getoutput(f'/opt/pbs/bin/pbsnodes -avj -F json'))
    pbs_jobs = safety_loads(subprocess.getoutput(f'/opt/pbs/bin/qstat -f -F json'))
    logging.debug('parsing pbs data')
    extra_data = {}
    for q, queue_data in pbs_queues["Queue"].items():
        queue_config = jmespath.search(
            'resources_available.{host: ncpu_perhost, vnode: ncpu_pernode, socket: ncpu_pernuma}', queue_data)
        if not queue_config:
            logging.warning(f'queue {q} is not well configured')
            continue
        queue_data['name'] = q
        queue_data['apps'] = get_resource_list('App', queue_data)
        queue_data['teams'] = get_resource_list('Team', queue_data)
        extra_data[q] = QueueInfo(q, **queue_config)
    for vnode, node_data in pbs_nodes["nodes"].copy().items():
        if q := node_data.get('queue'):
            queues = [q]
        elif q := jmespath.search("resources_available.Qlist", node_data):
            queues = q.split(',')
        else:
            continue
        is_private = len(queues) == 1
        all_cores = jmespath.search('resources_available.ncpus', node_data)
        assigned_cores = jmespath.search('resources_assigned.ncpus', node_data) or 0
        devices = jmespath.search('resources_available.{ibswitch: ibswitch, host: host, socket: numa, vnode: vnode}',
                                  node_data)
        is_offline = node_data.get('state') == 'offline'
        for q in queues:
            if queue := extra_data.get(q):
                queue.add_vnode(all_cores, assigned_cores, is_offline, is_private, **devices)
        pbs_nodes['nodes'][trans_key(vnode)] = pbs_nodes['nodes'].pop(vnode)
    for job, job_data in pbs_jobs['Jobs'].copy().items():
        job_data['id'] = job
        q = job_data["queue"]
        if q not in extra_data:
            logging.error(f'queue {q} is not well configured')
            continue
        queue = extra_data[q]
        state = job_data['job_state']
        cores = jmespath.search('Resource_List.ncpus', job_data) or 0
        if state == 'R':
            queue.counter.update(using_cores=cores)
        elif state == 'Q':
            queue.counter.update(waiting_cores=cores)
        pbs_jobs['Jobs'].pop(job)
        pbs_jobs['Jobs'][trans_key(job)] = job_data
    # update raw data
    for q, queue_info in extra_data.items():
        adv_data = queue_info.export()
        logging.debug(f'queue {q} statistics:')
        logging.debug(json.dumps(adv_data, indent=True))
        pbs_queues['Queue'][q]['statistics'] = adv_data
    return {**pbs_server, **pbs_queues, **pbs_nodes, **pbs_jobs}


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
    location = config['location']
    conns = [(redis.ConnectionPool(**conf), host) for host, conf in config['redis'].items()]
    data = pbs_data_ex()
    if args.test:
        print(json.dumps(data, indent=True))
        exit(0)
    for con, host in conns:
        logging.info(f'saving data in {host}')
        try:
            r = redis.Redis(connection_pool=con)
            j = r.json()
            j.set(f'pbs_{location}', '$', data)
        except Exception as e:
            logging.error(f'redis at {host} error: {str(e)}')
