#!/usr/bin/env python3.8
# author: thuhak.zhou@nio.com
"""
save pbs data into redis cache
"""
from argparse import ArgumentParser
from collections import Counter
from typing import List
import subprocess
import json
import logging
import statistics

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

    def __init__(self, dev_type, name, full_cores=None, full_gpus=None):
        self.type = dev_type
        self.name = name
        self.full_cores = full_cores
        self.full_gpus = full_gpus
        self.unused_cores = 0
        self.unused_gpus = 0
        self.children = {}

    def add_child(self, node):
        self.children[node.name] = node

    def has_child(self, name) -> bool:
        return name in self.children

    def free_cores_group(self, gpu=False) -> List[int]:
        if self.type == 'vnode':
            return [self.unused_gpus if gpu else self.unused_cores]
        else:
            cores = [0]
            for c in self.children.values():
                c_free = c.free_cores_group(gpu)
                full_cores = c.full_gpus if gpu else c.full_cores
                if full_cores and sum(c_free) == full_cores and self.type != 'cluster':
                    cores[0] += full_cores
                else:
                    cores.extend(c_free)
            return sorted([c for c in cores if c != 0], reverse=True)


class BaseStat:
    def __init__(self):
        self.users = set()
        self.job_size = []
        self.counter = Counter(waiting_cores=0,
                               offline_cores=0,
                               free_cores=0,
                               using_cores=0,
                               running_jobs=0,
                               waiting_jobs=0,
                               free_gpus=0,
                               using_gpus=0,
                               waiting_gpus=0,
                               offline_gpus=0
                               )

    @property
    def load(self) -> float:
        """
        load = (using_cores + waiting_cores)/(using_cores + free_cores)
        """
        using_cores = self.counter['using_cores']
        waiting_cores = self.counter['waiting_cores']
        free_cores = self.counter['free_cores']
        try:
            l = round((using_cores + waiting_cores) / (using_cores + free_cores), 2)
        except ZeroDivisionError:
            l = 0.0
        return l

    @property
    def job_size_avg(self):
        return 0.0 if not self.job_size else round(statistics.mean(self.job_size), 2)

    def export(self) -> dict:
        result = {'user_count': len(self.users)}
        result.update(self.counter)
        result['job_size_avg'] = self.job_size_avg
        result['load'] = self.load
        return result


class ServerInfo(BaseStat):
    """
    pbs server info
    """

    def __init__(self):
        super(ServerInfo, self).__init__()
        self.counter.update(total_cores=0)
        self.counter.update(total_gpus=0)

    def add_vnode(self, all_cores: int, assigned_cores: int, all_gpus: int, assigned_gpus: int, is_offline: bool):
        self.counter.update(total_cores=all_cores, total_gpus=all_gpus)
        if is_offline:
            self.counter.update(offline_cores=all_cores, offline_gpus=all_gpus)
        free_cores = all_cores - assigned_cores
        free_gpus = all_gpus - assigned_gpus
        self.counter.update(free_cores=free_cores, free_gpus=free_gpus)


class QueueInfo(BaseStat):
    """
    pbs queue info
    """

    def __init__(self, queue: str, host: int, socket: int, vnode: int, gpus_vnode: int):
        super(QueueInfo, self).__init__()
        self.root = DevNode('cluster', 'root')
        self.name = queue
        self.counter.update(min_cores=0, max_cores=0, min_gpus=0, max_gpus=0)
        self.full_cores_map = {
            "host": host,
            "socket": socket,
            "vnode": vnode,
        }
        self.full_gpus_map = {'vnode': gpus_vnode,
                              'socket': socket * gpus_vnode // vnode,
                              'host': host * gpus_vnode // vnode
                              }

    def add_vnode(self, all_cores: int, assigned_cores: int, all_gpus: int, assigned_gpus: int, is_offline: bool,
                  is_private: bool, **devices):
        if is_offline:
            self.counter.update(offline_cores=all_cores, offline_gpus=all_gpus)
            self.counter.update(max_cores=assigned_cores, max_gpus=assigned_gpus)
            if is_private:
                self.counter.update(min_cores=assigned_cores, min_gpus=assigned_gpus)
            return
        self.counter.update(max_cores=all_cores, max_gpus=all_gpus)
        if is_private:
            self.counter.update(min_cores=all_cores, min_gpus=all_gpus)
        free_cores = all_cores - assigned_cores
        free_gpus = all_gpus - assigned_gpus
        self.counter.update(free_cores=free_cores, free_gpus=free_gpus)
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
                node = DevNode(dev_type, dev_name, full_cores=self.full_cores_map.get(dev_type, 0),
                               full_gpus=self.full_gpus_map.get(dev_type, 0))
                if dev_type == 'vnode':
                    node.unused_cores = free_cores
                    node.unused_gpus = free_gpus
                cur_node.add_child(node)
                cur_node = node

    def export(self) -> dict:
        """
        dump queue info to dict
        """
        result = super(QueueInfo, self).export()
        result['queue'] = self.name
        result['free_cores_group'] = self.root.free_cores_group()
        result['free_gpus_group'] = self.root.free_cores_group(gpu=True)
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
    server_info = ServerInfo()
    extra_queue_data = {}
    # queue init
    for q, queue_data in pbs_queues["Queue"].items():
        raw = jmespath.search(
            'resources_available.{host: ncpu_perhost, vnode: ncpu_pernode, socket: ncpu_pernuma, gpus_vnode: ngpu_pernode}',
            queue_data)
        queue_config = {k: v or 0 for k, v in raw.items()}
        queue_data['name'] = q
        queue_data['apps'] = get_resource_list('App', queue_data)
        queue_data['teams'] = get_resource_list('Team', queue_data)
        extra_queue_data[q] = QueueInfo(q, **queue_config)
    # add node info to queue
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
        all_gpus = jmespath.search('resources_available.ngpus', node_data) or 0
        assigned_gpus = jmespath.search('resources_assigned.ngpus', node_data) or 0
        devices = jmespath.search('resources_available.{ibswitch: ibswitch, host: host, socket: numa, vnode: vnode}',
                                  node_data)
        node_stat = node_data.get('state', '').split(',')
        is_offline = bool(set(node_stat) & {'down', 'offline'})
        server_info.add_vnode(all_cores, assigned_cores, all_gpus, assigned_gpus, is_offline)
        for q in queues:
            if queue := extra_queue_data.get(q):
                queue.add_vnode(all_cores, assigned_cores, all_gpus, assigned_gpus, is_offline, is_private, **devices)
        pbs_nodes['nodes'][trans_key(vnode)] = pbs_nodes['nodes'].pop(vnode)
    # add job info to queue
    for job, job_data in pbs_jobs['Jobs'].copy().items():
        job_data['id'] = job
        q = job_data["queue"]
        if q not in extra_queue_data:
            logging.error(f'queue {q} is not well configured')
            continue
        queue = extra_queue_data[q]
        state = job_data['job_state']
        cores, gpus = [n or 0 for n in jmespath.search('Resource_List.[ncpus, ngpus]', job_data)]
        if state == 'R':
            server_info.counter.update(using_cores=cores, running_jobs=1, using_gpus=gpus)
            queue.counter.update(using_cores=cores, running_jobs=1, using_gpus=gpus)
            user = job_data['euser']
            queue.users.add(user)
            server_info.users.add(user)
            jobsize = jmespath.search('Resource_List.ncpus', job_data) or 0
            queue.job_size.append(jobsize)
            server_info.job_size.append(jobsize)
        elif state == 'Q':
            server_info.counter.update(waiting_cores=cores, waiting_jobs=1, waiting_gpus=gpus)
            queue.counter.update(waiting_cores=cores, waiting_jobs=1, waiting_gpus=gpus)
        pbs_jobs['Jobs'].pop(job)
        pbs_jobs['Jobs'][trans_key(job)] = job_data
    # update server data
    for d in pbs_server['Server'].values():
        d['statistics'] = server_info.export()
    # update queue data
    for q, queue_info in extra_queue_data.items():
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
            r = redis.Redis(connection_pool=con, socket_timeout=5, socket_connect_timeout=2)
            j = r.json()
            j.set(f'pbs_{location}', '$', data)
        except Exception as e:
            logging.error(f'redis at {host} error: {str(e)}')
