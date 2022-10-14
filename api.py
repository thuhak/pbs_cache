#!/usr/bin/env python3.8
# author: thuhak.zhou@nio.com
"""
pbs cache restful api
"""
import secrets
import time
import logging
from enum import Enum

import toml
import redis
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials


with open('/etc/pbs_cache.toml') as f:
    config = toml.load(f)
app = FastAPI()
security = HTTPBasic()
location = config['location']
redis_conf = config['redis'][location]
conn = redis.ConnectionPool(**redis_conf)
replacement = [('.', '_'), ('[', '_'), (']', '')]


class Subject(Enum):
    Server = 'Server'
    Queue = 'Queue'
    Jobs = 'Jobs'
    nodes = 'nodes'


def trans_key(key: str) -> str:
    for a, b in replacement:
        key = key.replace(a, b)
    return key


def get_current_username(credentials: HTTPBasicCredentials = Depends(security)):
    correct_username = secrets.compare_digest(credentials.username, config['api']['user'])
    correct_password = secrets.compare_digest(credentials.password, config['api']['password'])
    if not (correct_username and correct_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


def check_expire(j, site):
    result = {'result': True}
    timestamp = j.get(site, '.timestamp')
    if not timestamp:
        result['result'] = False
        result['error_msg'] = f'invalid site {site}'
    elif time.time() - timestamp > 120:
        result['result'] = False
        result['error_msg'] = 'pbs info too old'
    return result


@app.get('/')
def get_site_list(cred=Depends(get_current_username)):
    """
    get HPC sites
    """
    result = {'result': True}
    try:
        r = redis.Redis(connection_pool=conn)
        sites = r.keys()
        result['sites'] = sites
    except Exception as e:
        result = {'result': False, 'error_msg': f'backend failure, {str(e)}'}
    return result


@app.get('/{site}/')
def get_full_data(site: str, cred=Depends(get_current_username)):
    """
    get all data of site
    """
    result = {'result': True}
    try:
        r = redis.Redis(connection_pool=conn)
        j = r.json()
        check = check_expire(j, site)
        if check['result'] is False:
            return check
        data = j.get(site, '$')
        result['data'] = data
    except Exception as e:
        result = {'result': False, 'error_msg': f'backend failure, {str(e)}'}
    return result


@app.get('/{site}/{subject}/')
def get_subject_data(site: str, subject: Subject, cred=Depends(get_current_username)):
    """
    get all data for the specified subject
    """
    result = {'result': True}
    try:
        r = redis.Redis(connection_pool=conn)
        j = r.json()
        check = check_expire(j, site)
        if check['result'] is False:
            return check
        data = j.get(site, f'.{subject}')
        result[subject] = data
    except Exception as e:
        result = {'result': False, 'error_msg': f'backend failure, {str(e)}'}
    return result


@app.get('/{site}/{subject}/{name}/')
def get_detail_data(site: str, subject: Subject, name: str, item: str = None, cred=Depends(get_current_username)):
    """
    get detail data
    """
    result = {'result': True}
    name = trans_key(name)
    try:
        r = redis.Redis(connection_pool=conn)
        j = r.json()
        check = check_expire(j, site)
        if check['result'] is False:
            return check
        root = '$' if name == '*' else ''
        search_str = f'{root}.{subject.name}.{name}'
        if item:
            search_str += f'.{item}'
        logging.debug(f'searching expression:{search_str}')
        data = j.get(site, search_str)
        if not data:
            result['result'] = False
            result['error_msg'] = 'unable to search for data'
        else:
            result['data'] = data
    except Exception as e:
        result = {'result': False, 'error_msg': f'backend failure, {str(e)}'}
    return result
