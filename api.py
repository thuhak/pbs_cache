#!/usr/bin/env python3.8
# author: thuhak.zhou@nio.com
"""
pbs cache restful api
"""
import secrets
import time
import logging
from enum import Enum
import json
from typing import Union, List

import jmespath
import toml
import redis
from fastapi import FastAPI, Depends, HTTPException, status, Query
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.responses import JSONResponse
from python_freeipa import ClientMeta

with open('/etc/pbs_cache.toml') as f:
    config = toml.load(f)
app = FastAPI()
security = HTTPBasic()
location = config['location']
redis_conf = config['redis'][location]
logger = logging.getLogger()
ipa = ClientMeta(config['ipa']['host'], verify_ssl=False)
conn = redis.ConnectionPool(**redis_conf)
replacement = [('.', '_'), ('[', '_'), (']', '')]


class Subject(Enum):
    Server = 'Server'
    Queue = 'Queue'
    Jobs = 'Jobs'
    nodes = 'nodes'


class UserInfo(Enum):
    jobs = 'jobs'
    groups = 'groups'


Site = Enum('Site', {k['location']: k['location'] for k in config['site']})


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
    timestamp = j.get(f'pbs_{site}', '.timestamp')
    if not timestamp:
        result['result'] = False
        result['error_msg'] = f'invalid site {site}'
    elif time.time() - timestamp > 120:
        result['result'] = False
        result['error_msg'] = 'pbs info too old'
    return result


@app.get('/pbs')
def get_site_list(cred=Depends(get_current_username)):
    """
    get HPC sites
    """
    return {'result': True, 'site': config['site']}


@app.get('/pbs/{site}')
def get_full_data(site: Site, cred=Depends(get_current_username)):
    """
    get all data of site
    """
    result = {'result': True}
    site = site.name
    try:
        r = redis.Redis(connection_pool=conn)
        j = r.json()
        check = check_expire(j, site)
        if check['result'] is False:
            return check
        data = j.get(f'pbs_{site}', '$')
        result['data'] = data
    except Exception as e:
        result = {'result': False, 'error_msg': f'backend failure, {str(e)}'}
    return result


@app.get('/pbs/{site}/{subject}')
def get_list(site: Site, subject: Subject, cred=Depends(get_current_username)):
    """
    get all data for the specified subject
    """
    result = {'result': True}
    site = site.name
    try:
        r = redis.Redis(connection_pool=conn)
        j = r.json()
        check = check_expire(j, site)
        if check['result'] is False:
            return check
        data = j.get(f'pbs_{site}', f'.{subject.name}')
        keys = list(data.keys())
        if subject is Subject.Jobs:
            result['count'] = len(keys)
            result['data'] = [s.replace('_', '.', 1) for s in keys]
        elif subject is Subject.nodes:
            result['count'] = len(keys)
            result['data'] = list({s.split('_')[0] for s in keys})
        elif subject is Subject.Queue:
            result['count'] = len(keys)
            result['data'] = keys
        else:
            result['data'] = list(data.values())[0]
    except Exception as e:
        result = {'result': False, 'error_msg': f'backend failure, {str(e)}'}
    return result


@app.get('/pbs/{site}/{subject}/{name}')
def get_data(site: Site, subject: Subject, name: str, item: Union[List[str], None] = Query(default=None),
             cred=Depends(get_current_username)):
    """
    get detail data
    """
    result = {'result': True}
    site = site.name
    name = trans_key(name)
    try:
        r = redis.Redis(connection_pool=conn)
        j = r.json()
        check = check_expire(j, site)
        if check['result'] is False:
            return check
        root = '$' if name == '*' or item else ''
        if subject is Subject.nodes:
            search_str = f'$.nodes.*[?(@.Mom=="{name}")]'
        else:
            search_str = f'{root}.{subject.name}.{name}'
        if item:
            search_str += f'.{json.dumps(item)}'

        print(item)
        print(f'searching expression:{search_str}')
        logger.debug(f'searching expression:{search_str}')
        data = j.get(f'pbs_{site}', search_str)
        result['data'] = data
    except Exception as e:
        result = {'result': False, 'error_msg': f'backend failure, {str(e)}'}
    return result


@app.get('/user')
def get_user_list(group: str = 'hpc', cred=Depends(get_current_username)):
    """
    get user list
    """
    result = {'result': True}
    try:
        ipa.login(config['ipa']['user'], config['ipa']['password'])
        user_info = ipa.group_show(group)
        result['data'] = jmespath.search('result.member_user', user_info)
    except Exception as e:
        result['result'] = False
        result['error_msg'] = f'{str(e)}'
        return JSONResponse(status_code=404, content=result)
    return result


@app.get('/user/{username}')
def get_user_info(username: str, info: UserInfo = UserInfo.groups, cred=Depends(get_current_username)):
    """
    get user data
    """
    result = {'result': True}
    data = {}
    print(info)
    if info is UserInfo.groups:
        try:
            ipa.login(config['ipa']['user'], config['ipa']['password'])
            user_data = ipa.user_show(username)['result']
            group = user_data.get('memberof_group', []) + user_data.get('memberofindirect_group', [])
            if 'hpc' not in group:
                result['result'] = False
                result['error_msg'] = 'invalid user'
                return JSONResponse(status_code=404, content=result)
            data['group'] = group
            gid = user_data['gidnumber'][0]
            if user_data['uidnumber'][0] == gid:
                data['main_group'] = None
            else:
                group_info = ipa.group_find(o_gidnumber=gid)
                data['main_group'] = jmespath.search('result[0].cn[0]', group_info) or None
        except Exception as e:
            result['result'] = False
            result['error_msg'] = f'{str(e)}'
            return JSONResponse(status_code=404, content=result)
    elif info is UserInfo.jobs:
        try:
            r = redis.Redis(connection_pool=conn)
            j = r.json()
        except Exception as e:
            return {'result': False, 'error_msg': f'backend failure, {str(e)}'}
        jobs = []
        for site_dict in config['site']:
            site = site_dict['location']
            check = check_expire(j, site)
            if check['result'] is False:
                return check
            job_search = f'$.Jobs.*[?(@.euser=="{username}")].id'
            job_list = j.get(f'pbs_{site}', job_search)
            jobs.extend(job_list)
        data['jobs'] = jobs
    result['data'] = data
    return result
