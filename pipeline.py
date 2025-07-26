# encoding=utf8
import datetime
from distutils.version import StrictVersion
import hashlib
import os.path
import random
from seesaw.config import realize, NumberConfigValue
from seesaw.externalprocess import ExternalProcess
from seesaw.item import ItemInterpolation, ItemValue
from seesaw.task import SimpleTask, LimitConcurrent
from seesaw.tracker import GetItemFromTracker, PrepareStatsForTracker, \
    UploadWithTracker, SendDoneToTracker
import shutil
import socket
import subprocess
import sys
import time
import string
import json
import requests
from base64 import b64encode
import re

import seesaw
from seesaw.externalprocess import WgetDownload
from seesaw.pipeline import Pipeline
from seesaw.project import Project
from seesaw.util import find_executable

if StrictVersion(seesaw.__version__) < StrictVersion('0.8.5'):
    raise Exception('This pipeline needs seesaw version 0.8.5 or higher.')


###########################################################################
# Find a useful Wget+Lua executable.
#
# WGET_AT will be set to the first path that
# 1. does not crash with --version, and
# 2. prints the required version string

WGET_AT = find_executable(
    'Wget+AT',
    [
        'GNU Wget 1.21.3-at.20241119.01'
    ],
    [
        './wget-at',
        '/home/warrior/data/wget-at'
    ]
)

if not WGET_AT:
    raise Exception('No usable Wget+At found.')


###########################################################################
# The version number of this pipeline definition.
#
# Update this each time you make a non-cosmetic change.
# It will be added to the WARC files and reported to the tracker.
VERSION = '20250111.02'
USER_AGENT = 'Archiveteam (https://wiki.archiveteam.org/; communicate at https://webirc.hackint.org/#ircs://irc.hackint.org/#archiveteam)'
TRACKER_ID = 'cohost'
TRACKER_HOST = 'legacy-api.arpa.li'
MULTI_ITEM_SIZE = 20


###########################################################################
# This section defines project-specific tasks.
#
# Simple tasks (tasks that do not need any concurrency) are based on the
# SimpleTask class and have a process(item) method that is called for
# each item.
class CheckIP(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, 'CheckIP')
        self._counter = 0

    def process(self, item):
        # NEW for 2014! Check if we are behind firewall/proxy

        if self._counter <= 0:
            item.log_output('Checking IP address.')
            ip_set = set()

            ip_set.add(socket.gethostbyname('twitter.com'))
            ip_set.add(socket.gethostbyname('facebook.com'))
            ip_set.add(socket.gethostbyname('youtube.com'))
            ip_set.add(socket.gethostbyname('microsoft.com'))
            ip_set.add(socket.gethostbyname('icanhas.cheezburger.com'))
            ip_set.add(socket.gethostbyname('archiveteam.org'))

            if len(ip_set) != 6:
                item.log_output('Got IP addresses: {0}'.format(ip_set))
                item.log_output(
                    'Are you behind a firewall/proxy? That is a big no-no!')
                raise Exception(
                    'Are you behind a firewall/proxy? That is a big no-no!')


            # NEW for 2021! More network checks
            # 1 - TOR
            # if "Congratulations" in requests.get("https://check.torproject.org/").text:
            #   msg = "You seem to be using TOR."
            #   item.log_output(msg)
            #   raise Exception(msg)


            # 2 - NXDOMAIN hijacking (could be eliminated for some projects)
            try:
              socket.gethostbyname(hashlib.sha1(TRACKER_ID.encode('utf8')).hexdigest()[:6] + ".nonexistent-subdomain.archiveteam.org")
              msg = "You seem to be experiencing NXDOMAIN hijacking."
              item.log_output(msg)
              raise Exception(msg)
            except socket.gaierror:
              pass

        # Check only occasionally
        if self._counter <= 0:
            self._counter = 10
        else:
            self._counter -= 1


class PrepareDirectories(SimpleTask):
    def __init__(self, warc_prefix):
        SimpleTask.__init__(self, 'PrepareDirectories')
        self.warc_prefix = warc_prefix

    def process(self, item):
        item_name = item['item_name']
        item_name_hash = hashlib.sha1(item_name.encode('utf8')).hexdigest()
        escaped_item_name = item_name_hash
        dirname = '/'.join((item['data_dir'], escaped_item_name))

        if os.path.isdir(dirname):
            shutil.rmtree(dirname)

        os.makedirs(dirname)

        item['item_dir'] = dirname
        item['warc_file_base'] = '-'.join([
            self.warc_prefix,
            item_name_hash,
            time.strftime('%Y%m%d-%H%M%S')
        ])

        open('%(item_dir)s/%(warc_file_base)s.warc.gz' % item, 'w').close()
        open('%(item_dir)s/%(warc_file_base)s_data.txt' % item, 'w').close()


class MoveFiles(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, 'MoveFiles')

    def process(self, item):
        os.rename('%(item_dir)s/%(warc_file_base)s.warc.gz' % item,
            '%(data_dir)s/%(warc_file_base)s.warc.gz' % item)
        os.rename('%(item_dir)s/%(warc_file_base)s_data.txt' % item,
            '%(data_dir)s/%(warc_file_base)s_data.txt' % item)

        shutil.rmtree('%(item_dir)s' % item)


def get_hash(filename):
    with open(filename, 'rb') as in_file:
        return hashlib.sha1(in_file.read()).hexdigest()

class MaybeSendDoneToTracker(SendDoneToTracker):
    def enqueue(self, item):
        if len(item['item_name']) == 0:
            return self.complete_item(item)
        return super(MaybeSendDoneToTracker, self).enqueue(item)

CWD = os.getcwd()
PIPELINE_SHA1 = get_hash(os.path.join(CWD, 'pipeline.py'))
LUA_SHA1 = get_hash(os.path.join(CWD, 'cohost.lua'))

def stats_id_function(item):
    d = {
        'pipeline_hash': PIPELINE_SHA1,
        'lua_hash': LUA_SHA1,
        'python_version': sys.version,
    }

    return d


class WgetArgs(object):
    def realize(self, item):
        wget_args = [
            WGET_AT,
            '-U', USER_AGENT,
            '-nv',
            '--host-lookups', 'dns',
            '--hosts-file', '/dev/null',
            '--resolvconf-file', '/dev/null',
            '--dns-servers', '9.9.9.10,149.112.112.10,2620:fe::10,2620:fe::fe:10',
            '--reject-reserved-subnets',
            '--content-on-error',
            '--load-cookies', 'cookies.txt',
            '--lua-script', 'cohost.lua',
            '-o', ItemInterpolation('%(item_dir)s/wget.log'),
            #'--debug',
            '--no-check-certificate',
            '--output-document', ItemInterpolation('%(item_dir)s/wget.tmp'),
            '--truncate-output',
            '-e', 'robots=off',
            '--rotate-dns',
            '--recursive', '--level=inf',
            '--no-parent',
            '--page-requisites',
            '--timeout', '90',
            '--tries', 'inf',
            '--domains', 'cohost.org',
            '--span-hosts',
            '--waitretry', '90',
            '--warc-file', ItemInterpolation('%(item_dir)s/%(warc_file_base)s'),
            '--warc-header', 'operator: Archive Team',
            '--warc-header', 'x-wget-at-project-version: ' + VERSION,
            '--warc-header', 'x-wget-at-project-name: ' + TRACKER_ID,
            '--warc-dedup-url-agnostic'
        ]
        
        item_names = item['item_name'].split('\0')
        assert len(item_names) <= MULTI_ITEM_SIZE, "Basic check, got " + b64encode(item['item_name'].encode("utf-8")).decode("utf-8")
        start_urls = []
        item_names_table = []
        
        # Point of this function is to keep these together
        def set_start_url(item_type, item_value, start_url):
            start_urls.append(start_url)
            item_names_table.append([item_type, item_value])

        item_names_to_submit = item_names.copy()
        for item_name in item_names:
            item_name = item_name.replace("\n", "")
            wget_args.extend(['--warc-header', 'x-wget-at-project-item-name: '+item_name])
            wget_args.append('item-name://' + item_name)
            if ":" in item_name:
                item_type, item_value = item_name.split(':', 1)
            else:
                item_type, item_value = "dummy", item_name
            if item_type == 'user':
                if not re.match(r"^[0-9a-zA-Z\-]+(\+\d+)?$", item_value):
                    print("Skipping invalid item", item_value)
                    continue
                if m := re.match(r"^([0-9a-zA-Z\-]+)\+(\d+)$", item_value):
                    wget_args.extend(['--warc-header', 'cohost-user: ' + item_value])
                    url = f'https://cohost.org/{m.group(1)}?page={m.group(2)}'
                    wget_args.append(url)
                    set_start_url(item_type, item_value, url)
                else:
                    wget_args.extend(['--warc-header', 'cohost-user: ' + item_value])
                    wget_args.append(f'https://cohost.org/{item_value}')
                    set_start_url(item_type, item_value, f'https://cohost.org/{item_value}')
            elif item_type == "userfix1":
                wget_args.extend(['--warc-header', 'cohost-userfix1: ' + item_value])
                wget_args.append(f'https://cohost.org/{item_value}')
                set_start_url(item_type, item_value, f'https://cohost.org/{item_value}')
            elif item_type == 'userfix2':
                wget_args.extend(['--warc-header', 'cohost-userfix2: ' + item_value])
                if m := re.match(r"^([0-9a-zA-Z\-]+)\+(\d+)$", item_value):
                    url = f'https://cohost.org/{m.group(1)}?page={m.group(2)}'
                    wget_args.append(url)
                    set_start_url(item_type, item_value, url)
                else:
                    wget_args.append(f'https://cohost.org/{item_value}')
                    set_start_url(item_type, item_value, f'https://cohost.org/{item_value}')
            elif item_type == "usertag":
                user, tag = item_value.split("/", 1)
                wget_args.extend(['--warc-header', 'cohost-user-tag: ' + item_value])
                url = f'https://cohost.org/{user}/tagged/{tag}'
                wget_args.append(url)
                set_start_url(item_type, item_value, url)
            elif item_type == "tag":
                wget_args.extend(['--warc-header', 'cohost-tag: ' + item_value])
                wget_args.append(f'https://cohost.org/rc/tagged/{item_value}')
                set_start_url(item_type, item_value, f'https://cohost.org/rc/tagged/{item_value}')
            elif item_type == "tagext":
                start_offset, timestamp, tag_name = item_value.split("/", 2)
                url = f"https://cohost.org/rc/tagged/{tag_name}?refTimestamp={timestamp}&skipPosts={start_offset}"
                wget_args.extend(['--warc-header', 'cohost-tagext: ' + item_value])
                wget_args.append(url)
                
                set_start_url(item_type, item_value, url)
            elif item_type == "http" or item_type == "https":
                # Tracker item name is "http" and "https" to take care of some raw URLs that somehow ended up queued there as items before this type was added
                # But the name used by the script is "url:[value]"
                wget_args.extend(['--warc-header', 'cohost-url: ' + item_name])
                wget_args.append(item_name)
                set_start_url("url", item_name, item_name)
            elif item_type == "dummy":
                wget_args.extend(['--warc-header', 'cohost-user-dummy: ' + item_value])
                url = 'https://cohost.org/'
                wget_args.append(url)
                set_start_url(item_type, item_value, url)
            elif item_type == "post":
                # This item type is only used for testing; does not get everything to play back a post but causes a substantial portion of the logic to run
                wget_args.extend(['--warc-header', 'cohost-post: ' + item_value])
                wget_args.append(f'https://cohost.org/{item_value}')
                set_start_url(item_type, item_value, f'https://cohost.org/{item_value}')
            else:
                raise ValueError('item_type not supported.')

        item['item_name'] = '\0'.join(item_names_to_submit)

        item['start_urls'] = json.dumps(start_urls)
        item['item_names_table'] = json.dumps(item_names_table)

        assert len(item['item_name'].split('\0')) <= MULTI_ITEM_SIZE, "Final size " + b64encode(item['item_name'].encode("utf-8")).decode("utf-8")

        if 'bind_address' in globals():
            wget_args.extend(['--bind-address', globals()['bind_address']])
            print('')
            print('*** Wget will bind address at {0} ***'.format(
                globals()['bind_address']))
            print('')

        return realize(wget_args, item)

###########################################################################
# Initialize the project.
#
# This will be shown in the warrior management panel. The logo should not
# be too big. The deadline is optional.
project = Project(
    title = 'cohost',
    project_html = '''
    <img class="project-logo" alt="logo" src="https://wiki.archiveteam.org/images/4/41/Cohost_logo.png" height="50px"/>
    <h2>Cohost <span class="links"><a href="https://cohost.org/">Website</a> &middot; <a href="http://tracker.archiveteam.org/cohost/">Leaderboard</a></span></h2>
    ''',
    utc_deadline = datetime.datetime(2025,1,1, 0,0,0))

pipeline = Pipeline(
    CheckIP(),
    GetItemFromTracker('http://{}/{}/multi={}/'
        .format(TRACKER_HOST, TRACKER_ID, MULTI_ITEM_SIZE),
        downloader, VERSION),
    PrepareDirectories(warc_prefix='cohost'),
    WgetDownload(
        WgetArgs(),
        max_tries=1,
        accept_on_exit_code=[0, 4, 8],
        env={
            'item_dir': ItemValue('item_dir'),
            'warc_file_base': ItemValue('warc_file_base'),
            'start_urls': ItemValue('start_urls'),
            'item_names_table': ItemValue('item_names_table'),
            'LANG': 'en_US.UTF-8'
        }
    ),
    PrepareStatsForTracker(
        defaults={'downloader': downloader, 'version': VERSION},
        file_groups={
            'data': [
                ItemInterpolation('%(item_dir)s/%(warc_file_base)s.warc.gz')
            ]
        },
        id_function=stats_id_function,
    ),
    MoveFiles(),
    LimitConcurrent(NumberConfigValue(min=1, max=20, default='2',
        name='shared:rsync_threads', title='Rsync threads',
        description='The maximum number of concurrent uploads.'),
        UploadWithTracker(
            'http://%s/%s' % (TRACKER_HOST, TRACKER_ID),
            downloader=downloader,
            version=VERSION,
            files=[
                ItemInterpolation('%(data_dir)s/%(warc_file_base)s.warc.gz'),
                ItemInterpolation('%(data_dir)s/%(warc_file_base)s_data.txt')
            ],
            rsync_target_source_path=ItemInterpolation('%(data_dir)s/'),
            rsync_extra_args=[
                '--recursive',
                '--partial',
                '--partial-dir', '.rsync-tmp',
                '--min-size', '1',
                '--no-compress',
                '--compress-level', '0'
            ]
        ),
    ),
    MaybeSendDoneToTracker(
        tracker_url='https://%s/%s' % (TRACKER_HOST, TRACKER_ID),
        stats=ItemValue('stats')
    )
)
