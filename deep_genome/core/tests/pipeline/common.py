# Copyright (C) 2016 VIB/BEG/UGent - Tim Diels <timdiels.m@gmail.com>
# 
# This file is part of Deep Genome.
# 
# Deep Genome is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# Deep Genome is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public License
# along with Deep Genome.  If not, see <http://www.gnu.org/licenses/>.

from contextlib import contextmanager
from pathlib import Path
import logging
import re
import os

def assert_task_log(caplog, type_name, id_, events):
    '''
    Assert log contains task log messages in given order
    '''
    return assert_task_log_(caplog, [(type_name, id_, event) for event in events])
    
@contextmanager
def assert_task_log_(caplog, events):
    '''
    Assert log contains task log messages in given order
    '''
    # collect log difference
    original_count = len(caplog.text().splitlines())
    with caplog.atLevel(logging.INFO, logger='deep_genome.core.pipeline._local'):
        yield
    lines = caplog.text().splitlines()[original_count:]
    
    # assert
    events_seen = []
    pattern = r'(persisted_call|execute|drmaa_job)\[(.+)\]: (started|failed|finished|cancelling|cancelled)'
    for line in lines:
        match = re.search(pattern, line)
        if match:
            event = list(match.groups())
            event[1] = int(event[1])
            event = tuple(event)
            assert event not in events_seen, 'Event happens twice: {}'.format(event)
            events_seen.append(event)
    assert events_seen == events
    
def assert_is_read_only(directory):
    '''
    Assert directory and its descendants are read only
    '''
    for dir_, _, files in os.walk(str(directory)):
            dir_ = Path(dir_)
            assert (dir_.stat().st_mode & 0o777) == 0o500
            for file in files:
                assert ((dir_ / file).stat().st_mode & 0o777) == 0o400