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

@contextmanager
def assert_task_log(caplog, type_name, name, events):
    '''
    Assert log contains task log messages in given order
    '''
    # collect log difference
    original_count = len(caplog.text().splitlines())
    with caplog.atLevel(logging.INFO, logger='deep_genome.core.pipeline._local'):
        yield
    lines = caplog.text().splitlines()[original_count:]
    
    # assert
    if type_name == 'Job':
        name_name = 'Name'
    elif type_name == 'Coroutine':
        name_name = 'Repr'
    events_seen = []
    for line in lines:
        match = re.search(r"{} ([0-9]+) (started|failed|finished|cancelled). {}: (.+)".format(type_name, name_name), line)
        if match:
            assert match.group(3) == name  # event happened on wrong task
            event = match.group(2)
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