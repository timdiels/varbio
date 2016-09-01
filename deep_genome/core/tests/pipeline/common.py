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

import logging
import re
from contextlib import contextmanager

@contextmanager
def assert_task_log(caplog, type_name, name, events):
    '''
    Assert log contains task log messages in given order
    '''
    # collect log difference
    original_count = len(caplog.text().splitlines())
    with caplog.atLevel(logging.INFO, logger='deep_genome.core.pipeline._persisted'):
        yield
    lines = caplog.text().splitlines()[original_count:]
    
    # assert
    events_seen = []
    for line in lines:
        match = re.search(r"{} (started|failed|finished|cancelled): (.+)".format(type_name), line)
        if match:
            assert match.group(2) == name  # event happened on wrong task
            event = match.group(1)
            assert event not in events_seen, 'Event happens twice'
            events_seen.append(event)
    assert events_seen == events