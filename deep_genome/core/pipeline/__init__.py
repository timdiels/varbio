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

# TODO allow custom jobs dir on LocalJobServer but default to cache dir

'''
Utilities for building a pipeline

Features:

- concurrently execute code using the Python standard `asyncio` module
- concurrently execute executables locally and on clusters (which support a DRMAA interface: Grid Engine, ...)
- persist the results of each of the above
'''
#TODO in the guide, show how to do all the things: refer to a good asyncio intro, run some concurrent code that is persisted, show how Job works with LocalJobServer and DRMAAJobServer 
'''
A job can be run locally or on cluster via a DRMAA interface (e.g. Open Grid
Scheduler supports DRMAA). Each job represents a call to an executable with some
arguments, to be run locally or on the cluster. Each job is given its own clean
working directory (even when it was cancelled before, the next try will start
from a fresh directory again).
'''

from ._various import pipeline_cli, call_repr
from ._persisted import persisted
from ._job import Job, LocalJobServer, DRMAAJobServer

# TODO cache reset in core by using version numbers in task and job names. Or global version number thing (not related to app version).

# wishlist (nice to have but don't implement yet)
'''
TODO max concurrently submitted jobs is 30 (make customisable), because "If you plan to submit long jobs (over 60 mins), restrict yourself and submit at most 30 jobs."
Yes, test it (with a smaller max). {job1, job2} s, job1 f, job3 s, ... job2 made to be slower than job1

TODO warn if job takes less than duration (5 min by default)

TODO add job.reset: path_.remove(job.directory) and removes its entry from the
database. We track finishedness in the database, in some table there. Present
in table with job name <=> finished.
+ test
'''
