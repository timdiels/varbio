# Copyright (C) 2015 VIB/BEG/UGent - Tim Diels <timdiels.m@gmail.com>
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

from chicken_turtle_util.test import temp_dir_cwd  # @UnusedImport
from deep_genome.core import patch
import logging
import pytest

# http://stackoverflow.com/a/30091579/1031434
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL) # Ignore SIGPIPE

@pytest.fixture(autouse=True, scope='session')
def common_init():
    patch()

    # log levels
    logging.getLogger('chicken_turtle_util').setLevel(logging.INFO)
