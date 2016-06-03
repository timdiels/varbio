# Copyright (C) 2016 VIB/BEG/UGent - Tim Diels <timdiels.m@gmail.com>
# 
# This file is part of Chicken Turtle Util.
# 
# Chicken Turtle Util is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# Chicken Turtle Util is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public License
# along with Chicken Turtle Util.  If not, see <http://www.gnu.org/licenses/>.

'''
Test deep_genome.core.clean
'''

from chicken_turtle_util import path as path_
from deep_genome.core import clean
from pathlib import Path
    
def test_plain_text(temp_dir_cwd):
    '''
    Test all of clean.plain_text
    '''
    garbled = 'a null char\0 followed by\na newline, and a strange newline\r\r\nand finally some tabs\t\t done.'
    expected = 'a null char followed by\na newline, and a strange newline\nand finally some tabs\t done.'
    path = Path('temp')
    path_.write(path, garbled)
    with path.open() as f:
        assert clean.plain_text(f).read() == expected
    