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

'''
Test deep_genome.core.pipeline._common
'''

from deep_genome.core.pipeline import fresh_directory
from .common import assert_is_read_only
from pathlib import Path
import pytest

class TestFreshDirectory(object):
    
    @pytest.fixture
    def directory(self, temp_dir_cwd):
        return Path('directory')
    
    def test_happy_days(self, directory):
        '''
        When directory does not exist, create it, ...
        '''
        with fresh_directory(directory):
            (directory / 'file').touch()
        assert (directory / 'file').exists()
        assert_is_read_only(directory)
        
    def test_overwrite(self, directory):
        '''
        When directory exists, overwrite it
        '''
        directory.mkdir()
        (directory / 'dir').mkdir()
        (directory / 'dir/file').touch()
        with fresh_directory(directory):
            pass
        assert not list(directory.iterdir())
        assert_is_read_only(directory)
    
    def test_raise(self, directory):
        '''
        When raises, still make directory read only
        '''
        class Error(Exception):
            pass
        with pytest.raises(Error):
            with fresh_directory(directory):
                raise Error()
        assert directory.exists()
        assert_is_read_only(directory)
    
