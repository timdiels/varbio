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
Test deep_genome.core.pipeline._local.execute
'''

from chicken_turtle_util import path as path_
from deep_genome.core.pipeline import execute, ExitCodeError
from pathlib import Path
import plumbum as pb
import asyncio
import pytest

def ps_aux_contains(term):
    for line in pb.local['ps']('aux').splitlines():
        if term in line:
            return True
    return False

@pytest.fixture(autouse=True)
def autouse(temp_dir_cwd):
    pass

@pytest.mark.asyncio
async def test_stdout():
    '''
    Save stdout to file
    '''
    stdout_file, _ = await execute(['echo', 'hi'])
    assert stdout_file.name == 'stdout'
    assert path_.read(stdout_file) == 'hi\n'
    
@pytest.mark.asyncio
async def test_stderr():
    '''
    Save stderr to file
    '''
    _, stderr_file = await execute(['sh', '-c', 'echo hi >&2'])
    assert stderr_file.name == 'stderr'
    assert path_.read(stderr_file) == 'hi\n'

@pytest.mark.asyncio
async def test_prefix():
    '''
    When given prefix, stdout/stderr filenames are prefixed
    '''
    stdout_file, stderr_file = await execute(['true'], prefix='johncena')
    assert stdout_file.name == 'johncena.stdout'
    assert stderr_file.name == 'johncena.stderr'

@pytest.mark.asyncio
async def test_directory_default():
    '''
    When no directory given, run in cwd
    '''
    await execute(['touch', 'file'])
    assert Path('file').exists()

@pytest.mark.asyncio
async def test_directory():
    '''
    When directory given, run in given directory
    '''
    directory = Path('directory')
    directory.mkdir()
    await execute(['touch', 'file'], directory=directory)
    assert (directory / 'file').exists()
    assert (directory / 'stdout').exists()
    assert (directory / 'stderr').exists()

@pytest.mark.asyncio
async def test_non_zero_exit():
    '''
    When job exits non-zero, raise
    '''
    with pytest.raises(ExitCodeError):
        await execute(['false'])
        
@pytest.mark.asyncio
async def test_cancel():
    '''
    When cancelled, kill subprocess
    '''
    token = 'aiojfdoajr2083jlkvmvko023i09fposkjisourpfsopkv'
    # When cancelled, asyncio.CancelledError is raised
    task = asyncio.ensure_future(execute(['sh', '-c', 'sleep 99999; ' + token]))
    asyncio.get_event_loop().call_later(1, task.cancel)
    with pytest.raises(asyncio.CancelledError):
        await task
        
    # and its subprocess was killed
    assert not ps_aux_contains(token)
        
