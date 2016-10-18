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
Common stuff used by the other modules

This module does not depend on any .* module.
'''

from chicken_turtle_util import path as path_
from contextlib import contextmanager
from pathlib import Path
import os

@contextmanager
def fresh_directory(directory):
    '''
    Context manager: Create empty directory and on exit make it read-only.
        
    If the directory exists, it is overwritten. When the context exits
    successfully (i.e. not due to an exception), the directory is made read-
    only.
    
    Parameters
    ----------
    directory : pathlib.Path
        Path of directory to create
    '''
    # Create fresh job dir
    path_.remove(directory, force=True)  # remove if exists
    os.makedirs(str(directory), exist_ok=True)
    
    #
    try:
        yield
    finally:
        # Make job data dir read only
        for dir_, _, files in os.walk(str(directory)):
            dir_ = Path(dir_)
            dir_.chmod(0o500)
            for file in files:
                (dir_ / file).chmod(0o400)
                
class ExitCodeError(Exception):
    
    '''
    Exit with error exit code
    '''
    
def format_exit_code_error(name, command, exit_code, stdout_file, stderr_file):
    if name:
        message = name + ' exited'
    else:
        message = 'Exited'
    message += ' with exit code {}'.format(exit_code)
    
    message += '\n\nCommand:\n{}'.format(' '.join(map(repr, command)))
    
    for std_file, name in ((stdout_file, 'stdout'), (stderr_file, 'stderr')):
        if std_file:
            content = path_.read(std_file).strip()
            if content:
                message += '\n\n{}:\n{}'.format(name, content)
    
    return message
        