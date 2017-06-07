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
Cleaning of (file) input
'''

import plumbum as pb
import io

_sed = pb.local['sed']
_tr = pb.local['tr']

def plain_text(reader):
    '''
    Get sanitised contents of plain text file.

    - Remove null characters
    - Fix newlines, drop empty lines
    - Replace multiple tabs by single tab.

    Parameters
    ----------
    reader : io.BufferedReader of file
        Plain text file stream

    Returns
    -------
    io.BufferedReader
        Stream of sanitised text
    '''
    cmd = (_sed['-r', '-e', r's/[\x0]//g', '-e', r's/(\t)+/\t/g'] < reader) | _tr['-s', r'\r', r'\n'] 
    return io.TextIOWrapper(cmd.popen().stdout, encoding='UTF-8')
