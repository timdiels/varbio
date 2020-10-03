# Copyright (C) 2020 VIB/BEG/UGent - Tim Diels <tim@diels.me>
#
# This file is part of varbio.
#
# varbio is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# varbio is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with varbio.  If not, see <http://www.gnu.org/licenses/>.

'''
Common functions, ... May be imported by other varbio modules, but not the
other way around
'''

from contextlib import contextmanager
import chardet


class UserError(Exception):
    '''
    Error caused by user, e.g. invalid input

    Has user friendly message. The intent is for a CLI to show the message to
    the user without a stack trace (as those are scary). At the same time the
    API can still raise these errors directly to the API user; otherwise we'd
    have just printed the error right away as this is not a type of error the
    program itself can recover from; user input is required to fix it.
    '''

@contextmanager
def open_text(path):
    '''
    Robustly open text file

    Autodetect encoding. Python's universal newlines takes care of
    strange/mixed line endings.

    Parameters
    ----------
    path : ~pathlib.Path

    Returns
    -------
    file
        File object of the opened text file
    '''
    with path.open('rb') as f:
        encoding = chardet.detect(f.read())['encoding']
    with path.open(encoding=encoding) as f:
        yield f

def join_lines(text):
    return ' '.join(map(str.strip, text.splitlines())).strip()
