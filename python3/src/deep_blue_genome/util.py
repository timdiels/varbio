# Copyright (C) 2015 VIB/BEG/UGent - Tim Diels <timdiels.m@gmail.com>
# 
# This file is part of Deep Blue Genome.
# 
# Deep Blue Genome is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# Deep Blue Genome is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public License
# along with Deep Blue Genome.  If not, see <http://www.gnu.org/licenses/>.

import pandas
from itertools import chain

def is_sorted(l):
    return all(l[i] <= l[i+1] for i in range(len(l)-1))

def fill_na_with_none(df):
    '''
    Fill all NaN in DataFrame with None.
    
    These None values will not be treated as 'missing' by DataFrame, as the dtypes will be set to 'object'
    '''
    df.where(pandas.notnull(df), None, inplace=True)
    
def flatten(lists):
    '''
    Flatten shallow list
    
    Parameters
    ----------
    list-like of list-like
        Shallow list
    '''
    return list(chain(*lists))
    
# TODO throw in a debug.py
import os
import psutil
def print_mem():
    process = psutil.Process(os.getpid())
    print('{}MB memory usage'.format(int(process.memory_info().rss / 2**20)))