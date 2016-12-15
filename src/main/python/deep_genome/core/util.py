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

# TODO design, not sure whether part of API. Probably should be internal part of data prep
def is_data_file(path):
    '''
    Is a regular data file or directory, e.g. a clustering.
    
    Parameters
    ----------
    path : plumbum.Path
    
    Returns
    -------
    bool
    '''
    return not path.name.startswith('.')
    # XXX add filecmp.DEFAULT_IGNORES to things to ignore
    