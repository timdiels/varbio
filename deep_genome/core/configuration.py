# Copyright (C) 2015, 2016 VIB/BEG/UGent - Tim Diels <timdiels.m@gmail.com>
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

from enum import Enum

class Configuration(object):
    
    '''
    Deep Genome Core configuration
    
    Parameters
    ----------
    configuration : {section :: str => { option :: str => value :: str}}
        Raw configuration
    '''
    
    def __init__(self, config):
        self.unknown_gene_handling = UnknownGeneHandling[config['exception_handling']['unknown_gene']]

# see example config file for an explanation of these
UnknownGeneHandling = Enum('UnknownGeneHandling', 'add ignore fail')