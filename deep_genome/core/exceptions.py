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

'''
Exceptions used by `core`
'''

class TaskFailedException(Exception): #TODO rename to *Error, not sure whether still using this, maybe rm
    
    '''
    Raised when the current task should fail due to some (hopefully) exceptional
    event.
    
    To give an idea of the scope of these 'tasks', we give a few examples: a
    MORPH run; importing a gene expression matrix.
    '''
        
class DatabaseIntegrityError(Exception):
    
    '''
    When database integrity has been violated (and ValueError wasn't
    appropriate)
    
    This may occur when combining 2 integer scopes. E.g. 2 scopes containing the
    same gene family is fine as long as they don't end up in the same
    reading_scopes.
    '''
        
        