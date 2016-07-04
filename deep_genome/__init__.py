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

from pkgutil import extend_path
__path__ = extend_path(__path__, __name__)

def _init():
    import plumbum as pb
    import matplotlib
    from chicken_turtle_util import pymysql as pymysql_
    
    # from Bio import Entrez
    # Entrez.email = 'no-reply@psb.ugent.be'  # TODO perhaps this email address should be user supplied
    
    # init matplotlib
    if not 'DISPLAY' in pb.local.env:
        matplotlib.use('Agg')  # use this backend when no X server
    
    # find __root__
    global __root__
    __root__ = pb.local.path(__file__).dirname
    
    # various
    pymysql_.patch()
    
    # setup logging for testing
    # also log everything to stdout
    # XXX logging.basicConfig is easier to set things up
    import sys
    import logging
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logging.getLogger().root.addHandler(ch)
    logging.getLogger('deep_genome').setLevel(logging.INFO)
    logging.getLogger('deep_genome').setLevel(logging.DEBUG)
    logging.getLogger('deep_genome.core.Database').setLevel(logging.INFO)
    
__root__ = None  # make linter happy  #TODO unused? You know, __root__ might not even point to an actual file, could be inside an egg
_init()