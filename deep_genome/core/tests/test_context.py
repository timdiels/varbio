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
Test deep_genome.core.context
'''

from deep_genome.core import AlgorithmContext
from deep_genome.core.database import Database
from deep_genome.core.configuration import Configuration
from deep_genome.core.configuration import UnknownGeneHandling
from click.testing import CliRunner
from pathlib import Path
    
class TestAlgorithmContext(object):
    
    def test_most(self, cli_test_args):
        Context = AlgorithmContext('1.0.0')
        @Context.command()
        def main(context):
            assert isinstance(context, Context)
            assert isinstance(context.database, Database)
            assert isinstance(context.configuration, Configuration)
            assert isinstance(context.configuration.unknown_gene_handling, UnknownGeneHandling)  # sample a config attribute to see it's really loaded
            assert isinstance(context.data_directory, Path)
            assert isinstance(context.cache_directory, Path)
    
        # test it runs fine
        result = CliRunner().invoke(main, cli_test_args)
        assert not result.exception, result.output
         
        # test the help message contains most things
        result = CliRunner().invoke(main, ['--help'])
        assert not result.exception, result.output
        assert 'core.conf' in result.output  # dg core.ConfigurationsMixin
        assert '--database' in result.output  # dg core.DatabaseMixin
        
    def test_multiple_configurations(self):
        Context = AlgorithmContext('1.0.0', {'other': 'the other conf'})
        @Context.command()
        def main(context):
            pass
        
        # test the help message contains most things
        result = CliRunner().invoke(main, ['--help'])
        assert not result.exception, result.output
        assert 'core.conf' in result.output  # dg core.ConfigurationsMixin
        assert 'the other conf' in result.output
        
        