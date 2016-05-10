# Copyright (C) 2016 VIB/BEG/UGent - Tim Diels <timdiels.m@gmail.com>
# 
# This file is part of Chicken Turtle Util.
# 
# Chicken Turtle Util is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# Chicken Turtle Util is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public License
# along with Chicken Turtle Util.  If not, see <http://www.gnu.org/licenses/>.

'''
Test deep_blue_genome.core.cli
'''

from deep_blue_genome.core.cli import AlgorithmMixin
from deep_blue_genome.core.database import Database
from deep_blue_genome.core.configuration import Configuration
from deep_blue_genome.core.configuration import UnknownGeneHandling
from click.testing import CliRunner
    
def test_algorithm_mixin(cli_test_args):
    Context = AlgorithmMixin('1.0.0')
    @Context.command()
    def main(context):
        assert isinstance(context, Context)
        assert isinstance(context.database, Database)
        assert isinstance(context.configuration, Configuration)
        assert isinstance(context.configuration.unknown_gene_handling, UnknownGeneHandling)  # sample a config attribute to see it's really loaded

    # test it runs fine
    result = CliRunner().invoke(main, cli_test_args)
    assert not result.exception, result.output
     
    # test the help message contains most things
    result = CliRunner().invoke(main, ['--help'])
    assert not result.exception, result.output
    assert 'core.conf' in result.output  # dbg core.ConfigurationMixin
    assert '--database' in result.output  # dbg core.DatabaseMixin