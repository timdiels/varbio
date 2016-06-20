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
Test deep_genome.core.parsers
'''

from deep_genome.core.parsers import Parser
import pytest
from textwrap import dedent
import pandas as pd
import io

@pytest.fixture
def parser(context):
    return Parser(context)

class TestParseExpressionMatrix(object):
    
    def assert_equals(self, df1, df2):
        assert df1.equals(df2)
        assert df1.index.name == df2.index.name
        
    def test_happy_days(self, parser):
        matrix = parser.parse_expression_matrix(io.StringIO(dedent('''\
            ignored\tcondition1\tcondition2
            gene1\t1.5\t5
            gene2\t.89\t-.1'''
        )))
        expected = pd.DataFrame({'condition1': [1.5, .89], 'condition2': [5, -.1]}, index=pd.Index(['gene1', 'gene2'], name='gene'))
        self.assert_equals(matrix, expected)
    
class TestParseClustering(object):
    
    def test_mixed_input(self, parser):
        '''
        When clusters spread across rows, and multiple items on a line, parse
        just fine
        ''' 
        clustering = parser.parse_clustering(io.StringIO(dedent('''\
            cluster1\titem1\titem2
            cluster2\titem5\titem2
            cluster1\titem3'''
        )))
        expected = {
            'cluster1' : {'item1', 'item2', 'item3'},
            'cluster2' : {'item2', 'item5'}
        }
        assert clustering == expected
        
    def test_name_index_1(self, parser):
        '''
        When name_index=1, treat the second column as the cluster_id
        '''
        reader = io.StringIO(dedent('''\
            item1\tcluster1\titem2
            item5\tcluster2\titem2
            item3\tcluster1'''
        ))
        clustering = parser.parse_clustering(reader, name_index=1)
        expected = {
            'cluster1' : {'item1', 'item2', 'item3'},
            'cluster2' : {'item2', 'item5'}
        }
        assert clustering == expected
        
    def test_name_index_none(self, parser):
        '''
        When name_index=None, parse as 1 cluster per row
        '''
        reader = io.StringIO(dedent('''\
            item1\titem2
            item5\titem2
            item3'''
        ))
        clustering = parser.parse_clustering(reader, name_index=None)
        expected = {
            0: {'item1', 'item2'},
            1: {'item2', 'item5'},
            2: {'item3'}
        }  # Note: the returned cluster ids don't actually matter
        assert clustering == expected
    
    def test_name_index_negative(self, parser):
        '''
        When name_index<0, raise ValueError
        '''
        with pytest.raises(ValueError) as ex:
            parser.parse_clustering(io.StringIO(''), name_index=-1)
        assert 'name_index' in str(ex.value)
        