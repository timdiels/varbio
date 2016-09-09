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
Test deep_genome.core.database
'''

from deep_genome.core.database.entities import (
    Gene, GeneNameQueryItem, GeneNameQuery, GeneMappingTable
)
from deep_genome.core.database.importers import FileImporter
from more_itertools import first
from chicken_turtle_util import path as path_, data_frame as df_, series as series_
from pathlib import Path
import pytest
import sqlalchemy as sa
from textwrap import dedent
import pandas as pd

@pytest.yield_fixture
def session(db):
    with db.scoped_session() as session:
        yield session
        
        # No temp stuff left behind
        assert session.sa_session.query(GeneNameQuery).count() == 0
        assert session.sa_session.query(GeneNameQueryItem).count() == 0
        
def test_clear_and_create(context):
    db = context.database
    db.clear()  # initial db state could be anything, so clear it
    
    db.create()
    assert db._engine.table_names()  # there are some tables
    
    db.clear()
    assert not db._engine.table_names()  # and now they're gone

class TestScopedSession(object):
    
    def test_happy_days(self, db):
        '''
        When no exception, commit and close
        '''
        description = 'A gene'
        with db.scoped_session() as session:
            gene = Gene(description=description)
            sa_session = session.sa_session
            sa_session.add(gene)
            assert sa_session.new
        assert not sa_session.new  # session closed
        
        with db.scoped_session() as session:
            session.sa_session.query(Gene).filter_by(description=description).one()  # previous session committed
           
    def test_exception(self, db): 
        '''
        When exception, roll back and close
        '''
        description = 'A gene'
        class TestException(Exception):
            pass
        with pytest.raises(TestException):
            with db.scoped_session() as session:
                gene = Gene(description=description)
                sa_session = session.sa_session
                sa_session.add(gene)
                raise TestException()
        assert not sa_session.new  # session closed
        
        with db.scoped_session() as session:
            assert session.sa_session.query(Gene).filter_by(description=description).first() is None  # previous session rolled back
            
class TestGetGenesByName(object):
    
    '''
    Test get_genes_by_name
    
    See TestGeneMapping for testing of _map with actual mappings present
    '''
    
    def assert_dfs(self, original, passed_in, actual):
        df_.assert_equals(passed_in, original)  # musn't change what's passed in
        assert (actual.applymap(lambda x: len(x)).values == 1).all().all()
        actual = actual.applymap(lambda x: first(x).name)
        df_.assert_equals(actual, original)
        
    def assert_series(self, original, passed_in, actual):
        series_.assert_equals(passed_in, original)  # musn't change what's passed in
        assert (actual.apply(lambda x: len(x)).values == 1).all()
        actual = actual.apply(lambda x: first(x).name)
        series_.assert_equals(actual, original)
    
    @pytest.fixture(params=(True, False))
    def map_(self, request):  # The value of _map shouldn't matter when there are no mappings in database
        return request.param
    
    def test_add_df(self, session, map_):
        '''
        When get on empty db, add the missing genes and return correctly
        '''
        original = pd.DataFrame({'colA' : ['gene1', 'gene2'], 'colB': ['gene1', 'gene3']}, index=pd.Index(['first', 'second'], name='myIndex'))
        df = original.copy()
        actual = session.get_genes_by_name(df, _map=map_)
        assert first(actual.iloc[0,0]) is first(actual.iloc[0,1])
        self.assert_dfs(original, df, actual)
        
        # When doing it on existing genes, still return correctly
        actual2 = session.get_genes_by_name(df, _map=map_)
        assert actual2.equals(actual)
        self.assert_dfs(original, df, actual2)
    
    def test_add_series(self, session, map_):
        '''
        When get on empty db, add the missing genes and return correctly
        '''
        original = pd.Series(['gene1', 'gene2', 'gene1'], index=pd.Index(['first', 'second', 'second'], name='myIndex'), name='colA')
        series = original.copy()
        actual = session.get_genes_by_name(series, _map=map_)
        assert first(actual.iloc[0]) is first(actual.iloc[2])
        self.assert_series(original, series, actual)
        
        # When doing it on existing genes, still return correctly
        actual2 = session.get_genes_by_name(series, _map=map_)
        series_.assert_equals(actual, actual2)
        self.assert_series(original, series, actual2)
        
    def test_get_series_names(self, session, map_):
        '''
        When get series, return same name and same index.name
        
        Only testing with nameless series now, but named series is covered by test_add_series 
        '''
        original = pd.Series(['gene1', 'gene2', 'gene1'], index=['first', 'second', 'second'])
        series = original.copy()
        actual = session.get_genes_by_name(series, _map=map_)
        self.assert_series(original, series, actual)

class TestGeneMapping(object):
    
    '''
    Test Session.add_gene_mapping and Session.get_genes_by_name with mappings present
    '''
    
    @pytest.fixture
    def original(self):
        # Note: this also asserts that multiple source genes may map to the same gene
        return pd.DataFrame({'source': ['geneA1', 'geneA1', 'geneA2', 'geneA3', 'geneA3'], 'destination': ['geneB1', 'geneB2', 'geneB3', 'geneB4', 'geneB2']})
    
    def test_happy_days(self, original, session):
        '''
        When valid input, add all mappings
        '''
        passed_in = original.copy()
        session.add_gene_mapping(passed_in)
        df_.assert_equals(original, passed_in)
        
        actual = session.get_genes_by_name(pd.Series(['geneA1', 'geneB1', 'geneA2', 'geneA3', 'geneC1']))
        actual = actual.apply(lambda x: {y.name for y in x}).tolist()
        assert actual == [{'geneB1', 'geneB2'}, {'geneB1'}, {'geneB3'}, {'geneB4', 'geneB2'}, {'geneC1'}]
    
    def test_source_destination_conflict(self, session):
        '''
        When add_gene_mapping causes a gene to appear as both source and destination, raise ValueError
        
        Conflict added in one go
        '''
        with pytest.raises(ValueError) as ex:
            session.add_gene_mapping(pd.DataFrame({'source': ['geneA', 'geneB'], 'destination': ['geneB', 'geneA']})) # add conflicting mapping
        assert 'both the source and the destination' in str(ex.value)
        assert 'geneA' in str(ex.value)
        assert 'geneB' in str(ex.value)
        
    def test_source_destination_conflict2(self, session):
        '''
        When add_gene_mapping causes a gene to appear as both source and destination, raise ValueError
        
        Conflict spread across 2 additions
        '''
        session.add_gene_mapping(pd.DataFrame({'source': ['geneA'], 'destination': ['geneB']}))
        with pytest.raises(ValueError) as ex:
            session.add_gene_mapping(pd.DataFrame({'source': ['geneB'], 'destination': ['geneA']})) # add conflicting mapping
        assert 'both the source and the destination' in str(ex.value)
        assert 'geneA' in str(ex.value)
        assert 'geneB' in str(ex.value)
        
    def test_add_existing(self, session):
        '''
        When adding a mapping that already exists, just silently ignore
        '''
        session.add_gene_mapping(pd.DataFrame({'source': ['geneA'], 'destination': ['geneB']}))
        session.add_gene_mapping(pd.DataFrame({'source': ['geneA'], 'destination': ['geneB']}))
        actual = session.get_genes_by_name(pd.Series(['geneA']))[0]
        assert len(actual) == 1
        assert first(actual).name == 'geneB'
        assert session.sa_session.execute(sa.sql.select([sa.func.count()]).select_from(GeneMappingTable)).scalar() == 1
        
class TestFileImporter(object):
    
    @pytest.fixture
    def importer(self, context):
        return FileImporter(context)
    
    def test_import_gene_mapping(self, db, importer, temp_dir_cwd):
        '''
        Test FileImporter.import_gene_mapping, Database.get_genes_by_name(_map=True)
        '''
        path = Path('file')
        path_.write(path, dedent('''\
            geneA1\t\tgeneB1\tgeneB2
            \0geneA2\tgeneB3
            geneA3\tgeneB4\tgeneB2
            ''') + '\r\n\r\r'
        )
        
        importer.import_gene_mapping(path)
        
        with db.scoped_session() as session:
            actual = session.get_genes_by_name(pd.Series(['geneA1', 'geneB1', 'geneA2', 'geneA3', 'geneC1']))
            actual = actual.apply(lambda x: {y.name for y in x}).tolist()
            assert actual == [{'geneB1', 'geneB2'}, {'geneB1'}, {'geneB3'}, {'geneB4', 'geneB2'}, {'geneC1'}]
        
'''
TODO

Add a case to everything testing empty inputs. Hint: we already implemented the response in most cases

when rows are dropped, things are ignored, log warnings naming the ignored genes
'''
#TODO later: add scope to other data and test scoping on them as well