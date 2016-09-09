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
    Gene, GeneNameQueryItem, GeneNameQuery, ExpressionMatrix,
    Clustering, GeneMappingTable
)
from deep_genome.core.database.importers import FileImporter
from more_itertools import first
from chicken_turtle_util import path as path_, data_frame as df_, series as series_
from pathlib import Path
import pytest
import sqlalchemy as sa
from textwrap import dedent
import pandas as pd
import numpy as np

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
        
class TestExpressionMatrix(object):
    
    '''
    Test Session.add_expression_matrix and Session.get_expression_matrix_data
    '''
    
    _expression_matrix_df = pd.DataFrame({'condition1': [1.1, 3.3], 'condition2': [2.2, 4.4]}, index=['gene1', 'gene2'])
    _expression_matrix_df_duplicate_row = pd.DataFrame({'condition1': [1.1, 3.3, 3.3], 'condition2': [1.1, 4.4, 4.4]}, index=['gene1', 'gene2', 'gene2'])
    _expression_matrix_df_conflict = pd.DataFrame({'condition1': [1.1, 3.3], 'condition2': [1.1, 4.4]}, index=['gene1', 'gene1'])
    
    @pytest.fixture
    def expression_matrix_df(self):
        '''
        Simple valid matrix
        '''
        return self._expression_matrix_df.copy()
    
    @pytest.fixture
    def expression_matrix_df_duplicate_row(self):
        '''
        Valid matrix with a duplicate row
        '''
        return self._expression_matrix_df_duplicate_row.copy()
    
    @pytest.fixture
    def expression_matrix_df_conflict(self):
        '''
        Expression matrix with conflicting rows
        '''
        return self._expression_matrix_df_conflict.copy()
    
    def test_expression_matrix_invalid_type(self, session, expression_matrix_df):
        '''
        When matrix one of columns not int type, raise ValueError
        '''
        expression_matrix = expression_matrix_df
        expression_matrix['condition1'] = expression_matrix['condition1'].astype(int)
        with pytest.raises(ValueError) as ex:
            session.add_expression_matrix(expression_matrix, name='name')
        print(str(ex.value))
        assert (dedent('''\
            Expression matrix values must be of type {}, column types of given matrix:
            condition1      int64
            condition2    float64'''
            ).format(np.dtype(float))
            in str(ex.value)
        )
            
    def test_name_nul_characters(self, session, expression_matrix_df):
        '''
        When name contains nul characters, raise ValueError
        '''
        name = 'na\0me'
        with pytest.raises(ValueError) as ex:
            session.add_expression_matrix(expression_matrix_df, name=name)
        assert 'Expression matrix name contains nul characters: {!r}'.format(name.strip()) in str(ex.value)
        
    def test_name_empty(self, session, expression_matrix_df):
        '''
        When name is whitespace, raise ValueError
        '''
        for name in ('', ' ', '\t'):
            with pytest.raises(ValueError) as ex:
                session.add_expression_matrix(expression_matrix_df, name=name)
            assert "Expression matrix name is '' after stripping whitespace" in str(ex.value)
        
    @pytest.mark.parametrize('name', ('name', 'NAME', 'naME', 'na ME', ' nA Me ', '\nname\t', 'name:', 'na:me', 'na+me', 'na/me', 'na\tme', 'na\nme'))
    def test_name_valid(self, session, expression_matrix_df, name):
        '''
        When name valid, all good
        '''
        actual = session.add_expression_matrix(expression_matrix_df, name=name)
        assert actual.name == name.strip()
        
    def test_name_strip(self, session, expression_matrix_df):
        '''
        Strip surrounding whitespace of name
        '''
        actual = session.add_expression_matrix(expression_matrix_df, name=' na me ')
        assert actual.name == 'na me'
        
    def test_name_duplicate(self, session, expression_matrix_df):
        '''
        When add expression matrix with already existing name, raise ValueError
        '''
        session.add_expression_matrix(expression_matrix_df, name='name')
        session.add_expression_matrix(expression_matrix_df, name='name1') # duplicate data okay
        
        # duplicate name is not
        for name in ('name', ' name '):
            with pytest.raises(ValueError) as ex:
                session.add_expression_matrix(expression_matrix_df, name=name)
            assert "Expression matrix name already exists: {!r}".format(name.strip()) in str(ex.value)
    
    params = (
        (_expression_matrix_df, _expression_matrix_df),
        (_expression_matrix_df_duplicate_row, _expression_matrix_df_duplicate_row.iloc[0:2])
    )
    @pytest.mark.parametrize('original, expected', params)
    def test_happy_days(self, session, original, expected):
        '''
        When valid input, add all rows
        '''
        passed_in = original.copy()
        expression_matrix = session.add_expression_matrix(passed_in, 'expmat1')
        df_.assert_equals(original, passed_in) # input unchanged
        
        actual = session.get_expression_matrix_data(expression_matrix)
        actual.index = actual.index.to_series().apply(lambda x: x.name)
        df_.assert_equals(actual, expected)
        
    def test_conflict(self, session, expression_matrix_df_conflict):
        '''
        When a gene has multiple rows with different expression values, raise ValueError
        '''
        with pytest.raises(ValueError):
            session.add_expression_matrix(expression_matrix_df_conflict, 'expmat1')
            
    def test_empty(self, session, expression_matrix_df):
        '''
        When adding an empty matrix, raise ValueError
        '''
        with pytest.raises(ValueError) as ex:
            session.add_expression_matrix(expression_matrix_df.loc[[]], 'expmat1')
        assert 'Expression matrix must not be empty' in str(ex.value) 

class TestClustering(object):
    
    '''
    Test Session.add_clustering and Session.get_clustering_data
    '''
    
    def assert_equals(self, actual, expected):
        '''Compare clusterings'''
        actual['gene'] = actual['gene'].apply(lambda x: x.name)
        actual.sort_values(by=actual.columns.tolist(), inplace=True)
        actual.reset_index(drop=True, inplace=True)
        actual = actual.reindex(columns=('cluster_id', 'gene'))
        expected = expected.reindex(columns=('cluster_id', 'gene'))
        assert actual.equals(expected)
        
    @pytest.fixture
    def original(self):
        return pd.DataFrame({'cluster_id': ['cluster1', 'cluster1', 'cluster2', 'cluster2'], 'gene': ['gene1', 'gene2', 'gene3', 'gene4']})
    
    def test_happy_days(self, session, original):
        '''
        When valid input, add whole clustering
        '''
        expected = original
        
        passed_in = original.copy()
        clustering = session.add_clustering(passed_in)
        df_.assert_equals(original, passed_in) # input unchanged
        
        actual = session.get_clustering_data(clustering)
        self.assert_equals(actual, expected)

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
    
    def test_import_expression_matrix(self, db, importer, temp_dir_cwd):
        '''
        Test FileImporter.import_expression_matrix and Database.get_expression_matrix_data
        '''
        path = Path('file')
        path_.write(path, dedent('''\
            ignore\tcondition1\t\tcondition2
            \0gene1\t1.1\t2.2
            gene2\t3.3\t4.4
            ''') + '\r\n\r\r'
        )
        name = 'the name'
        
        id_ = importer.import_expression_matrix(path, name)
        assert id_ >= 0
        
        with db.scoped_session() as session:  # Note: starting the session after import guarantees seeing the import's effects
            expression_matrix = session.sa_session.query(ExpressionMatrix).get(id_)
            assert expression_matrix.name == name
            
            actual = session.get_expression_matrix_data(expression_matrix)
            actual.index.name = 'gene'
            actual.reset_index(inplace=True)
            actual['gene'] = actual['gene'].apply(lambda x: x.name)
            expected = pd.DataFrame({'gene': ['gene1', 'gene2'], 'condition1': [1.1, 3.3], 'condition2': [2.2, 4.4]})
            expected = expected.reindex(columns=('gene', 'condition1', 'condition2'))
            df_.assert_equals(actual, expected)
        
    def test_import_clustering(self, db, importer, temp_dir_cwd):
        '''
        Test FileImporter.import_clustering and Database.get_clustering_data
        '''
        path = Path('file')
        path_.write(path, dedent('''\
            item1\t\tCluster1
            \0item2\tCLUSTER1
            item3\tcluster2\titem4
            ''') + '\r\n\r\r'
        )
        
        id_ = importer.import_clustering(path, name_index=1)
        assert id_ >= 0
        
        with db.scoped_session() as session:
            clustering = session.sa_session.query(Clustering).get(id_)
            
            actual = session.get_clustering_data(clustering)
            cluster_ids = actual['cluster_id'].tolist()
            assert ('Cluster1' in cluster_ids) ^ ('CLUSTER1' in cluster_ids)
            
            actual['cluster_id'] = actual['cluster_id'].str.lower()
            actual['gene'] = actual['gene'].apply(lambda x: x.name)
            actual.sort_values(by=actual.columns.tolist(), inplace=True)
            actual.reset_index(drop=True, inplace=True)
            actual = actual.reindex(columns=('cluster_id', 'gene'))
            expected = pd.DataFrame({'cluster_id': ['cluster1', 'cluster1', 'cluster2', 'cluster2'], 'gene': ['item1', 'item2', 'item3', 'item4']})
            assert actual.equals(expected)
        
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
        
class TestGetByGenes(object):
    
    def test_get_nothing(self, session):
        gene_groups = pd.DataFrame(columns=['group_id', 'gene'])
        result = session.get_by_genes(gene_groups, min_genes=2, clusterings=False, expression_matrices=False)
        assert result.expression_matrices is None
        assert result.clusterings is None

    def test_happy_days(self, session):
        '''
        Test get_by_genes with gene mappings
        '''
        # import a gene mapping
        session.add_gene_mapping(pd.DataFrame({'source': ['a1', 'a1'], 'destination': ['b1', 'b2']}))
        
        # import some expression matrices
        exp_mat1 = session.add_expression_matrix(pd.DataFrame({'condition1': [1337, 1, 1]}, index=['a1', 'c1', 'c2'], dtype=float), 'expmat1')
        exp_mat2 = session.add_expression_matrix(pd.DataFrame({'condition1': [1337, 1]}, dtype=float, index=['b1', 'b2']), 'expmat2')
        session.add_expression_matrix(pd.DataFrame({'condition1': [1337, 1, 1]}, index=['b1', 'c2', 'c3'], dtype=float), 'expmat3')
        
        # import some clusterings
        clustering1 = session.add_clustering(pd.DataFrame({'cluster_id': ['1337', '1', '1'], 'gene': ['a1', 'c1', 'c2']}))
        clustering2 = session.add_clustering(pd.DataFrame({'cluster_id': ['1337', '1'], 'gene': ['b1', 'b2']}))
        session.add_clustering(pd.DataFrame({'cluster_id': ['1337', '1', '1'], 'gene': ['b1', 'c2', 'c3']}))
    
        # get
        gene_groups = pd.DataFrame({'group_id': [1]*3 + [2]*2, 'gene': ['b1', 'b2', 'c1', 'b2', 'c1']})
        gene_groups['gene'] = session.get_genes_by_name(gene_groups['gene']).apply(list)
        gene_groups = df_.split_array_like(gene_groups, 'gene')
        print(gene_groups)
        result = session.get_by_genes(gene_groups, min_genes=2, clusterings=True, expression_matrices=True)
        
        # assert
        expected = pd.DataFrame({
            'group_id': [1]*5 + [2]*2, 
            'gene': ['b1', 'b2', 'c1', 'b1', 'b2', 'b2', 'c1'], 
            'expression_matrix': [exp_mat1]*3 + [exp_mat2]*2 + [exp_mat1]*2
        })
        
        actual = result.expression_matrices
        actual['gene'] = actual['gene'].apply(lambda x: x.name)
        df_.assert_equals(actual, expected, ignore_order={0,1}, ignore_indices={0})
        
        # assert clusterings
        expected.drop('expression_matrix', axis=1, inplace=True)
        expected['clustering'] = [clustering1]*3 + [clustering2]*2 + [clustering1]*2
        actual = result.clusterings
        actual['gene'] = actual['gene'].apply(lambda x: x.name)
        df_.assert_equals(actual, expected, ignore_order={0,1}, ignore_indices={0})

'''
TODO

Add a case to everything testing empty inputs. Hint: we already implemented the response in most cases

when rows are dropped, things are ignored, log warnings naming the ignored genes
'''
#TODO later: add scope to other data and test scoping on them as well