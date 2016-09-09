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

'''
Database access

Whenever null is mentioned, this refers to any value `x` for which
``pandas.isnull(x)``
'''

from deep_genome.core.database.entities import (
    DBEntity, Gene, GeneName,
    GeneNameQueryItem, ExpressionMatrix, GetByGenesQuery, GetByGenesQueryItem,
    Clustering, GeneNameQuery, DataFile, GeneMappingTable,
    AddGeneMappingQuery, AddGeneMappingQueryItem
)
from chicken_turtle_util import data_frame as df_
from chicken_turtle_util.sqlalchemy import pretty_sql
import sqlalchemy as sa
import sqlalchemy.sql as sql
from sqlalchemy.orm import sessionmaker, aliased
from contextlib import contextmanager
from collections import namedtuple
import pandas as pd
import logging
import os
import numpy as np

_logger = logging.getLogger('deep_genome.core.Database')

_ReturnTuple = namedtuple('_ReturnTuple', 'expression_matrices clusterings'.split())

def _data_file_dir(context):
    return context.data_directory / 'data_files'

def _print_sql(stmt):
    print(pretty_sql(stmt))
    
# TODO not allowing new gene mappings later on may prove too limiting one day
# (where in a different scope a gene mapping is added, you can no longer have
# one pre-compiled set of mappings)

# Verdict:
# 1. just add it to our database, add to our scheme and such. Extend that Gene entity, though maybe go more weak reffed: e.g. IdType(id, name), GeneId(Gene, IdType, value:str), Gene(id, description, ...)
# - useful things in BioPython and BioSQL. We should know it and stay up to date with it. Set a schedule to learn it.
# - no apparent easy way to load gene2refseq, ... in BioSQL and BioPython in bulk. But before making your own, ask on relevant IRC or mailing list.
#   If none, understand the data provided on the FTP, that will be the structure of our entities.

# 0. Is there a Bio SQL example database to browse?
# 1. Read the Bio SQL schema (downloads), maybe combine with BioPython doc if you think it's related. Get an idea of what goes in it.
#    Bio.BioSQL only supports Seq, SeqRecord data
#    Maybe other Bio.* do still use Bio SQL?
#    Bio SQL schema however supports: taxonomy, ontology, bioentries in biodatabases along with features (e.g. a seqref id?)
# 2. What do? How integrate, if at all?
#TODO consider using and extending the BioSQL database scheme instead of making
#our own. Making a sqlalchemy interface on top of it may be useful though, or
#generally providing a less raw interface (if it is raw). Consider adding
#directly to BioPython though.
# "Bringing bulk operations and concurrency to (some of) BioPython for higher throughput.

#TODO BioPython has Bio.Affy, Bio.KEGG, Bio.Pathway, Bio.UniProt, ...

# TODO Nice to have: I could write something to automatically generate namespaced copies of tables (taking into account foreign keys as well)
#   gene_name_table = GeneName.__table__.tometadata(meta_data, name='tmp_gene_name')
#   gene_name_table.c.name.unique = False
# TODO if using ORM on those classes, you can use the ORM syntax, even though you wouldn't want to do any bulk on them via ORM, ever

#TODO take a connection string, not host, user, .... Instead have your create_database in the context take host, ... and make a conn string out of it as is done here

class Database(object):
    
    '''
    MySQL database access
    
    All string comparisons in the database are case-insensitive, strings are
    stored with case though.
    
    All gene mappings must be added before any entities containing genes are
    added.
    
    Parameters
    ----------
    context : deep_genome.core.Context
        Deep Genome context
    host : str
        DNS or IP of the DB host
    user : str
        Username to log in with
    password : str
        Password to log in with
    name : str
        Database name
    
    Notes
    -----
    The database serves 2 main purposes: persistence and not having to store
    everything in memory. As such, any non-trivial data operation requires
    the database.
    
    Genes which appear as a source gene of a gene mapping, will appear nowhere
    else, other than in GeneName.
    '''
    
    def __init__(self, context, host, user, password, name): 
        self._context = context
        self._engine = sa.create_engine('mysql+pymysql://{}:{}@{}/{}'.format(user, password, host, name), echo=False)
        self._Session = sessionmaker(bind=self._engine)
        os.makedirs(str(_data_file_dir(context)), exist_ok=True)
        
    def dispose(self):
        '''
        Release all resources.
        
        Do not use this instance after calling this.
        '''
        self._Session.close_all()
        self._engine.dispose()
        
    @contextmanager
    def scoped_session(self):
        '''
        Create a session context manager
        
        Rolls back if exception was raised, commits otherwise. In either case,
        the session is closed.
        
        Returns
        -------
        deep_genome.core.database.Session
        '''
        session = Session(self._context, self._create_session())
        try:
            yield session
            session.sa_session.commit()
        except:
            session.sa_session.rollback()
            raise
        finally:
            session.sa_session.close()
                 
    def clear(self):
        '''
        Remove all tables, constraints, ...
        '''
        metadata = sa.MetaData(bind=self._engine)
        for name in self._engine.table_names():
            sa.Table(name, metadata, autoload=True, autoload_with=self._engine)
        metadata.drop_all()
        
    def create(self):
        '''
        Create all missing tables, constraints, ...
        '''
        DBEntity.metadata.create_all(self._engine)
        
    def _create_session(self):
        return self._Session(bind=self._engine)
    
class Session(object):
    
    '''
    High level DB access session (provided using an underlying sqlalchemy Session).
    
    Don't create directly, see `Database.scoped_session` instead.
    
    None of `Session`\ 's methods commit or rollback the underlying SA session.
    It's up to the session creator to commit when done. However, anyone may
    flush the sqlalchemy session.

    `Session` objects should not be shared across threads, they're not thread safe.
    
    Any getter will return entities with valid ids, however add* methods provide
    no such guarantee. If you do need the id, call `sa_session.flush()`.
    
    Notes
    -----
    Consistency: inputs and outputs usually are in a tabular format, i.e. a
    pd.DataFrame or pd.Series.
    '''
    
    # Design note: when adding, allow the use of gene symbols (i.e. str); when
    # getting, require Gene instances
    
    def __init__(self, context, session):
        self._context = context
        self._session = session
        
    @property
    def sa_session(self):
        '''Get underlying sqlalchemy (SA) Session'''
        return self._session
    
    @contextmanager
    def query(self, Query): #TODO document #TODO test
        '''
        Examples
        --------
        class SomeQuery(DBEntity):
            id = Column(Integer, primary_key=True)
            
        class SomeQueryItem(DBEntity):
            query_id =  Column(Integer, ForeignKey('some_query.id', ondelete='cascade'), primary_key=True)
            id =  Column(Integer, primary_key=True)
            # ... actual query data fields
        
        def some_query(session):
            with session.query(SomeQuery) as query:
                # insert items using query.id as value for query_id
                # do stuff with query items
            # query entry and its items are removed when with statement ends
        '''
        query = Query()
        raised = False
        try:
            self._session.add(query)
            self._session.flush()  # we will need query.id
            yield query
        except Exception as ex:
            raised = True
            raise ex
        finally:
            # Even when there was an exception, try to get rid of query
            try:
                self._session.delete(query)
            except Exception as ex:
                # Ignore failure to delete if there has already been an
                # exception, session is probably borked because of it
                if not raised:
                    raise ex
    
    def _add_unknown_genes(self, query_id):
        select_missing_names_stmt = (
            self._session.query(GeneNameQueryItem.name)
            .distinct()
            .filter_by(query_id=query_id)
            .join(GeneName, GeneName.name == GeneNameQueryItem.name, isouter=True)
            .filter(GeneName.id.is_(None))
        )
        
        # Insert genes
        stmt = (
            Gene.__table__
            .insert()
            .from_select(['id'], 
                self._session
                .query(sa.null())
                .select_entity_from(select_missing_names_stmt.subquery())
            )
        )
        unknown_genes_count = self._session.execute(stmt).rowcount
        
        # Continue only if there actually were unknown genes 
        if unknown_genes_count:
            # Insert gene names
            stmt = (
                GeneName.__table__
                .insert()
                .from_select(['gene_id', 'name'],
                    sql.select([sql.text('@row := @row + 1'), select_missing_names_stmt.subquery()])
                    .select_from(sql.text('(SELECT @row := last_insert_id() - 1) range_'))
                )
            )
            self._session.execute(stmt)
            
            # Set the inserted names as the canonical name of the inserted genes
            stmt = (
                sa.update(Gene)
                .where(sql.and_(Gene.canonical_name_id==None, GeneName.gene_id==Gene.id))
                .values(canonical_name_id=GeneName.id)
            )
            self._session.execute(stmt)
            
            _logger.info('Added {} missing genes to database'.format(unknown_genes_count))
        
    def _get_genes_by_name(self, query_id, names, map_):
        # Build query
        UnmappedGene = aliased(Gene)
        stmt = (  # Select existing genes by name
            self._session
            .query() 
            .select_from(GeneNameQueryItem)
            .filter_by(query_id=query_id)
            .join(GeneName, GeneNameQueryItem.name == GeneName.name, isouter=True)
            .join(UnmappedGene, GeneName.gene, isouter=True)
        )
        
        if map_:
            MappedGene = aliased(Gene)
            
            # Select the MappedGene if exists, otherwise select UnmappedGene
            stmt = (
                stmt
                .with_entities(GeneNameQueryItem.row, GeneNameQueryItem.column, Gene)
                .join(MappedGene, UnmappedGene.mapped_to, isouter=True)
                .join(Gene, Gene.id == sql.func.ifnull(MappedGene.id, UnmappedGene.id), isouter=True)
            )
        else:
            stmt = stmt.with_entities(GeneNameQueryItem.row, GeneNameQueryItem.column, UnmappedGene)
            
        stmt = stmt.order_by('row', 'column')  # ensure return will have same ordering as names.index

        # XXX no need to fetch column when names is pd.Series
        
        # Load result
        if isinstance(names, pd.Series):
            names_ = names.to_frame()
        else:
            names_ = names
            
        genes = pd.DataFrame(iter(stmt), columns=['row', 'column', 'value'])
        genes = genes.groupby(['row', 'column'])['value'].apply(lambda x: frozenset(x.dropna())).unstack('column')
        genes.rename(
            index=dict(enumerate(names_.index)),
            columns=dict(enumerate(names_.columns)),
            inplace=True
        )
        
        genes.index.name = names_.index.name
        if isinstance(names, pd.Series):
            genes = genes[genes.columns[0]]
            genes.name = names.name
        else:
            genes.columns.name = names_.columns.name
            
        return genes
     
    # Note: removed map_suffix1 which mapped gene_name.1 to gene_name. This is easy enough to do manually or at least can be split out. I.e. names.applymap(lambda x: re.sub(r'\.1$', '', x))  #XXX rm note when done
    def get_genes_by_name(self, names, _map=True): #TODO maybe add an example per return
        '''
        Get genes by name
        
        If a gene is not found, it will be added with the given name as
        canonical name.
        
        Parameters
        ----------
        names : pd.DataFrame([[gene_name :: str]]) or pd.Series([gene_name :: str]) 
            A DataFrame or Series of gene names to look up.
        _map : bool
            Internal, do not use. Map genes according to gene mappings if True.
            
        Returns
        -------
        pd.DataFrame([[frozenset({Gene})]], index=names.index, columns=names.columns)
        or
        pd.Series([frozenset({Gene})], index=names.index, name=names.name)
            `names` with each gene name replaced by a set of matching Gene. Each
            set is non-empty.
             
        Notes
        -----
        All Session methods expect and return mapped genes when
        expecting/returning a `Gene` instead of a `str`.
        '''
        if names.empty:
            return names
        
        _logger.debug('Querying {} genes by name'.format(names.size))
        
        with self.query(GeneNameQuery) as query:
            # Insert gene group for query
            if isinstance(names, pd.Series):
                items = names.to_frame()
            else:
                items = names.copy(deep=False)
            items.columns = range(len(items.columns))
            items['row'] = range(len(items))
            items = pd.melt(items, id_vars='row', var_name='column', value_name='name')
            items['query_id'] = query.id
            self._session.bulk_insert_mappings(GeneNameQueryItem, items.to_dict('record'))
            
            # Add any unknown genes (genes not in database)
            self._add_unknown_genes(query.id)
            
            # Find genes by name
            genes = self._get_genes_by_name(query.id, names, map_=_map)
            
        return genes
    
    def _get_gene_collections_by_genes(self, query_id, min_genes_present, GeneCollection, gene_to_collection_relation, gene_collection_name):
        # Match baits to matrix genes
        select_baits_unmapped_container_stmt = (
            self._session
            .query(GetByGenesQueryItem.gene_group_id, Gene, GeneCollection)
            .select_from(GetByGenesQueryItem)
            .filter_by(query_id=query_id)
            .join(Gene)
            .join(GeneCollection, gene_to_collection_relation)
        )
        
        # Match baits to mapped matrix genes
        MappedGene = aliased(Gene, name='mapped_gene')
        select_baits_mapped_container_stmt = (
            self._session
            .query(GetByGenesQueryItem.gene_group_id, Gene, GeneCollection)
            .select_from(GetByGenesQueryItem)
            .filter_by(query_id=query_id)
            .join(MappedGene)
            .join(Gene, MappedGene.mapped_from)
            .join(GeneCollection, gene_to_collection_relation)
        )
        
        # Union previous 2 selects
        select_baits_container_stmt = select_baits_unmapped_container_stmt.union_all(select_baits_mapped_container_stmt)
        
        # Filter (baits_id, exp mat) combos to those with enough baits in them
        entities = [GetByGenesQueryItem.gene_group_id.label('gene_group_id'), GeneCollection.id.label('collection_id')]
        bait_count_filter_stmt = (
            select_baits_container_stmt
            .with_entities(*entities)
            .group_by(*entities)
            .having(sql.func.count() >= min_genes_present)
        )
        
        # Select the whole deal, with above filterdy 
        filter_sub = bait_count_filter_stmt.subquery(name='filter_sub')
        stmt = (
            select_baits_container_stmt
            .with_entities(GetByGenesQueryItem.gene_group_id, Gene, GeneCollection)
            .join(filter_sub, sql.and_(
                GeneCollection.id == filter_sub.c.collection_id,
                GetByGenesQueryItem.gene_group_id == filter_sub.c.gene_group_id,
            ))
        )
        
        # Run query and return result
        return pd.DataFrame(iter(stmt), columns=['group_id', 'gene', gene_collection_name])
        
    def get_by_genes(self, gene_groups, min_genes, expression_matrices=False, clusterings=False):
        '''
        Get gene collections (expression matrices and/or clusterings) containing
        a subset of a gene group
        
        Multiple gene groups can be supplied to make multiple queries at once.
        
        Parameters
        ----------
        gene_groups : pd.DataFrame({'group_id': [str], 'gene': [Gene]})
            List of gene groups to compare against
        min_genes : int
            Minimum number of genes of a gene group that must be present in a
            gene collection for it to be returned
        expression_matrices : bool
            Get expression matrices by genes
        clusterings : bool
            Get clusterings by genes
            
        Returns
        -------
        result : namedtuple(
            expression_matrices=pd.DataFrame({'group_id': [int], 'gene': [Gene], 'expression_matrix': [ExpressionMatrix]}) or None,
            clusterings=pd.DataFrame({'group_id': [int], 'gene': [Gene], 'clustering': [Clustering]}) or None
        )
            For each gene collection, if not requested, its corresponding field
            in `result` is None. Else, it contains a data frame containing the
            gene collections with the requested minimum of genes. The 'gene'
            column lists which genes of the corresponding group are present in
            the expression matrix. Note that groups with no matches, won't
            appear at all; there are no NA values in the returned data frames.
        '''
        if gene_groups.empty:
            if expression_matrices:
                expression_matrices = pd.DataFrame(columns=('group_id', 'gene', 'expression_matrix'))
            else:
                expression_matrices = None
            if clusterings:
                clusterings = pd.DataFrame(columns=('group_id', 'gene', 'clustering'))
            else:
                clusterings = None
            return _ReturnTuple(
                expression_matrices=expression_matrices, 
                clusterings=clusterings
            )
        with self.query(GetByGenesQuery) as query:
            gene_groups = gene_groups.copy()
            gene_groups['query_id'] = query.id
            gene_groups['gene'] = gene_groups['gene'].apply(lambda x: x.id)
            gene_groups.rename(columns={'group_id': 'gene_group_id', 'gene': 'gene_id'}, inplace=True)
            self._session.bulk_insert_mappings(GetByGenesQueryItem, gene_groups.to_dict('record'))
            
            if expression_matrices:
                expression_matrices = self._get_gene_collections_by_genes(query.id, min_genes, ExpressionMatrix, Gene.expression_matrices, 'expression_matrix')
            else:
                expression_matrices = None
            
            if clusterings:
                clusterings = self._get_gene_collections_by_genes(query.id, min_genes, Clustering, Gene.clusterings, 'clustering')
            else:
                clusterings = None
                
            return _ReturnTuple(expression_matrices=expression_matrices, clusterings=clusterings)
    
    def _create_data_file(self):
        file = DataFile()
        self._session.add(file)
        self._session.flush()
        return file
    
    def _data_file_path(self, data_file):
        return _data_file_dir(self._context) / str(data_file.id)
        
    def add_expression_matrix(self, expression_matrix, name):
        '''
        Add expression matrix
        
        Parameters
        ----------
        expression_matrix : pd.DataFrame({condition_name => [gene_expression :: float]}, index=[gene_symbol :: str])
            Expression matrix to add
        name : str
            Expression matrix name. Surrounding whitespace is stripped. Musn't be empty or contain nul characters.
            
        Returns
        -------
        .entities.ExpressionMatrix
            Added expression matrix
            
        Raises
        ------
        ValueError
            When either:
            
            - a gene appears multiple times with different expression values.
            - an expression matrix already exists with the given name
            - name contains invalid characters
            - expression matrix has a column with a dtype other than float
        '''
        # Validate name
        name = name.strip()
        if '\0' in name:
            raise ValueError('Expression matrix name contains nul characters: {!r}'.format(name))
        if not name:
            raise ValueError("Expression matrix name is '' after stripping whitespace")
        if self._session.query(ExpressionMatrix).filter_by(name=name).first():
            raise ValueError("Expression matrix name already exists: {!r}".format(name))
        
        # Validate expression_matrix
        if not (expression_matrix.dtypes == float).all(): #TODO test
            raise ValueError("Expression matrix values must be of type {}, column types of given matrix:\n{}".format(np.dtype(float), expression_matrix.dtypes.to_string()))
        if expression_matrix.empty:
            raise ValueError('Expression matrix must not be empty')
        
        # Get `Gene`s
        expression_matrix = expression_matrix.copy()
        expression_matrix['_Session__index'] = self.get_genes_by_name(expression_matrix.index.to_series()).apply(list)
        expression_matrix = df_.split_array_like(expression_matrix, '_Session__index')
        
        # Ignore duplicate rows. No warn, they're harmless
        expression_matrix.drop_duplicates(inplace=True)
        
        # Validate: no 2 different expression rows should be associated to the same gene
        duplicated = expression_matrix['_Session__index'].duplicated()
        if duplicated.any():
            duplicates = (x.canonical_name.name for x in expression_matrix['_Session__index'][duplicated])
            raise ValueError('Expression matrix has multiple gene expression rows for genes: ' + ', '.join(duplicates))
        
        #
        genes = expression_matrix['_Session__index'].tolist()
        expression_matrix.set_index('_Session__index', inplace=True)
        
        # Write data to file
        data_file = self._create_data_file()
        expression_matrix.index = expression_matrix.index.map(lambda x: x.id)
        expression_matrix.to_pickle(str(self._data_file_path(data_file)))
        
        # Insert in database
        expression_matrix = ExpressionMatrix(data_file=data_file, genes=genes, name=name)
        self._session.add(expression_matrix)
        
        return expression_matrix
        
    def get_expression_matrix_data(self, expression_matrix):
        '''
        Get expression matrix data
        
        Parameters
        ----------
        expression_matrix : .entities.ExpressionMatrix
            Expression matrix to get data of
            
        Returns
        -------
        pd.DataFrame({condition_name => [gene_expression :: float]}, index=[Gene])
        '''
        expression_matrix_ = pd.read_pickle(str(self._data_file_path(expression_matrix.data_file)))
        expression_matrix_.index = expression_matrix_.index.map(lambda id_: self._session.query(Gene).get(id_)) # Note: if this is slow, use expression_matrix.genes instead
        return expression_matrix_
    
    def add_clustering(self, clustering, expression_matrix=None):
        '''
        Add gene clustering
        
        Parameters
        ----------
        clustering : pd.DataFrame({'cluster_id' => [str], 'gene' => [str]})
            Clustering to add.
        expression_matrix : .entities.ExpressionMatrix or None
            If not None, hints algorithms only to use this clustering on the
            given expression matrix.
        
        Returns
        -------
        .entities.Clustering
            Added clustering
        '''
        #TODO should not allow empty clustering?
        # Get genes
        clustering = clustering.copy()
        clustering['gene'] = self.get_genes_by_name(clustering['gene']).apply(list)
        clustering = df_.split_array_like(clustering, 'gene')
        genes = clustering['gene']
        
        # Write data to file
        data_file = self._create_data_file()
        clustering['gene'] = clustering['gene'].apply(lambda x: x.id)
        clustering.to_pickle(str(self._data_file_path(data_file)))
        
        # Insert in database
        clustering = Clustering(data_file=data_file, genes=genes.drop_duplicates().tolist(), expression_matrix=expression_matrix)
        self._session.add(clustering)
        
        return clustering
        
    def get_clustering_data(self, clustering):
        '''
        Get clustering data
        
        Parameters
        ----------
        clustering : .entities.Clustering
            Clustering to get data of
            
        Returns
        -------
        pd.DataFrame({'cluster_id' => [str], 'gene' => [Gene]}) 
            Clustering.
        '''
        clustering_ = pd.read_pickle(str(self._data_file_path(clustering.data_file)))
        clustering_['gene'] = clustering_['gene'].apply(lambda id_: self._session.query(Gene).get(id_)) # Note: if this is slow, use clustering.genes instead
        return clustering_
    
    def add_gene_mapping(self, mapping):
        '''
        Add gene mapping
        
        We do not support transitivity on gene mappings. gene1 -> gene2 and
        gene2 -> gene3 cannot exist at the same time, instead add gene1 -> gene3
        and gene2 -> gene3.
        
        A gene mapping is a non-transitive, symmetric relation between gene names. 
        An example of this is the rice MSU-RAP mapping (http://www.thericejournal.com/content/6/1/4).
        
        Parameters
        ----------
        mapping : pd.DataFrame({'source' => [str], 'destination' => [str]})
            Mapping to add
        
        Raises
        ------
        ValueError
            When the addition causes a gene to appear both on the source side of
            one mapping and the destination side of another (or the same)
            mapping.
        '''
        if mapping.empty:
            return
        
        with self.query(AddGeneMappingQuery) as query:
            # Get genes from database
            mapping = self.get_genes_by_name(mapping, _map=False).applymap(list)
            mapping = df_.split_array_like(mapping)
            
            # Insert mappings in query table
            mapping = mapping.applymap(lambda x: x.id)
            mapping.rename(columns={'source': 'source_id', 'destination': 'destination_id'}, inplace=True)
            mapping['query_id'] = query.id
            self._session.bulk_insert_mappings(AddGeneMappingQueryItem, mapping.to_dict('record'))
            
            #
            select_query_items = (
                self._session
                .query(AddGeneMappingQueryItem)
                .filter_by(query_id=query.id)
            )
            
            # Remove any query mappings that are already in the mapping table
            stmt = sa.join(AddGeneMappingQueryItem, GeneMappingTable, sql.and_(
                AddGeneMappingQueryItem.source_id == GeneMappingTable.c.source_id,
                AddGeneMappingQueryItem.destination_id == GeneMappingTable.c.destination_id
                )
            )
            stmt = sa.delete(stmt, prefixes=[AddGeneMappingQueryItem.__tablename__]).where(AddGeneMappingQueryItem.query_id == query.id)
            self._session.execute(stmt)
            
            # Validate input: no genes appearing both on source and destination side
            AddGeneMappingQueryItem2 = aliased(AddGeneMappingQueryItem)
            select_input_source_input_destination_conflict_gene_ids = (
                select_query_items
                .with_entities(AddGeneMappingQueryItem.source_id.label('gene_id'))
                .join(AddGeneMappingQueryItem2, AddGeneMappingQueryItem.source_id == AddGeneMappingQueryItem2.destination_id)
            )
            
            select_input_source_real_destination_conflict_gene_ids = (
                select_query_items
                .with_entities(AddGeneMappingQueryItem.source_id.label('gene_id'))
                .join(GeneMappingTable, AddGeneMappingQueryItem.source_id == GeneMappingTable.c.destination_id)
            )
            
            select_real_source_input_destination_conflict_gene_ids = (
                select_query_items
                .with_entities(AddGeneMappingQueryItem.destination_id.label('gene_id'))
                .join(GeneMappingTable, AddGeneMappingQueryItem.destination_id == GeneMappingTable.c.source_id)
            )
            
            select_conflicting_gene_ids = (
                select_input_source_input_destination_conflict_gene_ids
                .union_all(
                    select_input_source_real_destination_conflict_gene_ids, 
                    select_real_source_input_destination_conflict_gene_ids
                )
                .subquery()
            )
            
            stmt = self._session.query(Gene).join(select_conflicting_gene_ids, Gene.id == select_conflicting_gene_ids.c.gene_id)
            conflicting_genes = list(stmt)
            if conflicting_genes:
                conflicting_genes = ', '.join(x.canonical_name.name for x in conflicting_genes)
                raise ValueError('Adding mapping would cause the following genes to appear on both the source and the destination side: ' + conflicting_genes)
            
            # Insert into the real table
            sub_stmt = (
                select_query_items
                .with_entities(AddGeneMappingQueryItem.source_id, AddGeneMappingQueryItem.destination_id)
            )
            stmt = (
                GeneMappingTable.insert().from_select(['source_id', 'destination_id'], sub_stmt)
            )
            self._session.execute(stmt)
