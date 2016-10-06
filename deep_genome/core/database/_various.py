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

from chicken_turtle_util import data_frame as df_
from chicken_turtle_util.sqlalchemy import pretty_sql
import sqlalchemy as sa
import sqlalchemy.sql as sql
from sqlalchemy.orm import sessionmaker, aliased
from contextlib import contextmanager
from collections import namedtuple
from ._entities import entities as create_entities
import pandas as pd
import logging
import attr
import os

_logger = logging.getLogger('deep_genome.core.Database')

_ReturnTuple = namedtuple('_ReturnTuple', 'expression_matrices clusterings'.split())

def _data_file_dir(context):
    return context.data_directory / 'data_files'

def _print_sql(stmt):
    print(pretty_sql(stmt))
    
@attr.s(frozen=True)
class Credentials(object):
    host = attr.ib()
    'Host to connect to as DNS or IP' #TODO do these show up in apiref or help()? If not, use Credentials' docstring
    
    database = attr.ib()
    'Database name'
    
    user = attr.ib()
    'Database user'
    
    password = attr.ib(repr=False)
    'Password of user'
    
class Database(object):
    
    # Note: could be used on other RDBMS with these changes (just not SQLite as
    # it's too basic, e.g. no transactions): connection string, table args to
    # ensure case-insensitive collation, ...
    
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
    credentials : Credentials
        Database credentials
    entities : {class.__name__ => class} or None
        If not ``None``, entities to use. See
        :meth:`deep_genome.core.database.entities`. Cannot be ``None`` if `tables`
        is not ``None``. If ``None``, default entities are used.
    tables : {name :: str => Table} or None
        If not ``None``, tables to use. See
        :meth:`deep_genome.core.database.entities`. Cannot be ``None`` if `entities`
        is not ``None``. If ``None``, default tables are used.
        
    Notes
    -----
    The database serves 2 main purposes: persistence and not having to store
    everything in memory. As such, any non-trivial data operation requires
    the database.
    
    Genes which appear as a source gene of a gene mapping, will appear nowhere
    else, other than in GeneName.
    '''
    
    def __init__(self, context, credentials, entities=None, tables=None): 
        if (entities is None) != (tables is None):
            raise ValueError(
                'entities and tables must be either both None or both not None, '
                'got:\nentities={},\ntables={}'.format(entities, tables)
            )
        
        self._context = context
        self._engine = sa.create_engine(
            'mysql+pymysql://{user}:{password}@{host}/{database}'.format(**attr.asdict(credentials)),
            pool_recycle=3600  # throw away connections that have been unused for 1h as they might have gone stale (mysql by default disconnects a connection after 8h) 
        )
        self._Session = sessionmaker(bind=self._engine)
        os.makedirs(str(_data_file_dir(context)), exist_ok=True)
        
        if entities is None:
            entities, tables, _ = create_entities()
                
        self.e = attr.make_class('Entities', list(entities))(**entities)
        self.t = attr.make_class('Tables', list(tables))(**tables)
        self._meta_datas = {entity.metadata for entity in entities.values()} | {table.metadata for table in tables.values()}
        
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
        session = Session(self._context, self._create_session(), self.e, self.t)
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
        for meta_data in self._meta_datas:
            meta_data.create_all(self._engine)
        
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
    
    def __init__(self, context, session, entities, tables):
        self._context = context
        self._session = session
        self.e = entities
        self.t = tables
        
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
        Gene = self.e.Gene
        GeneName = self.e.GeneName
        GeneNameQueryItem = self.e.GeneNameQueryItem
        
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
        Gene = self.e.Gene
        GeneName = self.e.GeneName
        GeneNameQueryItem = self.e.GeneNameQueryItem
        
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
        
        with self.query(self.e.GeneNameQuery) as query:
            # Insert gene group for query
            if isinstance(names, pd.Series):
                items = names.to_frame()
            else:
                items = names.copy(deep=False)
            items.columns = range(len(items.columns))
            items['row'] = range(len(items))
            items = pd.melt(items, id_vars='row', var_name='column', value_name='name')
            items['query_id'] = query.id
            self._session.bulk_insert_mappings(self.e.GeneNameQueryItem, items.to_dict('record'))
            
            # Add any unknown genes (genes not in database)
            self._add_unknown_genes(query.id)
            
            # Find genes by name
            genes = self._get_genes_by_name(query.id, names, map_=_map)
            
        return genes
    
    def _create_data_file(self):
        file = self.e.DataFile()
        self._session.add(file)
        self._session.flush()
        return file
    
    def _data_file_path(self, data_file):
        return _data_file_dir(self._context) / str(data_file.id)
        
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
        Gene = self.e.Gene
        AddGeneMappingQueryItem = self.e.AddGeneMappingQueryItem
        GeneMappingTable = self.t.GeneMappingTable
        
        if mapping.empty:
            return
        
        with self.query(self.e.AddGeneMappingQuery) as query:
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
