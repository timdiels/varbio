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
Database entities (i.e. roughly equivalent to the tables of the database)
'''

from sqlalchemy.ext.declarative import declarative_base, declared_attr
from inflection import underscore
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Table
from sqlalchemy.orm import relationship, deferred
from chicken_turtle_util.various import URL_MAX_LENGTH, PATH_MAX_LENGTH
from datetime import datetime

# Note: there are some `x = None` statements in class definitions, this is to
# help autocomplete IDE functions know these attributes exist. Their actual
# value is filled in by sqlalchemy. Sqlalchemy does not require these statements.

# max length (inclusive) of a key used in any index, expressed in chars
_max_index_key_length_char = 255

class DBEntity(object):
    @declared_attr
    def __tablename__(cls):
        return underscore(cls.__name__)
    
    @declared_attr
    def __table_args__(cls):
        return {
            'mysql_engine': 'InnoDB',
            'mysql_character_set': 'utf8',
            'mysql_collate': 'utf8_general_ci'
        }

DBEntity = declarative_base(cls=DBEntity)

class CachedFile(DBEntity):
    
    id =  Column(Integer, primary_key=True)
    source_url = Column(String(min(URL_MAX_LENGTH, _max_index_key_length_char)), unique=True, nullable=False)
    path = Column(String(PATH_MAX_LENGTH), nullable=False)  # absolute path to file in cache
    cached_at = Column(DateTime, nullable=False)
    expires_at = Column(DateTime, nullable=False)
    
    def __repr__(self):
        return (
            '<CachedFile(id={!r}, source_url={!r}, path={!r}, cached_at={!r}'
            ', expires_at={!r})>'.format(
                self.id, self.source_url, self.path,
                self.cached_at, self.expires_at
            )
        )
    
    @property
    def expired(self):
        return self.expires_at <= datetime.now()
    
        
class GeneName(DBEntity):
     
    id =  Column(Integer, primary_key=True)
    name = Column(String(250), unique=True, nullable=False)
    gene_id =  Column(Integer, ForeignKey('gene.id'), nullable=False)
     
    gene = relationship('Gene', backref='names', foreign_keys=[gene_id])
     
    def __repr__(self):
        return '<GeneName(id={!r}, name={!r})>'.format(self.id, self.name)

class DataFile(DBEntity):
    
    '''
    A data file in ``context.configuration.data_directory / data_files``
    '''
    
    id =  Column(Integer, primary_key=True)
    
class GeneNameQuery(DBEntity):
    id =  Column(Integer, primary_key=True)
    
class GeneNameQueryItem(DBEntity):
    
    '''Temporary data for get_genes_by_name query'''
    
    query_id =  Column(Integer, ForeignKey('gene_name_query.id', ondelete='cascade'), primary_key=True)
    row =  Column(Integer, primary_key=True, autoincrement=False)
    column =  Column(Integer, primary_key=True, autoincrement=False)
    name = Column(String(250), nullable=False)

class AddGeneMappingQuery(DBEntity):
    id =  Column(Integer, primary_key=True)

class AddGeneMappingQueryItem(DBEntity):
     
    '''Temporary data for add_gene_mapping'''
     
    query_id =  Column(Integer, ForeignKey('add_gene_mapping_query.id', ondelete='cascade'), primary_key=True)
    source_id =  Column(Integer, ForeignKey('gene.id'), primary_key=True, autoincrement=False)
    destination_id =  Column(Integer, ForeignKey('gene.id'), primary_key=True, autoincrement=False)
    
    def __repr__(self):
        return 'AddGeneMappingQueryItem(query_id={}, source_id={}, destination_id={})'.format(self.query_id, self.source_id, self.destination_id)
    
GeneMappingTable = Table('gene_mapping', DBEntity.metadata,
    Column('source_id', Integer, ForeignKey('gene.id'), primary_key=True),
    Column('destination_id', Integer, ForeignKey('gene.id'), primary_key=True),
)
'''
Maps genes from one set (called the source set) to the other (destination set).

A gene may appear on either side (source or destination), but not both. I.e.,
source_ids and destination_ids must be disjoint.
'''
 
class Gene(DBEntity):
     
    id =  Column(Integer, primary_key=True)
    description = deferred(Column(String(1000), nullable=True))
    canonical_name_id =  Column(Integer, ForeignKey('gene_name.id'), nullable=True)
     
    canonical_name = relationship('GeneName', foreign_keys=[canonical_name_id], post_update=True)  # The preferred name to assign to this gene. Each gene must have a canonical name.
    names = None # GeneName backref, all names
    expression_matrices = None  # ExpressionMatrix backref, all matrices of which the gene is part of
    clusterings = None  # Clustering backref, all clusterings of which the gene is part of
    
    mapped_to = relationship(   # genes which this gene maps to
        "Gene",
        backref='mapped_from', 
        secondary=GeneMappingTable,
        primaryjoin=id == GeneMappingTable.c.source_id,
        secondaryjoin=id == GeneMappingTable.c.destination_id
    )
    mapped_from = None  # genes that map to this gene
     
    def __repr__(self):
        return 'Gene(id={!r}, name={!r})'.format(self.id, self.canonical_name.name)
    
    def __str__(self):
        return 'Gene({!r})'.format(self.canonical_name.name)
    
    def __lt__(self, other):
        if isinstance(other, Gene):
            return self.id < other.id
        else:
            return False
    
    
GeneExpressionMatrixTable = Table('gene_expression_matrix', DBEntity.metadata,
    Column('gene_id', Integer, ForeignKey('gene.id')),
    Column('expression_matrix_id', Integer, ForeignKey('expression_matrix.id'))
)


class ExpressionMatrix(DBEntity):
     
    id =  Column(Integer, primary_key=True)
    data_file_id = Column(Integer, ForeignKey('data_file.id'), nullable=False)
     
    genes = relationship("Gene", backref='expression_matrices', secondary=GeneExpressionMatrixTable)  # Genes whose expression was measured in the expression matrix
    data_file = relationship('DataFile')
     
    def __repr__(self):
        return '<ExpressionMatrix(id={!r})>'.format(self.id, self.path)
    
    def __str__(self):
        return '<ExpressionMatrix {!s}>'.format(self.id)
    
    def __lt__(self, other):
        if isinstance(other, ExpressionMatrix):
            return self.id < other.id
        else:
            return False


GeneClusteringTable = Table('gene_clustering', DBEntity.metadata,
    Column('gene_id', Integer, ForeignKey('gene.id')),
    Column('clustering_id', Integer, ForeignKey('clustering.id'))
)


class Clustering(DBEntity):
    
    'Gene clustering'
     
    id =  Column(Integer, primary_key=True)
    data_file_id = Column(Integer, ForeignKey('data_file.id'), nullable=False)
    expression_matrix_id =  Column(Integer, ForeignKey('expression_matrix.id'), nullable=True)
     
    genes = relationship("Gene", backref='clusterings', secondary=GeneClusteringTable)  # Genes mentioned in the clustering
    expression_matrix = relationship('ExpressionMatrix')  # The expression_matrix this clustering should be used with, else can be used with any expression matrix
    data_file = relationship('DataFile')
     
    def __repr__(self):
        return '<Clustering(id={!r})>'.format(self.id, self.path)
    
    def __str__(self):
        return '<Clustering {!s}>'.format(self.id)
    
    def __lt__(self, other):
        if isinstance(other, Clustering):
            return self.id < other.id
        else:
            return False
    
class GetByGenesQuery(DBEntity):
    id =  Column(Integer, primary_key=True)
    
class GetByGenesQueryItem(DBEntity):
    
    '''Temporary storage for bait sets to query on'''
    
    query_id =  Column(Integer, ForeignKey('get_by_genes_query.id', ondelete='cascade'), primary_key=True)
    gene_group_id =  Column(Integer, primary_key=True, autoincrement=False)
    gene_id =  Column(Integer, ForeignKey('gene.id'), primary_key=True)
    
    gene = relationship('Gene', foreign_keys=[gene_id])
    
class Task(DBEntity):
     
    id =  Column(Integer, primary_key=True)
    name = Column(String(250), unique=True, nullable=False)
     
    def __repr__(self):
        return '<Task(name={!r})>'.format(self.name)
    