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
Database mapped entities
'''

from inflection import underscore
from sqlalchemy.ext.declarative import as_declarative, declared_attr
from sqlalchemy import Column, Integer, String, ForeignKey, Table, Boolean, Text, PickleType
from sqlalchemy.orm import relationship, deferred

# Note: there are some `x = None` statements in class definitions, this is to
# help autocomplete IDE functions know these attributes exist. Their actual
# value is filled in by sqlalchemy. Sqlalchemy does not require these statements.

# MySQL max utf-8 chars (inclusive) allowed on a column with an index
_max_index_key_length_char = 255

# Note: when using multiple declarative bases and you want the entities to refer
# to each other, make both bases share the same class_registry. You probably
# want them to share the same MetaData too, but I don't think it's necessary.
# Before relying on this, you may still want to double check this on the
# sqlalchemy mailing list.

# Note: when adding new database tables, if you want to fetch rows as a mapped
# entity, add a mapped entity for it, else just add a Table

# Note: while it may be tempting to use instrument_declarative to avoid having
# to inherit from a declarative base, this is a fragile solution. E.g. currently
# __init__ doesn't work as you'd hope, leading to hacks such as the example
# given here:
# https://websauna.org/docs/_modules/websauna/system/model/utils.html#attach_model_to_base

_table_args = {
    'mysql_engine': 'InnoDB',
    'mysql_character_set': 'utf8',
    'mysql_collate': 'utf8_general_ci'
}
    
def entities():
    '''
    Create entities and tables for use with deep_genome.core.Context
    
    You may directly use and backwards compatibly change (e.g. adding
    attributes) these entities: Gene, GeneName. Other entities and tables should
    be treated as private.
    
    Returns
    -------
    entities :: {class.__name__ => class}
        Mapped entities. All inherit from `base`.
            
    tables :: {name :: str => Table}
        The tables. All belong to ``base.metadata``.
        
    base ::
        Declarative base class used
    '''
    
    entities_ = []
    tables = {}
    
    @as_declarative()
    class DBEntity(object):
        
        @declared_attr
        def __tablename__(cls):
            return underscore(cls.__name__)
        
        @declared_attr
        def __table_args__(cls):
            return _table_args
        
        def __hash__(self):
            return hash(self.id)
    
    GeneMappingTable = Table('gene_mapping', DBEntity.metadata,
        Column('source_id', Integer, ForeignKey('gene.id'), primary_key=True),
        Column('destination_id', Integer, ForeignKey('gene.id'), primary_key=True),
        **_table_args
    )
    '''
    Maps genes from one set (called the source set) to the other (destination set).
    
    A gene may appear on either side (source or destination), but not both. I.e.,
    source_ids and destination_ids must be disjoint.
    '''
    tables['GeneMappingTable'] = GeneMappingTable
    
    class Gene(DBEntity):
        
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
         
        id =  Column(Integer, primary_key=True)
        description = deferred(Column(String(1000), nullable=True))
        canonical_name_id =  Column(Integer, ForeignKey('gene_name.id'), nullable=True)
          
        canonical_name = relationship('GeneName', foreign_keys=[canonical_name_id], post_update=True)  # The preferred name to assign to this gene. Each gene must have a canonical name.
        names = None # GeneName backref, all names
          
        mapped_to = relationship(   # genes which this gene maps to
            "Gene",
            backref='mapped_from', 
            secondary=GeneMappingTable,
            primaryjoin=id == GeneMappingTable.c.source_id,
            secondaryjoin=id == GeneMappingTable.c.destination_id
        )
        mapped_from = None  # genes that map to this gene
          
        @property
        def name(self):
            return self.canonical_name.name
           
        def __repr__(self):
            return 'Gene({!r}, {!r})'.format(self.id, self.canonical_name.name)
          
        def __str__(self):
            return 'Gene({!r})'.format(self.canonical_name.name)
          
        def __lt__(self, other):
            if not isinstance(other, Gene):
                raise TypeError()
            return self.name < other.name
        
    entities_.append(Gene)
    
    class GeneName(DBEntity):
         
        id =  Column(Integer, primary_key=True)
        name = Column(String(250), unique=True, nullable=False)
        gene_id =  Column(Integer, ForeignKey('gene.id'), nullable=False)
         
        gene = relationship('Gene', backref='names', foreign_keys=[gene_id])
         
        def __repr__(self):
            return 'GeneName({!r}, {!r})'.format(self.id, self.name)
        
    entities_.append(GeneName)
        
    class DataFile(DBEntity):
        
        '''
        A data file in ``context.data_directory / data_files``
        '''
        
        id =  Column(Integer, primary_key=True)
        
    entities_.append(DataFile)
        
    class CoroutineCall(DBEntity):
         
        id =  Column(Integer, primary_key=True)
        name = Column(Text, nullable=False)
        finished = Column(Integer, nullable=True)  # When NULL, not finished, else version number it finished as
        return_value = Column(PickleType, nullable=True) 
         
        def __repr__(self):
            return 'CoroutineCall({!r}, {!r})'.format(self.id, self.name)
        
    entities_.append(CoroutineCall)
        
    class Job(DBEntity):
         
        id =  Column(Integer, primary_key=True)
        name = Column(Text, nullable=False)
        finished = Column(Integer, nullable=True)  # When NULL, not finished, else version number it finished as
         
        def __repr__(self):
            return 'Job({!r}, {!r})'.format(self.id, self.name)
        
    entities_.append(Job)
        
    #####################################################
    # Query helper tables (temporary data, staging area)
        
    class GeneNameQuery(DBEntity):
        id =  Column(Integer, primary_key=True)
    entities_.append(GeneNameQuery)
        
    class GeneNameQueryItem(DBEntity):
        
        query_id =  Column(Integer, ForeignKey('gene_name_query.id', ondelete='cascade'), primary_key=True)
        row =  Column(Integer, primary_key=True, autoincrement=False)
        column =  Column(Integer, primary_key=True, autoincrement=False)
        name = Column(String(250), nullable=False)
        
    entities_.append(GeneNameQueryItem)
    
    class AddGeneMappingQuery(DBEntity):
        id =  Column(Integer, primary_key=True)
    entities_.append(AddGeneMappingQuery)
    
    class AddGeneMappingQueryItem(DBEntity):
         
        '''Temporary data for add_gene_mapping'''
         
        query_id =  Column(Integer, ForeignKey('add_gene_mapping_query.id', ondelete='cascade'), primary_key=True)
        source_id =  Column(Integer, ForeignKey('gene.id'), primary_key=True, autoincrement=False)
        destination_id =  Column(Integer, ForeignKey('gene.id'), primary_key=True, autoincrement=False)
        
        def __repr__(self):
            return 'AddGeneMappingQueryItem(query_id={}, source_id={}, destination_id={})'.format(self.query_id, self.source_id, self.destination_id)
        
    entities_.append(AddGeneMappingQueryItem)
        
    entities_ = {entity.__name__ : entity for entity in entities_}
    return entities_, tables, DBEntity
