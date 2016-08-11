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

from deep_genome.core.database.entities import ExpressionMatrix
from deep_genome.core.parsers import Parser
from deep_genome.core import clean
from chicken_turtle_util import data_frame as df_
import logging
import pandas as pd

_logger = logging.getLogger(__name__)
    
class FileImporter(object):
    
    '''
    Imports files to database
    '''
    
    def __init__(self, context):
        self._context = context
        self._parser = Parser(context)
    
    def import_expression_matrix(self, path):
        '''
        Import expression matrix file into database
        
        The file is read and cleaned (without modifying the original) using an
        equivalent of `core.clean.plain_text`, then parsed according to
        `core.parsers.Parser.parse_expression_matrix`. The result is added to
        database.
        
        Parameters
        ----------
        path : pathlib.Path
            Path to expression matrix file.
            
        Returns
        -------
        int
            Id of added expression matrix
        '''
        db = self._context.database
        with db.scoped_session() as session:
            _logger.info('Adding expression matrix: {}'.format(path))
            with path.open() as f:
                exp_mat = self._parser.parse_expression_matrix(clean.plain_text(f))
            exp_mat = session.add_expression_matrix(exp_mat)
            session.sa_session.flush()
            return exp_mat.id
               
    # TODO think about thorough validation for each input here and in file reading and don't forget to make quick todo notes on new input in the future
    # TODO the funcs here will be reusable and should be thrown in somewhere else, something in core. We used to call it DataImporter, we won't now
    def import_clustering(self, path, name_index=0, expression_matrix_id=None):
        '''
        Import gene clustering file into database
        
        The file is read and cleaned (without modifying the original) using an
        equivalent of `core.clean.plain_text`, then parsed according to
        `core.parsers.Parser.parse_clustering`. Cluster ids are treated case-
        insensitively, overlapping clusters are merged. The result is added to
        database.
        
        Parameters
        ----------
        path : pathlib.Path
            Path to clustering file
        name_index
            See `core.parsers.Parser.parse_clustering`
        expression_matrix_id : int or None
            If not None, hint to algorithms to use this clustering only on this
            expression matrix. Otherwise, it may be used on any expression matrix.
            
        Returns
        -------
        int
            Id of added clustering
        '''
        db = self._context.database
        with db.scoped_session() as session:
            _logger.info('Adding clustering: {}'.format(path))
            with path.open() as f:
                clustering = self._parser.parse_clustering(clean.plain_text(f), name_index=name_index)
            clustering = pd.DataFrame(list(clustering.items()), columns=('cluster_id', 'gene'))
            
            # Use consistent case for each clustering (without resorting to all lower case)
            clustering['cluster_id_lower'] = clustering['cluster_id'].str.lower()
            canonical_ids = clustering.groupby('cluster_id_lower')['cluster_id'].apply(lambda x: x.iloc[0])  # pd.Series mapping 1-to-1 lowercase => case
            clustering.drop('cluster_id', inplace=True, axis=1)
            clustering = clustering.join(canonical_ids, on='cluster_id_lower')
            clustering.drop('cluster_id_lower', inplace=True, axis=1)
            
            # Split gene sets
            clustering['gene'] = clustering['gene'].apply(list)
            clustering = df_.split_array_like(clustering, 'gene')

            #
            if expression_matrix_id is not None:
                expression_matrix = session.sa_session.query(ExpressionMatrix).get(expression_matrix_id)
            else:
                expression_matrix = None
                
            clustering = session.add_clustering(clustering, expression_matrix)
            session.sa_session.flush()
            return clustering.id
            
    def import_gene_mapping(self, path):
        '''
        Import gene mapping into database
        
        The file is read and cleaned (without modifying the original) using an
        equivalent of `core.clean.plain_text`, then parsed according to
        `core.parsers.Parser.parse_clustering`. The result is added to
        database.
        
        Parameters
        ----------
        path : pathlib.Path
            Path to gene mapping file.
        '''
        db = self._context.database
        with db.scoped_session() as session:
            # Read file
            _logger.info('Adding gene mapping from: {}'.format(path))
            with path.open() as f:
                mapping = self._parser.parse_clustering(clean.plain_text(f))
            mapping = pd.DataFrame(list(mapping.items()), columns=('source', 'destination'))
            mapping['destination'] = mapping['destination'].apply(list)
            mapping = df_.split_array_like(mapping, 'destination')
            
            # Add
            session.add_gene_mapping(mapping)

# '''
# Affymetrix format parsers
# 
# Affymetrix files come either as one matrix or as multiple CEL files, each containing
# one condition/column of the expression matrix. In the former case, use `matrix`,
# in the latter use `cel` and `merge_cels`.
# 
# These return Affymetrix specific expression matrices, to convert to DG expression
# matrices, use `to_expression_matrix`.
# '''
# No parse expose, too hard, needs probe to gene map which requires database ideally. For now just assume affy probe mappings are the same across species and experiments, i.e. no reuse of probe id for different genes (though a probe id can map to multiple genes?) 
# parts
# matrix
# 
# def _read(path, is_part):
#     header = 0 if is_part else [0,1]
#     affy = pandas.read_table(str(path), index_col=0, header=header, engine='python')
#     return affy
#     #TODO used to affy.index = affy.index.str.lower(), adjust callers
#
# class AffymetrixFileImporter(object):
#     
#     '''
#     Imports files to database
#     '''
#     
#     def __init__(self, database):
#         self._database = database
#             
#     #: names of columns containing gene expression values
#     expression_column_names = ['Affymetrix:CHPSignal', 'GEO:AFFYMETRIX_VALUE', 'VALUE']
#     
#     #: names of columns containing abs-call values
#     abs_call_column_names = ['Affymetrix:CHPDetection', 'GEO:AFFYMETRIX_ABS_CALL', 'ABS_CALL']
#     
#     def import_parts(self, paths):
#         '''
#         Import Affymetrix gene expression matrix by part files
#         
#         Each part file represents a single condition.
#         
#         Expected format of each file is a plain text table using tab as column
#         separator. The table has one header line. One of
#         `FileImporter.expression_column_names` must appear once in the header.
#         
#         Parameters
#         ----------
#         paths : [pathlib.Path]
#             Paths to the affymetrix parts
#         
#         Examples
#         --------
#         File format example::
#         
#             ID_REF  VALUE
#             AFFX-BioB-5_at  279.356
#             AFFX-BioB-M_at  290.292
#             AFFX-BioB-3_at  324.325
#         '''
#         return _read(reader, True)
#     
#     def import_matrix(self, paths):
#         '''
#         Import Affymetrix gene expression matrix file
#         
#         Expected format of each file is a plain text table using tab as column
#         separator. The table has two header lines. The second header line must
#         contain one of `FileImporter.expression_column_names` once per
#         condition. The first header line contains the condition names, repeated
#         to match the second header line.
#         
#         Parameters
#         ----------
#         path : pathlib.Path
#             Path to the affymetrix matrix file
#         
#         Examples
#         --------
#         File format example::
#         
#             Scan REF<tab>GSM357133.CEL<tab>GSM357133.CEL<tab>GSM357234.CEL<tab>GSM357234.CEL
#             Composite Element REF<tab>VALUE<tab>Affymetrix:CHPPairs<tab>VALUE<tab>Affymetrix:CHPPairs
#             AFFX-BioB-5_at<tab>179.1<tab>20<tab>123.2<tab>20
#             AFFX-BioB-M_at<tab>123.8<tab>20<tab>232.5<tab>20
#         '''
#         return _read(path, False)
#         # TODO no longer doing this: affy.sortlevel(axis=1, inplace=True, sort_remaining=True), adjust callers
#     
# def parts(*paths):
#     '''
#     Read unmerged affymetrix parts.
# 
#     Returns merged data frame with sorted columns index
#     '''
#     parts = [_read(path, True) for path in paths]
#     condition_names = list(map(os.path.basename, paths))
#     merged = pandas.concat(parts, axis=1, keys=condition_names, names=['Condition', 'Field']) #TODO don't bother maintaining
#     merged.sortlevel(axis=1, inplace=True, sort_remaining=True)
#     return merged
# 
# def to_expression_matrix(affy, probe_to_gene):
#     '''
#     - affy: data frame as read
#     - probe_to_gene: see `deep_genome.reader.probe_gene_map` module
# 
#     Returns data frame in exp mat format
#     '''
# 
#     # Ignore probes with any other suffix
#     probe_suffix = 's1_at' # than this one
#     affy = affy.loc[affy.index.str.endswith(probe_suffix)]
# 
#     # replace probe names with gene names
#     # Note: not each probe maps to a gene. (Some probes serve as a scientific control)
#     affy.index = affy.index.map(lambda x: probe_to_gene[x] if x in probe_to_gene else None)
#     affy.drop(None)
# 
#     # Multiple probes can map to the same gene. When it does, we only use the
#     # first encountered probe (TODO figure out a proper scheme of picking the
#     # best probe; why are there multiple probes of the same gene in the first
#     # place (gene variants perhaps?)?)
#     affy = affy.groupby(level=0).first()
# 
#     # Determine the column naming used 
#     for (signal_column, abs_call_column) in _column_names:
#         if signal_column in affy.columns.get_level_values(1):
#             break
#     else:
#         raise Exception('Unknown signal column name used in affymetrix file. Perhaps another variant of column naming should be added to the code?')
# 
#     # Filter to rows with favorable abs call for each condition  # TODO consider letting through those with a few failed abs calls. TODO take along the P-value if any, if doing proper bayesian calculations
#     if abs_call_column in affy.columns.get_level_values(1):
#         rows_with_all_present = affy.loc[:, (slice(None),abs_call_column)].apply(lambda x: x.str.match('(?i)p|present')).all(axis=1)
#         affy = affy.loc[rows_with_all_present]
# 
#     # Transform to exp mat
#     affy = affy.loc[:, (slice(None), signal_column)]
#     affy.columns = affy.columns.droplevel(1)
#     affy.columns = ['gene'] + affy.columns[1:].tolist()
# 
#     return affy

# TODO DataImporter.add_affymetrix(whole : Path), add_affymetrix_parts(parts : Paths)
# def gup():
#     experiment_name="$1"
# 
#     # For the unmerged ones
#     "$basedir/merge_data_files.py" "$@"
#     mv merged_data $experiment_name
# 
#     #
#     file="$experiment_name"
#     stats_file="${file}.stats"
#     expr_file="${file}.expression_matrix"
# 
#     write_stats():
#         wc -l "$file" >> "$stats_file"
#         # TODO switches to expr_file along the way
# 
#     touch(stats_file)
# 
#     affy = affymetrix.read(TODO) or read_parts(TODO, ...)
#     probe_to_gene = probe_gene_map.read(TODO, TODO)
#         conf.src_dir / 'rice/expression_matrices/affymetrix/probe_to_gene/probe_names gene_names'
#     affymetrix.to_expression_matrix(affy, probe_to_gene)
# 
# 
#     echo Quantile normalization
#     "$basedir/quantile_normalise.py" "$expr_file"
# 
#     echo 'Filter rows with all(values < 10) or sd<0.1'
#     "$basedir/filter.py" '0.1' "$expr_file"
#     wc -l "$expr_file" >> "$stats_file"
# 
#     #echo 'Filter rows with sd < 25'
#     #"$basedir/filter.py" 25 "$expr_file"
#     #wc -l "$expr_file" >> "$stats_file"
#     echo "NaN NA" >> "$stats_file"
# 
#     #TODO output should go to `target`, no other file should be written
