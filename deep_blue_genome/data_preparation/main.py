# Copyright (C) 2015, 2016 VIB/BEG/UGent - Tim Diels <timdiels.m@gmail.com>
# 
# This file is part of Deep Blue Genome.
# 
# Deep Blue Genome is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# Deep Blue Genome is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
# 
# You should have received a copy of the GNU Lesser General Public License
# along with Deep Blue Genome.  If not, see <http://www.gnu.org/licenses/>.

import pandas as pd
import click
import deep_blue_genome.core.cli as ctx
from deep_blue_genome.core.reader.various import read_expression_matrix_file,\
    read_clustering_file, read_gene_mapping_file
from deep_blue_genome.core.database.entities import ExpressionMatrix, Clustering,\
    GeneMappingTable
import plumbum as pb
import logging
from deep_blue_genome.core.exceptions import TaskFailedException
from chicken_turtle_util.pandas import df_has_null, series_has_duplicates
from chicken_turtle_util.exceptions import log_exception
from chicken_turtle_util.various import is_data_file
from deep_blue_genome.util.plumbum import list_files

_logger = logging.getLogger('deep_blue_genome.prepare')

'''
The main tool to prepare data for DBG tools
'''

def load_rice_genes(database):
    '''
    Load MSU and RAP gene names
    '''
    
class Context(ctx.CacheMixin, ctx.DatabaseMixin, ctx.TemporaryFilesMixin, ctx.OutputMixin, ctx.ConfigurationMixin):
    pass


# '''/www/group/biocomp/extra/morph/ARABIDOBSIS/gene_descriptions
# /www/group/biocomp/extra/morph/ITAG/gene_descriptions
# /www/group/biocomp/extra/morph/PGSC/gene_descriptions
# /www/group/biocomp/extra/morph/TOMATO/gene_descriptions
# /www/group/biocomp/extra/morph/rice/annotations
# /www/group/biocomp/extra/morph/catharanthus_roseus/functional_annotations'''


    
@click.command()
@ctx.cli_options(Context) #TODO we still have version on this? Add to cli_options if not
@click.pass_obj
def prepare(main_config, **kwargs):
    '''Create and/or update database.'''
    kwargs['main_config'] = main_config
    context = Context(**kwargs)
    context.database.recreate()
    
    def to_paths(listing):
        paths = (p.strip() for p in listing.splitlines())
        paths = [p.replace('/www/group/biocomp/extra/morph', '/mnt/data/doc/work/prod_data') for p in paths if p]
        paths = list(list_files(map(pb.local.path, paths), filter_=is_data_file))
        return paths
    
    gene_mappings = to_paths('''
        /www/group/biocomp/extra/morph/rice/msu_to_rap.mapping
    ''')
    
    expression_matrices = to_paths('''
        /www/group/biocomp/extra/morph/ARABIDOBSIS/data_sets
        /www/group/biocomp/extra/morph/ITAG/data_sets
        /www/group/biocomp/extra/morph/PGSC/data_sets
        /www/group/biocomp/extra/morph/TOMATO/data_sets
        /www/group/biocomp/extra/morph/rice/data_sets
        /www/group/biocomp/extra/morph/catharanthus_roseus/expression_matrices
    ''')
    
    clusterings = to_paths('''
        /www/group/biocomp/extra/morph/ARABIDOBSIS/cluster_solution/Pollen_boavida_IsEnzymeClusteringSol.txt
        /www/group/biocomp/extra/morph/ARABIDOBSIS/cluster_solution
        /www/group/biocomp/extra/morph/ITAG/cluster_solution
        /www/group/biocomp/extra/morph/PGSC/cluster_solution
        /www/group/biocomp/extra/morph/TOMATO/cluster_solution
        /www/group/biocomp/extra/morph/rice/clusterings
        /www/group/biocomp/extra/morph/catharanthus_roseus/clusterings
    ''')
    
    for path in gene_mappings:
        with log_exception(_logger, TaskFailedException):
            add_gene_mapping(context, path)
             
    for exp_mat in expression_matrices:
        with log_exception(_logger, TaskFailedException):
            add_expression_matrix(context, exp_mat)
         
    for clustering in clusterings:
        with log_exception(_logger, TaskFailedException):
            add_clustering(context, clustering)

    # Generate pathway files (files with genes in each pathway)
    pathways = pd.read_table('Ath_AGI_LOCUS_TAIR10_Aug2012.txt', quotechar="'")
    pathways.columns = pathways.columns.to_series().apply(str.lower)
    arabidopsis_pathways_dir = pb.local.path('arabidopsis_pathways')
    arabidopsis_pathways_dir.mkdir()
    for name, genes in pathways.groupby('name')['identifier']:
        genes = genes.dropna()
        if not genes.empty:
            with (arabidopsis_pathways_dir / name.replace(' ', '_').replace('/', '.')).open('w') as f:
                f.write('\n'.join(genes.tolist()))
    
    # TODO output_dir context
    # TODO dist to output_dir
    
    # XXX old stuff:
#     load_gene_info(database)
#     load_rice_genes(database)
#     merge_plaza()
    

# TODO locking? E.g. Should not run morph when database is being rebuilt from scratch. This would happen in a daily or weekly nightly
# batch.
# ... we need to design the required locking (e.g. prep in a separate file, then swap files and in the meantime prevent writes to the previous one or something. Or simply have downtime.)
    
    