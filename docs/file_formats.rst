File formats
============
Description of all file formats.

Expression matrix file
----------------------
A matrix file is a TSV file, i.e. it starts with a header line of column names
separated by tabs and is followed by rows with values separated by tabs.

The first column is the gene name; its column name is ignored and may be empty.
Other columns are conditions with as values the expression level of the genes
given that condition; the column name is the name of the condition. So, each
row is a gene and its expression levels in various conditions.

For example::

    gene	Heinz_bud	Heinz_flower	Heinz_leaf
    Solyc02g085950	12710.51	10259.24	122316.7
    Solyc10g075130	22209.16	46884	.78
    Solyc04g071610	4880.03	2966.38	310.43

Here, Solyc04g071610 has an expression level of 310.43 in the leaf.
