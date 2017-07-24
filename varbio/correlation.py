# Copyright (C) 2015 VIB/BEG/UGent - Tim Diels <timdiels.m@gmail.com>
#
# This file is part of varbio.
#
# varbio is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# varbio is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with varbio.  If not, see <http://www.gnu.org/licenses/>.

'''
Vectorised correlation statistics and correlation matrices

Efficient implementations for Pearson's r. A slow generic implementation for
other correlation statistics.
'''

# TODO manual inspect mutual_information for correctness
# - is it exactly what we want in bioinformatics? 
# - is it used correctly?

# TODO efficient mutual_information
#
# - add to above " and mutual information" once
# - actually implement a more efficient version below
# - remove the mention of slowness in the docstring of its _df func

import numpy as np
import pandas as pd
from sklearn.metrics import mutual_info_score

def generic(correlation_function, data, indices):
    '''
    Get correlation of each row in a 2d array compared to a subset thereof

    This function is less efficient than those specialised to a specific
    correlation function. See the 'See also' section for whether a specialised
    alternative is available for your correlation function.

    Parameters
    ----------
    correlation_function : ((x :: array-like(dtype=float)), (y :: array-like(dtype=float)) -> float
        Function that returns a correlation statistic when given 2 numpy array-
        like's.
    data : np.array(dtype=float, dimensions=2)
        Data for which to calculate correlations
    indices
        Indices to derive the subset ``data[indices]`` to compare against. You
        may use any form of numpy indexing.

    Returns
    -------
    correlation_matrix : np.array(dtype=float, shape=(len(data), len(indices)))
        2D array containing all correlations. ``correlation_matrix[i,j]``
        contains ``correlation_function(data[i], data[indices][j]``.

    See also
    --------
    pearson : Get Pearson's r of each row in a 2d array compared to a subset thereof
    '''
    # TODO: optimise: can be sped up (turn into metric specific linalg, or keep generic and use np ~enumerate). For vectorising mutual info further, see https://github.com/scikit-learn/scikit-learn/blob/c957249/sklearn/metrics/cluster/supervised.py#L507
    if not data.size or not len(indices):
        return np.empty((data.shape[0], len(indices)))
    return np.array([np.apply_along_axis(correlation_function, 1, data, data[item]) for item in indices]).T

# Note: GSL's pearson also returns .999...98 instead of 1 when comparing (1,2,3) to itself
def pearson(data, indices):
    '''
    Get Pearson's r of each row in a 2d array compared to a subset thereof

    Parameters
    ----------
    data : np.array(dtype=float, dimensions=2)
        Data for which to calculate correlations
    indices
        Indices to derive the subset ``data[indices]`` to compare against. You
        may use any form of numpy indexing.

    Returns
    -------
    correlation_matrix : np.array(dtype=float, shape=(len(data), len(indices)))
        2D array containing all correlations. ``correlation_matrix[i,j]``
        contains the correlation between ``data[i]`` and ``data[indices][j]``.

    Notes
    -----
    The current implementation is a vectorised form of `gsl_stats_correlation`
    from the GNU Scientific Library. Unlike GSL's implementation,
    correlations are clipped to [-1, 1].

    Pearson's r is also, perhaps more commonly, known as the product-moment
    correlation coefficient.
    '''
    if not data.size or not len(indices):
        return np.empty((data.shape[0], len(indices)))

    matrix = data
    mean = matrix[:,0].copy()
    delta = np.empty(matrix.shape[0])
    sum_sq = np.zeros(matrix.shape[0])  # sum of squares
    sum_cross = np.zeros((matrix.shape[0], len(indices)))

    for i in range(1,matrix.shape[1]):
        ratio = i / (i+1)
        delta = matrix[:,i] - mean
        sum_sq += (delta**2) * ratio;
        sum_cross += np.outer(delta, delta[indices]) * ratio;
        mean += delta / (i+1);

    sum_sq = np.sqrt(sum_sq)
    with np.errstate(divide='ignore',invalid='ignore'):  # divide by zero, it happens
        correlations = sum_cross / np.outer(sum_sq, sum_sq[indices])
    np.clip(correlations, -1, 1, correlations)
    return correlations

def mutual_information(data, indices): #TODO docstring + mention it's slow implementation
    return generic(mutual_info_score, data, indices)

#TODO when implementing, be careful of numerical error (walk through numpy and read up on numerical analysis)
# def mutual_information(data, subset, metric):
#     # A more vectorised version of https://github.com/scikit-learn/scikit-learn/blob/c957249/sklearn/metrics/cluster/supervised.py#L507
#     if contingency is None:
#         labels_true, labels_pred = check_clusterings(labels_true, labels_pred)
#         contingency = contingency_matrix(labels_true, labels_pred)
#     contingency = np.array(contingency, dtype='float')
#     contingency_sum = np.sum(contingency)
#     pi = np.sum(contingency, axis=1)
#     pj = np.sum(contingency, axis=0)
#     outer = np.outer(pi, pj)
#     nnz = contingency != 0.0
#     # normalized contingency
#     contingency_nm = contingency[nnz]
#     log_contingency_nm = np.log(contingency_nm)
#     contingency_nm /= contingency_sum
#     # log(a / b) should be calculated as log(a) - log(b) for
#     # possible loss of precision
#     log_outer = -np.log(outer[nnz]) + log(pi.sum()) + log(pj.sum())
#     mi = (contingency_nm * (log_contingency_nm - log(contingency_sum))
#           + contingency_nm * log_outer)
#     return mi.sum()

def generic_df(vectorised_correlation_function, data, subset):
    '''
    Get correlation of each row in a DataFrame compared to a subset thereof

    Parameters
    ----------
    vectorised_correlation_function : (data :: np.array(dimensions=2), indices) -> correlation_matrix :: np.array
        Function for creating a correlation matrix. This function is exactly
        like ``generic`` with a ``correlation_function`` already applied.
    data : pd.DataFrame([[float]])
        Data for which to calculate correlations
    subset
        Subset of ``data`` to compare against. Additionally, ``subset.index``
        must be a subset of ``data.index``.

    Returns
    -------
    correlation_matrix : pd.DataFrame([[float]], index=data.index, columns=subset.index)
        Data frame with all correlations. ``correlation_matrix.iloc[i,j]``
        contains the correlation between ``data.iloc[i]`` and
        ``subset.iloc[j]``.
    '''
    if not data.index.is_unique:
        raise ValueError('``data.index`` must be unique')
    correlations = vectorised_correlation_function(data.values, subset.index.map(data.index.get_loc))
    correlations = pd.DataFrame(correlations, index=data.index, columns=subset.index)
    return correlations

def pearson_df(data, subset):
    # Docstring copy pasted from generic_df and adjusted to refer to pearson
    '''
    Get Pearson correlation of each row in a DataFrame compared to a subset thereof

    Parameters
    ----------
    data : pd.DataFrame([[float]])
        Data for which to calculate correlations
    subset
        Subset of ``data`` to compare against. Additionally, ``subset.index``
        must be a subset of ``data.index``.

    Returns
    -------
    correlation_matrix : pd.DataFrame([[float]], index=data.index, columns=subset.index)
        Data frame with all correlations. ``correlation_matrix.iloc[i,j]``
        contains the correlation between ``data.iloc[i]`` and
        ``subset.iloc[j]``.
    '''
    return generic_df(pearson, data, subset)

def mutual_information_df(data, subset): #TODO docstring from generic_df
    return generic_df(mutual_information, data, subset)

