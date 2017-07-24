API reference
=============
The API reference makes use of a `type language`_; for example, to
describe exactly what arguments can be passed to a function.  

Overview
--------

.. currentmodule:: varbio
.. autosummary::

    clean
    correlation
    parse

varbio.clean
------------
.. currentmodule:: varbio.clean
.. automodule:: varbio.clean
.. autosummary::
   :nosignatures:

   plain_text

.. autofunction:: plain_text


varbio.correlation
------------------
.. currentmodule:: varbio.correlation
.. automodule:: varbio.correlation
.. autosummary::
   :nosignatures:

   generic
   generic_df
   pearson
   pearson_df

.. autofunction:: generic
.. autofunction:: generic_df
.. autofunction:: pearson
.. autofunction:: pearson_df


varbio.parse
------------
.. currentmodule:: varbio.parse
.. automodule:: varbio.parse
.. autosummary::
   :nosignatures:

   clustering
   expression_matrix

.. autofunction:: clustering
.. autofunction:: expression_matrix

.. _type language: http://pytil.readthedocs.io/en/5.0.0/type_language.html
