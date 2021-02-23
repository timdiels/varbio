varbio (various bioinformatics) is an internal library with common code of
CoExpNetViz and MORPH.

### Links
- [Conda](https://anaconda.org/timdiels/varbio)
- [GitHub](https://github.com/timdiels/varbio/)
- [Old PyPI](https://pypi.python.org/pypi/varbio/), for older versions of the
  library.

### Usage
Add varbio as a dependency to your conda recipe.

### Development guide
Guide for developing varbio itself. It's analog to [pytil's dev guide](https://github.com/timdiels/pytil#development-guide).

You do not need to release pytil just to try it out in varbio, conda install
pytil's dependencies and finally `pip install -e .` in your local pytil
checkout. If for some reason you want to try out the pytil conda pkg instead,
you could build the pytil pkg locall first and then `conda install --use-local
pytil`.
