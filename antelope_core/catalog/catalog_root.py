"""CATALOG_ROOT specifies the local folder that stores the reference catalog
It must either:
 - be specified in an environment variable os.environ['ANTELOPE_CATALOG_ROOT'],
 - or as a file in the local directory named antelope_catalog_root

 Used as a default location for testing and benchmarking.
"""

import os
import warnings

ENVVAR = 'ANTELOPE_CATALOG_ROOT'

try:
    CATALOG_ROOT = os.environ[ENVVAR]
    print('Using specified %s=%s' % (ENVVAR, CATALOG_ROOT))
except KeyError:
    CATALOG_ROOT = '/data/LCI/cat-demo/'  # persistent
    warnings.warn('environ %s missing; using BK default: %s' % (ENVVAR, CATALOG_ROOT))

